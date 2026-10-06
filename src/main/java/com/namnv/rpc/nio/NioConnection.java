package com.namnv.rpc.nio;

import com.namnv.agent.AgentLoop;
import com.namnv.rpc.RpcCodec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * Một kết nối TCP không chặn, do thread của {@link AgentLoop} đọc và ghi. Cùng định dạng khung với {@link RpcCodec}, nên
 * nói chuyện được với SocketRpcClient/SocketRpcServer.
 * <p>
 * Đọc: mỗi lần kênh sẵn sàng, đọc một lần vào bộ đệm rồi tách mọi khung đã đủ. Ghi: {@link #send} chỉ mã hoá khung vào
 * bộ đệm ra; cuối vòng, vòng gọi {@link #flush} một lần cho cả loạt khung của vòng đó. Phần socket chưa nhận hết được
 * ghi tiếp khi kênh báo ghi được. Mọi phương thức chỉ được gọi trên thread của vòng.
 */
final class NioConnection implements AgentLoop.Handler, AgentLoop.Flushable {
    private static final int BUFFER_BYTES = 1 << 16;
    private static final int LENGTH_BYTES = Integer.BYTES;
    // bộ đệm lớn hơn mức này (sau một AppendEntries rất lớn) được thu về kích thước thường khi rỗng
    private static final int MAX_RETAINED_BYTES = 1 << 20;

    interface Listener {
        void onFrame(NioConnection connection, RpcCodec.Frame frame);

        void onClose(NioConnection connection, IOException cause);
    }

    private final AgentLoop loop;
    private final SocketChannel channel;
    private final Listener listener;
    private SelectionKey key;
    private ByteBuffer in = ByteBuffer.allocate(BUFFER_BYTES);
    private ByteBuffer out = ByteBuffer.allocate(BUFFER_BYTES);
    private final ByteArrayOutputStream scratch = new ByteArrayOutputStream();
    private final DataOutputStream outStream = new DataOutputStream(new OutputStream() {
        @Override
        public void write(int b) {
            ensureOut(1);
            out.put((byte) b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            ensureOut(len);
            out.put(b, off, len);
        }
    });
    private boolean connected;
    private boolean dirty;
    private boolean closed;
    private final long createdNanos = System.nanoTime();

    NioConnection(AgentLoop loop, SocketChannel channel, Listener listener) {
        this.loop = loop;
        this.channel = channel;
        this.listener = listener;
    }

    /** Kênh đã kết nối (phía server) */
    void registerConnected() throws IOException {
        connected = true;
        key = loop.register(channel, SelectionKey.OP_READ, this);
    }

    /** Kênh đang kết nối không chặn (phía client); khung gửi trước khi kết nối xong nằm chờ trong bộ đệm ra */
    void registerConnecting() throws IOException {
        key = loop.register(channel, SelectionKey.OP_CONNECT, this);
    }

    boolean isClosed() {
        return closed;
    }

    boolean isConnected() {
        return connected;
    }

    long ageNanos() {
        return System.nanoTime() - createdNanos;
    }

    void send(long requestId, Object message) {
        if (closed) {
            return;
        }
        int start = out.position();
        try {
            RpcCodec.writeFrame(outStream, requestId, message, scratch);
        } catch (IOException e) {
            // message quá lớn: bỏ phần đã ghi dở, kết nối vẫn dùng được
            out.position(start);
            throw new IllegalArgumentException(e);
        } finally {
            if (scratch.size() > MAX_RETAINED_BYTES) {
                scratch.reset();
            }
        }
        if (!dirty && connected) {
            dirty = true;
            loop.markDirty(this);
        }
    }

    @Override
    public void flush() {
        dirty = false;
        if (closed || !connected || out.position() == 0) {
            return;
        }
        try {
            out.flip();
            channel.write(out);
            out.compact();
        } catch (IOException e) {
            close(e);
            return;
        }
        if (out.position() > 0) {
            // socket đầy: ghi tiếp khi nó báo ghi được, trong lúc đó vòng không phải quay vì kết nối này
            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        } else {
            key.interestOps(SelectionKey.OP_READ);
            if (out.capacity() > MAX_RETAINED_BYTES) {
                out = ByteBuffer.allocate(BUFFER_BYTES);
            }
        }
    }

    @Override
    public void onSelected(SelectionKey selected) {
        if (closed) {
            return;
        }
        try {
            if (selected.isConnectable()) {
                channel.finishConnect();
                connected = true;
                key.interestOps(SelectionKey.OP_READ);
                flush(); // các khung xếp hàng trong lúc kết nối
                return;
            }
            if (selected.isWritable()) {
                flush();
            }
            if (selected.isReadable()) {
                read();
            }
        } catch (IOException | RuntimeException e) {
            close(e instanceof IOException io ? io : new IOException(e));
        }
    }

    private void read() throws IOException {
        if (channel.read(in) < 0) {
            close(new IOException("connection closed by peer"));
            return;
        }
        in.flip();
        while (!closed && in.remaining() >= LENGTH_BYTES) {
            int length = in.getInt(in.position());
            if (length <= 0 || length > RpcCodec.MAX_FRAME_BYTES) {
                throw new IOException("Invalid RPC frame length " + length);
            }
            int frameBytes = LENGTH_BYTES + length;
            if (in.remaining() < frameBytes) {
                if (in.capacity() < frameBytes) {
                    // khung lớn hơn bộ đệm: nới ra cho vừa
                    var bigger = ByteBuffer.allocate(frameBytes);
                    bigger.put(in);
                    in = bigger;
                    return;
                }
                break;
            }
            var frame = RpcCodec.read(new DataInputStream(new ByteArrayInputStream(in.array(), in.position(), frameBytes)));
            in.position(in.position() + frameBytes);
            listener.onFrame(this, frame);
        }
        if (closed) {
            return;
        }
        if (!in.hasRemaining() && in.capacity() > MAX_RETAINED_BYTES) {
            in = ByteBuffer.allocate(BUFFER_BYTES);
        } else {
            in.compact();
        }
    }

    void close(IOException cause) {
        if (closed) {
            return;
        }
        closed = true;
        if (key != null) {
            key.cancel();
        }
        try {
            channel.close();
        } catch (IOException e) {
            // Ignore close errors
        }
        listener.onClose(this, cause);
    }

    private void ensureOut(int bytes) {
        if (out.remaining() < bytes) {
            var bigger = ByteBuffer.allocate(Math.max(out.capacity() * 2, out.position() + bytes));
            out.flip();
            bigger.put(out);
            out = bigger;
        }
    }
}

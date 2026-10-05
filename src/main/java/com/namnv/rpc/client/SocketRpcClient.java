package com.namnv.rpc.client;

import com.namnv.rpc.FrameWriter;
import com.namnv.rpc.RpcCodec;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.InstallSnapshotRequest;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.ReadIndexRequest;
import com.namnv.rpc.model.request.RequestVoteRequest;
import com.namnv.rpc.model.request.TimeoutNowRequest;
import com.namnv.rpc.model.response.AppendEntriesResponse;
import com.namnv.rpc.model.response.InstallSnapshotResponse;
import com.namnv.rpc.model.response.PreVoteResponse;
import com.namnv.rpc.model.response.ReadIndexResponse;
import com.namnv.rpc.model.response.RequestVoteResponse;
import com.namnv.rpc.model.response.TimeoutNowResponse;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Transport TCP. Mỗi node đích một kết nối dùng lại; mọi lời gọi tới node đó đi chung kết nối này và không chờ nhau:
 * mỗi request mang một id, một thread đọc response về và trả cho đúng lời gọi theo id.
 */
public class SocketRpcClient implements RpcProcessor, AutoCloseable {
    // đủ lớn để nhiều message nhỏ đi chung một lần ghi/đọc socket
    private static final int BUFFER_BYTES = 1 << 16;

    private final int timeoutMs;
    // null: kết nối không mã hoá
    private final SSLContext sslContext;
    private final ExecutorService rpcExecutor;
    private final Map<String, Connection> connections = new ConcurrentHashMap<>();
    private volatile boolean closed;

    public SocketRpcClient(int timeoutMs) {
        this(timeoutMs, null);
    }

    /**
     * @param timeoutMs  thời hạn cho việc kết nối và cho mỗi lời gọi
     * @param sslContext TLS với chứng chỉ của node này; server chỉ nhận client có chứng chỉ mà nó tin
     */
    public SocketRpcClient(int timeoutMs, SSLContext sslContext) {
        this.timeoutMs = timeoutMs;
        this.sslContext = sslContext;
        rpcExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(String address, RequestVoteRequest request) {
        return sendRPC(address, request, RequestVoteResponse.class);
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(String address, AppendEntriesRequest request) {
        return sendRPC(address, request, AppendEntriesResponse.class);
    }

    @Override
    public CompletableFuture<PreVoteResponse> preVote(String address, PreVoteRequest request) {
        return sendRPC(address, request, PreVoteResponse.class);
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(String address, InstallSnapshotRequest request) {
        return sendRPC(address, request, InstallSnapshotResponse.class);
    }

    @Override
    public CompletableFuture<TimeoutNowResponse> timeoutNow(String address, TimeoutNowRequest request) {
        return sendRPC(address, request, TimeoutNowResponse.class);
    }

    @Override
    public CompletableFuture<ReadIndexResponse> readIndex(String address, ReadIndexRequest request) {
        return sendRPC(address, request, ReadIndexResponse.class);
    }

    private <T> CompletableFuture<T> sendRPC(String address, Object request, Class<T> responseType) {
        return send(address, request, responseType);
    }

    /**
     * Gửi một message bất kỳ mà {@link RpcCodec} biết tới node ở {@code address} và chờ response kiểu {@code responseType}.
     */
    public <T> CompletableFuture<T> send(String address, Object request, Class<T> responseType) {
        var result = new CompletableFuture<Object>();
        // Người gọi (đang giữ lock của node) không bao giờ bị chặn bởi mạng: kết nối đã có thì request chỉ được xếp vào
        // hàng ghi của nó, chưa có thì việc kết nối chạy ở thread riêng.
        var connection = connections.get(address);
        if (connection == null || !connection.trySend(request, result)) {
            rpcExecutor.execute(() -> connections.computeIfAbsent(address, Connection::new).send(request, result, true));
        }
        return result.orTimeout(timeoutMs, TimeUnit.MILLISECONDS).thenApply(responseType::cast);
    }

    @Override
    public void close() {
        closed = true;
        connections.values().forEach(Connection::close);
        connections.clear();
        rpcExecutor.shutdownNow();
    }

    // địa chỉ của một node cùng kết nối hiện tại tới nó (được thay bằng kết nối mới khi kết nối cũ hỏng)
    private class Connection {
        private final String address;
        private final ReentrantLock lock = new ReentrantLock();
        private volatile Link link;

        Connection(String address) {
            this.address = address;
        }

        // gửi trên kết nối đang mở mà không chờ gì; false nếu phải kết nối (lại) trước
        boolean trySend(Object request, CompletableFuture<Object> result) {
            Link current = link;
            if (current == null || current.closed) {
                return false;
            }
            call(current, true, request, result, true);
            return true;
        }

        void send(Object request, CompletableFuture<Object> result, boolean mayRetry) {
            Link current;
            boolean reused;
            lock.lock();
            try {
                reused = link != null && !link.closed;
                if (!reused) {
                    link = new Link(address);
                }
                current = link;
            } catch (IOException e) {
                result.completeExceptionally(e);
                return;
            } finally {
                lock.unlock();
            }

            call(current, reused, request, result, mayRetry);
        }

        private void call(Link current, boolean reused, Object request, CompletableFuture<Object> result, boolean mayRetry) {
            var response = current.call(request);
            // lời gọi hết hạn hoặc bị huỷ thì không giữ chỗ chờ response của nó nữa
            result.whenComplete((value, error) -> response.cancel(false));
            response.whenComplete((value, error) -> {
                if (error == null) {
                    result.complete(value);
                } else if (reused && mayRetry && !closed && !result.isDone()) {
                    // kết nối dùng lại có thể đã bị phía kia đóng từ trước: thử lại một lần trên kết nối mới
                    rpcExecutor.execute(() -> send(request, result, false));
                } else {
                    result.completeExceptionally(error);
                }
            });
        }

        void close() {
            lock.lock();
            try {
                if (link != null) {
                    link.close(new IOException("client closed"));
                }
            } finally {
                lock.unlock();
            }
        }
    }

    // một kết nối TCP: nhiều lời gọi ghi xen kẽ vào, một thread đọc response và ghép lại theo id
    private class Link {
        private final Socket socket;
        private final FrameWriter writer;
        private final Map<Long, CompletableFuture<Object>> pending = new ConcurrentHashMap<>();
        private final AtomicLong nextId = new AtomicLong();
        volatile boolean closed;

        Link(String address) throws IOException {
            String[] parts = address.split(":");
            Socket s = sslContext != null ? sslContext.getSocketFactory().createSocket() : new Socket();
            try {
                s.connect(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])), timeoutMs);
                s.setTcpNoDelay(true);
                if (s instanceof SSLSocket tls) {
                    tls.setSoTimeout(timeoutMs);
                    tls.startHandshake();
                }
                // thread đọc chờ response bao lâu cũng được; thời hạn của từng lời gọi do future của nó lo
                s.setSoTimeout(0);
                writer = new FrameWriter(new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), BUFFER_BYTES)),
                        rpcExecutor, this::close);
                var in = new DataInputStream(new BufferedInputStream(s.getInputStream(), BUFFER_BYTES));
                socket = s;
                rpcExecutor.execute(() -> readResponses(in));
            } catch (IOException | RuntimeException e) {
                s.close();
                throw e instanceof IOException io ? io : new IOException(e);
            }
        }

        CompletableFuture<Object> call(Object request) {
            long id = nextId.incrementAndGet();
            var response = new CompletableFuture<Object>();
            pending.put(id, response);
            response.whenComplete((value, error) -> pending.remove(id));
            if (!closed) {
                writer.send(id, request);
            }
            // kết nối đóng ngay trước hoặc ngay sau khi xếp hàng thì không ai còn báo lỗi cho lời gọi này
            if (closed) {
                response.completeExceptionally(new IOException("connection closed"));
            }
            return response;
        }

        private void readResponses(DataInputStream in) {
            try {
                while (!closed) {
                    var frame = RpcCodec.read(in);
                    // response của lời gọi đã hết hạn thì không còn ai chờ, bỏ qua
                    var response = pending.remove(frame.requestId());
                    if (response != null) {
                        response.complete(frame.message());
                    }
                }
            } catch (IOException | RuntimeException e) {
                close(e);
            }
        }

        // đóng kết nối và báo lỗi cho mọi lời gọi còn đang chờ trên nó
        void close(Exception cause) {
            closed = true;
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore close errors
            }
            for (var response : pending.values()) {
                response.completeExceptionally(new IOException("connection closed", cause));
            }
        }
    }
}

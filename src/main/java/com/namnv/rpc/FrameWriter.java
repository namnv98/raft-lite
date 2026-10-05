package com.namnv.rpc;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Phía ghi của một kết nối. {@link #send} không bao giờ chờ mạng: message được xếp hàng, và một thread duy nhất ghi lần lượt
 * mọi message đang chờ rồi mới flush một lần. Khi nhiều message đến dồn dập (response của hàng nghìn lệnh vừa commit,
 * request của nhiều client đi chung kết nối) chúng ra socket trong một lời gọi hệ thống thay vì mỗi message một lời gọi.
 */
public final class FrameWriter {
    // bộ đệm mã hoá lớn hơn mức này (một AppendEntries rất lớn) không được giữ lại cho lần sau
    private static final int MAX_RETAINED_SCRATCH_BYTES = 1 << 20;

    private record Outgoing(long requestId, Object message) {
    }

    private final DataOutputStream out;
    private final Executor executor;
    private final Consumer<IOException> onFailure;
    private final ConcurrentLinkedQueue<Outgoing> queue = new ConcurrentLinkedQueue<>();
    // số message đã xếp hàng mà thread ghi chưa ghi nhận; 0 nghĩa là không có thread ghi nào đang chạy
    private final AtomicInteger waiting = new AtomicInteger();
    // chỉ thread ghi dùng
    private ByteArrayOutputStream scratch = new ByteArrayOutputStream();

    /**
     * @param executor  nơi chạy thread ghi mỗi khi hàng đợi có việc
     * @param onFailure gọi một lần khi không ghi được nữa; kết nối phải được đóng, các message sau đó bị bỏ
     */
    public FrameWriter(DataOutputStream out, Executor executor, Consumer<IOException> onFailure) {
        this.out = out;
        this.executor = executor;
        this.onFailure = onFailure;
    }

    public void send(long requestId, Object message) {
        queue.add(new Outgoing(requestId, message));
        if (waiting.getAndIncrement() == 0) {
            try {
                executor.execute(this::drain);
            } catch (RejectedExecutionException e) {
                onFailure.accept(new IOException("connection closed", e));
            }
        }
    }

    private void drain() {
        int seen = 1;
        try {
            while (true) {
                Outgoing next;
                while ((next = queue.poll()) != null) {
                    RpcCodec.writeFrame(out, next.requestId(), next.message(), scratch);
                    if (scratch.size() > MAX_RETAINED_SCRATCH_BYTES) {
                        scratch = new ByteArrayOutputStream();
                    }
                }
                out.flush();
                // message xếp hàng trong lúc đang ghi không khởi động thread mới, nên phải quay lại lấy chúng
                seen = waiting.addAndGet(-seen);
                if (seen == 0) {
                    return;
                }
            }
        } catch (IOException | RuntimeException e) {
            // waiting không về 0 nữa: không thread ghi nào được khởi động lại trên kết nối đã hỏng
            onFailure.accept(e instanceof IOException io ? io : new IOException(e));
        }
    }
}

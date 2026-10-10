package com.namnv.ledger.gateway;

import com.namnv.ledger.client.LedgerClient;
import com.namnv.ledger.model.LedgerResult;
import com.namnv.ledger.model.LedgerTransfer;
import com.namnv.raft.UnknownOutcomeException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Gom các giao dịch gửi lẻ (mỗi request HTTP một giao dịch) thành lô cho {@link LedgerClient#transfers}, kiểu Nagle
 * tự thích nghi: không bao giờ chờ theo thời gian. Khi số lô đang bay dưới {@code maxInflight}, giao dịch được gửi
 * ngay (tải thấp: độ trễ bằng một lần gửi lẻ); khi đã đủ, giao dịch mới xếp hàng và đi cùng nhau trong lô kế tiếp, ngay
 * khi một lô trước đó trả lời (tải cao: lô tự lớn lên, tới {@code maxBatch}).
 * <p>
 * Mỗi giao dịch vẫn được kiểm tra và ghi riêng: một giao dịch bị từ chối không ảnh hưởng các giao dịch khác trong lô.
 * Khi không rõ kết quả của cả lô (leader đổi, hết thời gian), mọi giao dịch trong lô nhận {@link UnknownOutcomeException};
 * gửi lại cùng id là an toàn.
 */
public final class TransferBatcher {
    private static final UnknownOutcomeException OVERLOADED = new UnknownOutcomeException("gateway queue is full");

    private final LedgerClient client;
    private final int maxBatch;
    private final int maxInflight;
    private final int maxQueued;

    private final Object lock = new Object();
    private List<Pending> queue = new ArrayList<>();
    private int inflight;
    private long batches;
    private long transfers;

    private record Pending(LedgerTransfer transfer, CompletableFuture<LedgerResult> result) {
    }

    public TransferBatcher(LedgerClient client, int maxBatch, int maxInflight, int maxQueued) {
        this.client = client;
        this.maxBatch = maxBatch;
        this.maxInflight = maxInflight;
        this.maxQueued = maxQueued;
    }

    public CompletableFuture<LedgerResult> submit(LedgerTransfer transfer) {
        var result = new CompletableFuture<LedgerResult>();
        List<Pending> batch;
        synchronized (lock) {
            if (queue.size() >= maxQueued) {
                return CompletableFuture.failedFuture(OVERLOADED);
            }
            queue.add(new Pending(transfer, result));
            batch = takeBatch();
        }
        if (batch != null) {
            send(batch);
        }
        return result;
    }

    // gọi khi đang giữ lock
    private List<Pending> takeBatch() {
        if (inflight >= maxInflight || queue.isEmpty()) {
            return null;
        }
        inflight++;
        List<Pending> batch;
        if (queue.size() <= maxBatch) {
            batch = queue;
            queue = new ArrayList<>();
        } else {
            var head = queue.subList(0, maxBatch);
            batch = new ArrayList<>(head);
            head.clear();
        }
        batches++;
        transfers += batch.size();
        return batch;
    }

    private void send(List<Pending> batch) {
        var list = new ArrayList<LedgerTransfer>(batch.size());
        batch.forEach(pending -> list.add(pending.transfer));
        client.transfers(list).whenComplete((results, error) -> {
            List<Pending> next;
            synchronized (lock) {
                inflight--;
                next = takeBatch();
            }
            // lô kế tiếp đi trước, rồi mới trả lời lô này: không để hàng đợi chờ các callback
            if (next != null) {
                send(next);
            }
            for (int i = 0; i < batch.size(); i++) {
                if (error != null) {
                    batch.get(i).result.completeExceptionally(error);
                } else {
                    batch.get(i).result.complete(results.get(i));
                }
            }
        });
    }

    /** số giao dịch trung bình mỗi lô từ lúc khởi động */
    public double averageBatch() {
        synchronized (lock) {
            return batches == 0 ? 0 : (double) transfers / batches;
        }
    }

    /** {số lô đã gửi, số giao dịch trong các lô đó} từ lúc khởi động */
    public long[] counters() {
        synchronized (lock) {
            return new long[]{batches, transfers};
        }
    }
}

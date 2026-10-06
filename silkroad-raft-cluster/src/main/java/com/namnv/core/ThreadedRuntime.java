package com.namnv.core;

import com.namnv.agent.AgentLoop;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ThreadedRuntime implements RaftRuntime {
    private final ScheduledExecutorService timers = Executors.newScheduledThreadPool(2);
    private final ExecutorService io = Executors.newSingleThreadExecutor();
    // thread riêng cho việc đĩa không nằm trên đường trả lời client (đọc log cũ cho một follower tụt xa, xoá phần log đã
    // compact), để chúng không làm các lần fsync của lệnh mới phải chờ
    private final ExecutorService reads = Executors.newSingleThreadExecutor();
    // thread xử lý của node (mô hình một thread cho mỗi node, như agent của Aeron); transport NIO cũng chạy trên vòng này
    private final AgentLoop node = new AgentLoop("raft-node");
    private final Random random = new Random();

    @Override
    public long nanoTime() {
        return System.nanoTime();
    }

    @Override
    public ScheduledTask schedule(Runnable task, long delayMs) {
        try {
            var future = timers.schedule(() -> run(task), delayMs, TimeUnit.MILLISECONDS);
            // không interrupt: callback có thể đang ghi đĩa
            return () -> future.cancel(false);
        } catch (RejectedExecutionException e) {
            return () -> {
            };
        }
    }

    @Override
    public void executeIo(Runnable task) {
        try {
            io.execute(() -> run(task));
        } catch (RejectedExecutionException e) {
            // đã shutdown
        }
    }

    @Override
    public void executeRead(Runnable task) {
        try {
            reads.execute(() -> run(task));
        } catch (RejectedExecutionException e) {
            // đã shutdown
        }
    }

    /** Vòng của node, để NioRpcServer/NioRpcClient đọc ghi socket ngay trên thread của node */
    public AgentLoop loop() {
        return node;
    }

    @Override
    public void executeNode(Runnable task) {
        node.execute(task);
    }

    private void run(Runnable task) {
        try {
            task.run();
        } catch (Exception e) {
            log.error("Raft task failed", e);
        }
    }

    @Override
    public int nextInt(int bound) {
        return random.nextInt(bound);
    }

    @Override
    public void shutdown() {
        timers.shutdownNow();
        // task ghi đĩa đã xếp hàng vẫn được chạy nốt
        io.shutdown();
        reads.shutdownNow();
        // sự kiện đã xếp hàng vẫn được xử lý nốt: chúng thấy node đã dừng và báo lỗi cho người đang chờ
        node.close();
    }
}

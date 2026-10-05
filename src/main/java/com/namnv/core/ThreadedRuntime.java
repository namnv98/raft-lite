package com.namnv.core;

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
    }
}

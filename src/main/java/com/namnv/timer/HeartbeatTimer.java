package com.namnv.timer;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


@Slf4j
public class HeartbeatTimer {
    private final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
    private final int intervalMs;
    private final Runnable task;
    private ScheduledFuture<?> current;


    public HeartbeatTimer(int intervalMs, Runnable task) {
        this.intervalMs = intervalMs;
        this.task = task;
    }


    public synchronized void start() {
        if (current != null || exec.isShutdown()) return;
        current = exec.scheduleAtFixedRate(this::runTask, 0, intervalMs, TimeUnit.MILLISECONDS);
    }

    // scheduleAtFixedRate dừng hẳn nếu task ném exception, nên phải bắt ở đây
    private void runTask() {
        try {
            task.run();
        } catch (Exception e) {
            log.error("Heartbeat task failed", e);
        }
    }


    public synchronized void stop() {
        if (current != null) current.cancel(false);
        current = null;
    }

    public synchronized void shutdown() {
        exec.shutdownNow();
    }
}

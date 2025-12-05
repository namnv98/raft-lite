package com.namnv.timer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


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
        if (current != null) return;
        current = exec.scheduleAtFixedRate(task, 0, intervalMs, TimeUnit.MILLISECONDS);
    }


    public synchronized void stop() {
        if (current != null) current.cancel(false);
        current = null;
    }
}

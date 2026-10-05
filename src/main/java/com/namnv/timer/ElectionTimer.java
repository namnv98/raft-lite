package com.namnv.timer;


import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ElectionTimer {
    private final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
    private final int minTimeoutMs;
    private final int maxTimeoutMs;
    private final Runnable onTimeout;
    private final Random rand = new Random();
    private ScheduledFuture<?> current;

    public ElectionTimer(int minTimeoutMs, int maxTimeoutMs, Runnable onTimeout) {
        this.minTimeoutMs = minTimeoutMs;
        this.maxTimeoutMs = maxTimeoutMs;
        this.onTimeout = onTimeout;
    }

    public synchronized void start() {
        reset();
    }

    public synchronized void reset() {
        if (exec.isShutdown()) return;
        // không interrupt: callback có thể đang ghi đĩa
        if (current != null) current.cancel(false);
        int timeout = minTimeoutMs + rand.nextInt(maxTimeoutMs - minTimeoutMs + 1);
        current = exec.schedule(onTimeout, timeout, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        if (current != null) current.cancel(false);
    }

    public synchronized void shutdown() {
        exec.shutdownNow();
    }

}

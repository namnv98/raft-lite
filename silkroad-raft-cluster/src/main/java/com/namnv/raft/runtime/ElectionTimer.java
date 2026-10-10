package com.namnv.raft.runtime;



public class ElectionTimer {
    private final RaftRuntime runtime;
    private final int minTimeoutMs;
    private final int maxTimeoutMs;
    private final Runnable onTimeout;
    private RaftRuntime.ScheduledTask current;

    public ElectionTimer(RaftRuntime runtime, int minTimeoutMs, int maxTimeoutMs, Runnable onTimeout) {
        this.runtime = runtime;
        this.minTimeoutMs = minTimeoutMs;
        this.maxTimeoutMs = maxTimeoutMs;
        this.onTimeout = onTimeout;
    }

    public synchronized void start() {
        reset();
    }

    public synchronized void reset() {
        if (current != null) current.cancel();
        int timeout = minTimeoutMs + runtime.nextInt(maxTimeoutMs - minTimeoutMs + 1);
        current = runtime.schedule(onTimeout, timeout);
    }

    public synchronized void stop() {
        if (current != null) current.cancel();
    }

}

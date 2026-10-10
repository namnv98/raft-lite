package com.namnv.raft.runtime;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class HeartbeatTimer {
    private final RaftRuntime runtime;
    private final int intervalMs;
    private final Runnable task;
    private RaftRuntime.ScheduledTask current;
    // tăng mỗi lần start/stop để một tick của lần chạy cũ không tự hẹn lại
    private long generation;
    private boolean running;


    public HeartbeatTimer(RaftRuntime runtime, int intervalMs, Runnable task) {
        this.runtime = runtime;
        this.intervalMs = intervalMs;
        this.task = task;
    }


    public synchronized void start() {
        if (running) return;
        running = true;
        long startedGeneration = ++generation;
        current = runtime.schedule(() -> tick(startedGeneration), 0);
    }

    private void tick(long tickGeneration) {
        synchronized (this) {
            if (!running || tickGeneration != generation) return;
        }
        // không giữ monitor khi chạy task: task lấy lock của node, còn node gọi stop() khi đang giữ lock đó
        try {
            task.run();
        } catch (Exception e) {
            log.error("Heartbeat task failed", e);
        }
        synchronized (this) {
            if (running && tickGeneration == generation) {
                current = runtime.schedule(() -> tick(tickGeneration), intervalMs);
            }
        }
    }


    public synchronized void stop() {
        running = false;
        generation++;
        if (current != null) current.cancel();
        current = null;
    }
}

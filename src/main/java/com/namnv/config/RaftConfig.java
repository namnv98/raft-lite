package com.namnv.config;

public class RaftConfig {
    private final int electionTimeoutMinMs;
    private final int electionTimeoutMaxMs;
    private final int heartbeatIntervalMs;

    public RaftConfig(int electionTimeoutMinMs, int electionTimeoutMaxMs, int heartbeatIntervalMs) {
        if (electionTimeoutMinMs > electionTimeoutMaxMs) {
            throw new IllegalArgumentException("min timeout phải <= max timeout");
        }
        this.electionTimeoutMinMs = electionTimeoutMinMs;
        this.electionTimeoutMaxMs = electionTimeoutMaxMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public int getElectionTimeoutMinMs() {
        return electionTimeoutMinMs;
    }

    public int getElectionTimeoutMaxMs() {
        return electionTimeoutMaxMs;
    }

    public int getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }
}

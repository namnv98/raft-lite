package com.namnv.state;

public class VolatileState {
    private long commitIndex = 0;
    private long lastApplied = 0;

    public synchronized long getCommitIndex() {
        return commitIndex;
    }

    public synchronized void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public synchronized long getLastApplied() {
        return lastApplied;
    }

    public synchronized void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }
}
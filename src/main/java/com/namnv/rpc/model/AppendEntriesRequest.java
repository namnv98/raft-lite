package com.namnv.rpc.model;


import com.namnv.entity.LogEntry;

import java.util.List;


public class AppendEntriesRequest {
    public final long term;
    public final String leaderId;
    public final long prevLogIndex;
    public final long prevLogTerm;
    public final List<LogEntry> entries;
    public final long leaderCommit;


    public AppendEntriesRequest(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                                List<LogEntry> entries, long leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }
}
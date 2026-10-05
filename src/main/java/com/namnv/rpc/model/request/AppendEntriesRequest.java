package com.namnv.rpc.model.request;


import com.namnv.entity.LogEntry;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;


public class AppendEntriesRequest implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public final long term;
    public final String leaderId;
    public final long prevLogIndex;
    public final long prevLogTerm;
    public final List<LogEntry> entries;
    public final long leaderCommit;


    @JsonCreator
    public AppendEntriesRequest(@JsonProperty("term") long term,
            @JsonProperty("leaderId") String leaderId,
            @JsonProperty("prevLogIndex") long prevLogIndex,
            @JsonProperty("prevLogTerm") long prevLogTerm,
            @JsonProperty("entries") List<LogEntry> entries,
            @JsonProperty("leaderCommit") long leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }
}
package com.namnv.rpc.model;

public class PreVoteRequest {
    public final long term;
    public final String candidateId;
    public final long lastLogIndex;
    public final long lastLogTerm;

    public PreVoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
}


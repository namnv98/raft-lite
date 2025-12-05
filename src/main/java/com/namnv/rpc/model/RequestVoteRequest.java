package com.namnv.rpc.model;



public class RequestVoteRequest {
    public final long term;
    public final String candidateId;
    public final long lastLogIndex;
    public final long lastLogTerm;


    public RequestVoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
}
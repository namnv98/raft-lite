package com.namnv.rpc.model;

public class RequestVoteResponse {
    public final long term;
    public final boolean voteGranted;


    public RequestVoteResponse(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }
}

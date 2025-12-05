package com.namnv.rpc.model;

public class PreVoteResponse {
    public final long term;
    public final boolean voteGranted;

    public PreVoteResponse(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }
}

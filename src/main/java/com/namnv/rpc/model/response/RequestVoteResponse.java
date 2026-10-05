package com.namnv.rpc.model.response;

import java.io.Serial;
import java.io.Serializable;

public class RequestVoteResponse implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    public final long term;
    public final boolean voteGranted;

    public RequestVoteResponse(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }
}

package com.namnv.rpc.model.request;

import java.io.Serial;
import java.io.Serializable;

public class PreVoteRequest implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
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


package com.namnv.rpc.model.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;

public class PreVoteRequest implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    public final long term;
    public final String candidateId;
    public final long lastLogIndex;
    public final long lastLogTerm;

    @JsonCreator
    public PreVoteRequest(@JsonProperty("term") long term,
            @JsonProperty("candidateId") String candidateId,
            @JsonProperty("lastLogIndex") long lastLogIndex,
            @JsonProperty("lastLogTerm") long lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
}


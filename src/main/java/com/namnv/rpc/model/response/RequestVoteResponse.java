package com.namnv.rpc.model.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;

public class RequestVoteResponse implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    public final long term;
    public final boolean voteGranted;

    @JsonCreator
    public RequestVoteResponse(@JsonProperty("term") long term,
            @JsonProperty("voteGranted") boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }
}

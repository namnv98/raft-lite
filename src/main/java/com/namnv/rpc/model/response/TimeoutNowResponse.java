package com.namnv.rpc.model.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;

public class TimeoutNowResponse implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public final long term;
    public final boolean success;

    @JsonCreator
    public TimeoutNowResponse(@JsonProperty("term") long term,
            @JsonProperty("success") boolean success) {
        this.term = term;
        this.success = success;
    }
}

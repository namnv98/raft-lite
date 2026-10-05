package com.namnv.rpc.model.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;

public class ReadIndexResponse implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    // false: node được hỏi không phải leader, hoặc không xác nhận được quyền leader với đa số
    public final boolean success;
    public final long readIndex;
    // leader mà node được hỏi đang biết, có thể null
    public final String leaderId;

    @JsonCreator
    public ReadIndexResponse(@JsonProperty("success") boolean success,
            @JsonProperty("readIndex") long readIndex,
            @JsonProperty("leaderId") String leaderId) {
        this.success = success;
        this.readIndex = readIndex;
        this.leaderId = leaderId;
    }
}

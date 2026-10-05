package com.namnv.rpc.model.response;

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

    public ReadIndexResponse(boolean success, long readIndex, String leaderId) {
        this.success = success;
        this.readIndex = readIndex;
        this.leaderId = leaderId;
    }
}

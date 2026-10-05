package com.namnv.rpc.model.response;

import java.io.Serial;
import java.io.Serializable;

public class TimeoutNowResponse implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public final long term;
    public final boolean success;

    public TimeoutNowResponse(long term, boolean success) {
        this.term = term;
        this.success = success;
    }
}

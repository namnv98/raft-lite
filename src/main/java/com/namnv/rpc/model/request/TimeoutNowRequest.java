package com.namnv.rpc.model.request;

import java.io.Serial;
import java.io.Serializable;

// leader yêu cầu một follower bầu cử ngay để nhận quyền leader
public class TimeoutNowRequest implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public final long term;
    public final String leaderId;

    public TimeoutNowRequest(long term, String leaderId) {
        this.term = term;
        this.leaderId = leaderId;
    }
}

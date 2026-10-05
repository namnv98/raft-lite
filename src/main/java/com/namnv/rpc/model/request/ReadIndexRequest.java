package com.namnv.rpc.model.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;

// follower hỏi leader: tôi phải apply tới index nào thì đọc được dữ liệu mới nhất?
public class ReadIndexRequest implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public final String requesterId;

    @JsonCreator
    public ReadIndexRequest(@JsonProperty("requesterId") String requesterId) {
        this.requesterId = requesterId;
    }
}

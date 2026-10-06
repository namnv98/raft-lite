package com.namnv.rpc.model.request;

import java.io.Serial;
import java.io.Serializable;

// follower hỏi leader: tôi phải apply tới index nào thì đọc được dữ liệu mới nhất?
public class ReadIndexRequest implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public final String requesterId;

    public ReadIndexRequest(String requesterId) {
        this.requesterId = requesterId;
    }
}

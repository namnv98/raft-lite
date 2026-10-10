package com.namnv.raft;

import lombok.Getter;

/**
 * Node không (còn) là leader nên không phục vụ được yêu cầu; client nên thử lại ở leaderId nếu có.
 */
@Getter
public class NotLeaderException extends RuntimeException {
    // leader mà node này biết, null nếu chưa biết
    private final String leaderId;

    public NotLeaderException(String nodeId, String leaderId) {
        super("Node " + nodeId + " is not the leader" + (leaderId != null ? ", try " + leaderId : ""));
        this.leaderId = leaderId;
    }
}

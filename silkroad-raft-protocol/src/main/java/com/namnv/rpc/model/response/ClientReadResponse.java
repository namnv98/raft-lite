package com.namnv.rpc.model.response;

public class ClientReadResponse {
    public final boolean success;
    // leader mà node này biết khi success = false; có thể null
    public final String leaderId;
    public final byte[] result;

    public ClientReadResponse(boolean success, String leaderId, byte[] result) {
        this.success = success;
        this.leaderId = leaderId;
        this.result = result;
    }
}

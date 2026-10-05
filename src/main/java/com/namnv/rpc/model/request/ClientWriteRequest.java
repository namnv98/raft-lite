package com.namnv.rpc.model.request;

// client bên ngoài gửi một lệnh ghi; clientId null nghĩa là không cần chống ghi trùng
public class ClientWriteRequest {
    public final String clientId;
    public final long sequence;
    public final byte[] command;

    public ClientWriteRequest(String clientId, long sequence, byte[] command) {
        this.clientId = clientId;
        this.sequence = sequence;
        this.command = command;
    }
}

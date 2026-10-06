package com.namnv.rpc.model.request;

// client bên ngoài gửi một lệnh ghi; clientId null nghĩa là không cần chống ghi trùng.
// batch: command là một lô nhiều lệnh (CommandBatch) đi chung một entry của log
public class ClientWriteRequest {
    public final String clientId;
    public final long sequence;
    public final byte[] command;
    public final boolean batch;

    public ClientWriteRequest(String clientId, long sequence, byte[] command) {
        this(clientId, sequence, command, false);
    }

    public ClientWriteRequest(String clientId, long sequence, byte[] command, boolean batch) {
        this.clientId = clientId;
        this.sequence = sequence;
        this.command = command;
        this.batch = batch;
    }
}

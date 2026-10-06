package com.namnv.rpc.model.response;

public class ClientWriteResponse {
    // false: không rõ kết quả (node không phải leader, mất quyền, quá hạn, quá tải); client nên gửi lại
    public final boolean success;
    // leader mà node này biết, để client chuyển hướng; có thể null
    public final String leaderId;
    // kết quả mà state machine trả cho lệnh (StateMachine.onApplyWithResult); null nếu không có
    public final byte[] result;

    public ClientWriteResponse(boolean success, String leaderId) {
        this(success, leaderId, null);
    }

    public ClientWriteResponse(boolean success, String leaderId, byte[] result) {
        this.success = success;
        this.leaderId = leaderId;
        this.result = result;
    }
}

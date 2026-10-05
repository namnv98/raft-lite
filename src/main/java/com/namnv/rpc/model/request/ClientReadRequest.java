package com.namnv.rpc.model.request;

// client bên ngoài yêu cầu một lần đọc nhất quán; query là dữ liệu tuỳ ý mà ứng dụng tự hiểu
public class ClientReadRequest {
    public final byte[] query;

    public ClientReadRequest(byte[] query) {
        this.query = query;
    }
}

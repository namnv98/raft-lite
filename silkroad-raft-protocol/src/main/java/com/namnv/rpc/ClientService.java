package com.namnv.rpc;

import com.namnv.rpc.model.request.ClientReadRequest;
import com.namnv.rpc.model.request.ClientWriteRequest;
import com.namnv.rpc.model.response.ClientReadResponse;
import com.namnv.rpc.model.response.ClientWriteResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Các yêu cầu mà client bên ngoài gửi tới một node qua mạng.
 */
public interface ClientService {
    CompletableFuture<ClientWriteResponse> handleClientWrite(ClientWriteRequest request);

    CompletableFuture<ClientReadResponse> handleClientRead(ClientReadRequest request);
}

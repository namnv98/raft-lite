package com.namnv.core;

import com.namnv.rpc.ClientService;
import com.namnv.rpc.model.request.ClientReadRequest;
import com.namnv.rpc.model.request.ClientWriteRequest;
import com.namnv.rpc.model.response.ClientReadResponse;
import com.namnv.rpc.model.response.ClientWriteResponse;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Nối yêu cầu của client bên ngoài vào một {@link RaftNode}: lệnh ghi đi qua log, lệnh đọc đi qua đọc nhất quán.
 */
public class RaftClientService implements ClientService {
    private final RaftNode node;
    private final Function<byte[], byte[]> queryHandler;

    /**
     * @param queryHandler trả lời một câu hỏi của client từ state machine của node này. Được gọi khi node đang giữ lock,
     *                     sau khi state machine đã có mọi lệnh được xác nhận trước lúc yêu cầu đọc tới nơi, nên cần nhanh.
     */
    public RaftClientService(RaftNode node, Function<byte[], byte[]> queryHandler) {
        this.node = node;
        this.queryHandler = queryHandler;
    }

    @Override
    public CompletableFuture<ClientWriteResponse> handleClientWrite(ClientWriteRequest request) {
        return node.appendClientCommand(request.clientId, request.sequence, request.command, request.batch)
                .handle((ok, error) -> new ClientWriteResponse(Boolean.TRUE.equals(ok), node.getLeaderId()));
    }

    @Override
    public CompletableFuture<ClientReadResponse> handleClientRead(ClientReadRequest request) {
        return node.read(() -> queryHandler.apply(request.query))
                .handle((result, error) -> error == null
                        ? new ClientReadResponse(true, null, result)
                        : new ClientReadResponse(false, node.getLeaderId(), null));
    }
}

package com.namnv.rpc;

import java.util.concurrent.CompletableFuture;

/**
 * Gửi một message bất kỳ mà {@link com.namnv.rpc.RpcCodec} biết tới một node và chờ response của nó.
 * {@link SocketRpcClient} và {@link com.namnv.transport.nio.NioRpcClient} đều là transport như vậy.
 */
public interface MessageTransport extends AutoCloseable {
    <T> CompletableFuture<T> send(String address, Object request, Class<T> responseType);

    @Override
    void close();
}

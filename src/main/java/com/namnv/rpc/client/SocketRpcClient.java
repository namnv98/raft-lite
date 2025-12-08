package com.namnv.rpc.client;

import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.InstallSnapshotRequest;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.RequestVoteRequest;
import com.namnv.rpc.model.response.AppendEntriesResponse;
import com.namnv.rpc.model.response.InstallSnapshotResponse;
import com.namnv.rpc.model.response.PreVoteResponse;
import com.namnv.rpc.model.response.RequestVoteResponse;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SocketRpcClient implements RpcProcessor {
    private final int timeoutMs;
    private final ExecutorService rpcExecutor;

    public SocketRpcClient(int timeoutMs) {
        this.timeoutMs = timeoutMs;
        rpcExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(String address, RequestVoteRequest request) {
        return sendRPC(address, request, RequestVoteResponse.class);
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(String address, AppendEntriesRequest request) {
        return sendRPC(address, request, AppendEntriesResponse.class);
    }

    @Override
    public CompletableFuture<PreVoteResponse> preVote(String address, PreVoteRequest request) {
        return sendRPC(address, request, PreVoteResponse.class);
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(String address, InstallSnapshotRequest request) {
        return sendRPC(address, request, InstallSnapshotResponse.class);
    }

    private <T> CompletableFuture<T> sendRPC(String address, Object request, Class<T> responseType) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String[] parts = address.split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);

                try (Socket socket = new Socket(host, port);
                     ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                     ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                    socket.setSoTimeout(timeoutMs);
                    out.writeObject(request);
                    out.flush();

                    Object response = in.readObject();
                    return responseType.cast(response);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, rpcExecutor);
    }
}

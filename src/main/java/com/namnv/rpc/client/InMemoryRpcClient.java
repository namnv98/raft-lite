package com.namnv.rpc.client;


import com.namnv.rpc.RaftServerService;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.InstallSnapshotRequest;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.ReadIndexRequest;
import com.namnv.rpc.model.request.RequestVoteRequest;
import com.namnv.rpc.model.request.TimeoutNowRequest;
import com.namnv.rpc.model.response.AppendEntriesResponse;
import com.namnv.rpc.model.response.InstallSnapshotResponse;
import com.namnv.rpc.model.response.PreVoteResponse;
import com.namnv.rpc.model.response.ReadIndexResponse;
import com.namnv.rpc.model.response.RequestVoteResponse;
import com.namnv.rpc.model.response.TimeoutNowResponse;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;


@Setter
@Getter
public class InMemoryRpcClient implements RpcProcessor {
    private final Map<String, RaftServerService> registry = new ConcurrentHashMap<>();
    private volatile Map<String, Set<String>> reachable = new ConcurrentHashMap<>(); // nodeId -> set of nodeIds reachable


    public void register(String nodeId, RaftServerService handler) {
        registry.put(nodeId, handler);
    }

    public void setPartition(String nodeId, Set<String> canReach) {
        reachable.put(nodeId, canReach);
    }

    public void unregister(String nodeId) {
        registry.remove(nodeId);
    }

    // partition hoặc node chưa đăng ký thì RPC thất bại như lỗi mạng thật, không trả response giả
    private <T> CompletableFuture<T> call(String from, String target, Function<RaftServerService, T> invoke) {
        RaftServerService h = registry.get(target);
        if (h == null || !reachable.getOrDefault(from, Set.of()).contains(target)) {
            return CompletableFuture.failedFuture(new IOException(from + " cannot reach " + target));
        }
        return CompletableFuture.supplyAsync(() -> invoke.apply(h));
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(String target, RequestVoteRequest request) {
        return call(request.candidateId, target, h -> h.handleRequestVoteRequest(request));
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(String target, AppendEntriesRequest req) {
        return call(req.leaderId, target, h -> h.handleAppendEntriesRequest(req));
    }

    @Override
    public CompletableFuture<PreVoteResponse> preVote(String target, PreVoteRequest req) {
        return call(req.candidateId, target, h -> h.handlePreVoteRequest(req));
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(String target, InstallSnapshotRequest req) {
        return call(req.getLeaderId(), target, h -> h.handleInstallSnapshotRequest(req));
    }

    @Override
    public CompletableFuture<TimeoutNowResponse> timeoutNow(String target, TimeoutNowRequest req) {
        return call(req.leaderId, target, h -> h.handleTimeoutNowRequest(req));
    }

    @Override
    public CompletableFuture<ReadIndexResponse> readIndex(String target, ReadIndexRequest req) {
        // handler trả về future; nối nó vào kết quả của lời gọi
        return call(req.requesterId, target, h -> h.handleReadIndexRequest(req)).thenCompose(response -> response);
    }

}

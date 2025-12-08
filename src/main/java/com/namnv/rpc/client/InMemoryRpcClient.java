package com.namnv.rpc.client;


import com.namnv.rpc.RaftServerService;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.InstallSnapshotRequest;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.RequestVoteRequest;
import com.namnv.rpc.model.response.AppendEntriesResponse;
import com.namnv.rpc.model.response.InstallSnapshotResponse;
import com.namnv.rpc.model.response.PreVoteResponse;
import com.namnv.rpc.model.response.RequestVoteResponse;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;


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

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(String target, RequestVoteRequest request) {
        if (!reachable.getOrDefault(request.candidateId, Set.of()).contains(target)) {
            return CompletableFuture.completedFuture(new RequestVoteResponse(request.term, false));
        }
        RaftServerService h = registry.get(target);
        if (h == null) return CompletableFuture.completedFuture(new RequestVoteResponse(request.term, false));
        return CompletableFuture.supplyAsync(() -> h.handleRequestVoteRequest(request));
    }


    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(String target, AppendEntriesRequest req) {
        if (!reachable.getOrDefault(req.leaderId, Set.of()).contains(target)) {
            return CompletableFuture.completedFuture(null);
        }
        RaftServerService h = registry.get(target);
        if (h == null) return CompletableFuture.completedFuture(new AppendEntriesResponse(req.term, false, 0));
        return CompletableFuture.supplyAsync(() -> h.handleAppendEntriesRequest(req));
    }

    @Override
    public CompletableFuture<PreVoteResponse> preVote(String target, PreVoteRequest req) {
        if (!reachable.getOrDefault(req.candidateId, Set.of()).contains(target)) {
            return CompletableFuture.completedFuture(new PreVoteResponse(req.term, false));
        }

        RaftServerService h = registry.get(target);
        if (h == null) {
            return CompletableFuture.completedFuture(new PreVoteResponse(req.term, false));
        }

        return CompletableFuture.supplyAsync(() -> h.handlePreVoteRequest(req));
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(String target, InstallSnapshotRequest req) {
        RaftServerService h = registry.get(target);
        if (h == null) {
            return CompletableFuture.completedFuture(new InstallSnapshotResponse());
        }
        return CompletableFuture.supplyAsync(() -> h.handleInstallSnapshotRequest(req));
    }

}

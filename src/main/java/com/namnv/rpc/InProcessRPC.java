package com.namnv.rpc;


import com.namnv.rpc.model.*;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;


@Setter
@Getter
public class InProcessRPC implements RaftRPC {
    private final Map<String, RaftNodeRPCHandler> registry = new ConcurrentHashMap<>();
    private volatile Map<String, Set<String>> reachable = new ConcurrentHashMap<>(); // nodeId -> set of nodeIds reachable


    public void register(String nodeId, RaftNodeRPCHandler handler) {
        registry.put(nodeId, handler);
    }

    public void setPartition(String nodeId, Set<String> canReach) {
        reachable.put(nodeId, canReach);
    }

    public void unregister(String nodeId) {
        registry.remove(nodeId);
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(String target, RequestVoteRequest req) {
        if (!reachable.getOrDefault(req.candidateId, Set.of()).contains(target)) {
            return CompletableFuture.completedFuture(new RequestVoteResponse(req.term, false));
        }
        RaftNodeRPCHandler h = registry.get(target);
        if (h == null) return CompletableFuture.completedFuture(new RequestVoteResponse(req.term, false));
        return CompletableFuture.supplyAsync(() -> h.handleRequestVoteRequest(req));
    }


    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(String target, AppendEntriesRequest req) {
        if (!reachable.getOrDefault(req.leaderId, Set.of()).contains(target)) {
            return CompletableFuture.completedFuture(null);
        }
        RaftNodeRPCHandler h = registry.get(target);
        if (h == null) return CompletableFuture.completedFuture(new AppendEntriesResponse(req.term, false, 0));
        return CompletableFuture.supplyAsync(() -> h.handleAppendEntriesRequest(req));
    }

    @Override
    public CompletableFuture<PreVoteResponse> preVote(String target, PreVoteRequest req) {
//        // Kiểm tra node target có reachable không
        if (!reachable.getOrDefault(req.candidateId, Set.of()).contains(target)) {
            return CompletableFuture.completedFuture(new PreVoteResponse(req.term, false));
        }

        RaftNodeRPCHandler h = registry.get(target);
        if (h == null) {
            return CompletableFuture.completedFuture(new PreVoteResponse(req.term, false));
        }

        return CompletableFuture.supplyAsync(() -> h.handlePreVoteRequest(req));
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(String target, InstallSnapshotRequest req) {
        RaftNodeRPCHandler h = registry.get(target);
        if (h == null) {
            return CompletableFuture.completedFuture(new InstallSnapshotResponse());
        }
        return CompletableFuture.supplyAsync(() -> h.handleInstallSnapshotRequest(req));
    }

}

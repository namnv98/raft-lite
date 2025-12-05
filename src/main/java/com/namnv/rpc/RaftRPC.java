package com.namnv.rpc;

import com.namnv.rpc.model.*;

import java.util.concurrent.CompletableFuture;


public interface RaftRPC {
    CompletableFuture<RequestVoteResponse> requestVote(String target, RequestVoteRequest req);

    CompletableFuture<AppendEntriesResponse> appendEntries(String target, AppendEntriesRequest req);

    CompletableFuture<PreVoteResponse> preVote(String target, PreVoteRequest req);

    CompletableFuture<InstallSnapshotResponse> installSnapshot(String target, InstallSnapshotRequest req);

}
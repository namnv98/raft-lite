package com.namnv.rpc.client;

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

import java.util.concurrent.CompletableFuture;

public interface RpcProcessor {
    CompletableFuture<RequestVoteResponse> requestVote(String serverId, RequestVoteRequest request);

    CompletableFuture<AppendEntriesResponse> appendEntries(String serverId, AppendEntriesRequest request);

    CompletableFuture<PreVoteResponse> preVote(String serverId, PreVoteRequest request);

    CompletableFuture<InstallSnapshotResponse> installSnapshot(String serverId, InstallSnapshotRequest request);

    CompletableFuture<TimeoutNowResponse> timeoutNow(String serverId, TimeoutNowRequest request);

    CompletableFuture<ReadIndexResponse> readIndex(String serverId, ReadIndexRequest request);
}
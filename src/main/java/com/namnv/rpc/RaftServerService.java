package com.namnv.rpc;

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

public interface RaftServerService {
    RequestVoteResponse handleRequestVoteRequest(RequestVoteRequest req);

    AppendEntriesResponse handleAppendEntriesRequest(AppendEntriesRequest req);

    PreVoteResponse handlePreVoteRequest(PreVoteRequest req);

    InstallSnapshotResponse handleInstallSnapshotRequest(InstallSnapshotRequest req);

    TimeoutNowResponse handleTimeoutNowRequest(TimeoutNowRequest req);

    // khác các RPC còn lại, câu trả lời chỉ có sau một vòng heartbeat của leader nên được trả về bất đồng bộ
    CompletableFuture<ReadIndexResponse> handleReadIndexRequest(ReadIndexRequest req);
}

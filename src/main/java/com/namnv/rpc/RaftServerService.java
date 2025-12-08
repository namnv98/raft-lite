package com.namnv.rpc;

import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.InstallSnapshotRequest;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.RequestVoteRequest;
import com.namnv.rpc.model.response.AppendEntriesResponse;
import com.namnv.rpc.model.response.InstallSnapshotResponse;
import com.namnv.rpc.model.response.PreVoteResponse;
import com.namnv.rpc.model.response.RequestVoteResponse;

public interface RaftServerService {
    RequestVoteResponse handleRequestVoteRequest(RequestVoteRequest req);

    AppendEntriesResponse handleAppendEntriesRequest(AppendEntriesRequest req);

    PreVoteResponse handlePreVoteRequest(PreVoteRequest req);

    InstallSnapshotResponse handleInstallSnapshotRequest(InstallSnapshotRequest req);
}

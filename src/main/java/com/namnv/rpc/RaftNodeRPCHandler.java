package com.namnv.rpc;

import com.namnv.rpc.model.*;

public interface RaftNodeRPCHandler {
    RequestVoteResponse handleRequestVoteRequest(RequestVoteRequest req);

    AppendEntriesResponse handleAppendEntriesRequest(AppendEntriesRequest req);

    PreVoteResponse handlePreVoteRequest(PreVoteRequest req);

    InstallSnapshotResponse handleInstallSnapshotRequest(InstallSnapshotRequest req);

}

package com.namnv.raft;

/**
 * Ảnh chụp trạng thái và các bộ đếm của một node, lấy bằng {@link RaftNode#metrics()}.
 * Các bộ đếm tính từ lúc node khởi động.
 */
public record RaftMetrics(
        String nodeId,
        NodeState state,
        long term,
        String leaderId,
        long commitIndex,
        long lastApplied,
        long firstLogIndex,
        long lastLogIndex,
        int pendingCommands,
        int pendingReads,
        long electionsStarted,
        long timesElectedLeader,
        long commandsAccepted,
        long commandsRejected,
        long duplicateCommands,
        long readsServed,
        long snapshotsCreated,
        long snapshotsInstalled
) {
}

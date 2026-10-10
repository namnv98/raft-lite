package com.namnv.raft;

import com.namnv.entity.LogEntry;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.response.AppendEntriesResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Phía follower của replication: kiểm tra và ghi các entry leader gửi vào log, gom các AppendEntries đến trong lúc đĩa
 * bận vào một lần fsync chung, và chỉ trả lời khi entry đã bền vững.
 */
@Slf4j
final class FollowerLog {
    private final RaftNode node;

    // các AppendEntries đã ghi vào log, chờ một lần fsync chung rồi mới trả lời
    private List<SyncWaiter> syncWaiters = new ArrayList<>();
    // lô đang được fsync ở thread IO (null nếu không có); shutdown() trả lời nó nếu thread IO không còn chạy tới
    private List<SyncWaiter> syncingBatch;

    FollowerLog(RaftNode node) {
        this.node = node;
    }

    private record SyncWaiter(AppendEntriesResponse response, CompletableFuture<AppendEntriesResponse> future) {
    }

    AppendEntriesResponse handleAppendEntriesRequest(AppendEntriesRequest req) {
        var response = appendToLog(req);
        if (!response.success) {
            return response;
        }
        // fsync ngoài lock: node vẫn xử lý RPC khác trong lúc chờ đĩa, và chỉ ack khi entry đã bền vững
        Exception failure = null;
        try {
            syncLogAndVote();
        } catch (RuntimeException e) {
            failure = e;
        }
        node.lock.lock();
        try {
            return ackAfterSync(response, failure);
        } finally {
            node.unlock();
        }
    }

    // chạy trên thread của node: ghi vào log rồi xếp hàng chờ lần fsync chung
    void appendAsync(AppendEntriesRequest req, CompletableFuture<AppendEntriesResponse> future) {
        AppendEntriesResponse response;
        try {
            response = appendToLog(req);
        } catch (RuntimeException notRunning) {
            node.fail(future, notRunning);
            return;
        }
        if (!response.success) {
            node.complete(future, response);
            return;
        }
        syncWaiters.add(new SyncWaiter(response, future));
        requestFollowerSync();
    }

    // mỗi lúc nhiều nhất một lần fsync của follower; nó phủ mọi entry đã ghi trước khi nó được xếp hàng
    private void requestFollowerSync() {
        if (syncingBatch != null || syncWaiters.isEmpty()) {
            return;
        }
        var batch = syncWaiters;
        syncWaiters = new ArrayList<>();
        if (!node.stopped && !node.nodeOptions.isLogSync() && !node.persistent.hasUnsyncedVote()) {
            // không fsync log và phiếu không đổi: sync() chỉ ghi xuống file, không đáng một lần chuyển sang thread IO
            RuntimeException failure = null;
            try {
                node.persistent.getLogStore().sync();
            } catch (RuntimeException e) {
                failure = e;
            }
            for (SyncWaiter waiter : batch) {
                node.complete(waiter.future(), ackAfterSync(waiter.response(), failure));
            }
            return;
        }
        if (node.stopped) {
            // shutdown() đã flush log trước khi đóng nó, nên mọi entry đã ghi đều đã bền vững
            for (SyncWaiter waiter : batch) {
                node.complete(waiter.future(), ackAfterSync(waiter.response(), null));
            }
            return;
        }
        syncingBatch = batch;
        node.runtime.executeIo(() -> {
            Exception failure = null;
            try {
                syncLogAndVote();
            } catch (Exception e) {
                failure = e;
            }
            var syncFailure = failure;
            node.onNode(() -> {
                if (syncingBatch == batch) {
                    syncingBatch = null;
                }
                for (SyncWaiter waiter : batch) {
                    node.complete(waiter.future(), ackAfterSync(waiter.response(), syncFailure));
                }
                requestFollowerSync();
            });
        });
    }

    /**
     * Gọi từ shutdown() sau khi đã flush log: AppendEntries đã ghi vào log mà chưa được trả lời thì thread IO có thể không
     * còn chạy tới chúng, và leader sẽ không gửi gì thêm cho node này chừng nào request đó còn chưa có câu trả lời.
     */
    void answerUnsynced(Exception flushFailure) {
        var unanswered = new ArrayList<SyncWaiter>(syncWaiters);
        if (syncingBatch != null) {
            unanswered.addAll(syncingBatch);
        }
        syncWaiters.clear();
        syncingBatch = null;
        for (SyncWaiter waiter : unanswered) {
            node.complete(waiter.future(), ackAfterSync(waiter.response(), flushFailure));
        }
    }

    private void syncLogAndVote() {
        node.persistent.getLogStore().sync();
        node.persistent.syncVote();
    }

    // câu trả lời cho một AppendEntries đã ghi vào log, sau khi fsync xong (failure != null nếu fsync lỗi)
    private AppendEntriesResponse ackAfterSync(AppendEntriesResponse response, Exception failure) {
        if (failure != null) {
            log.error("Node {} failed to sync log", node.nodeId, failure);
            return new AppendEntriesResponse(response.term, false, 0);
        }
        // trong lúc chờ đĩa node có thể đã sang term khác và log bị leader mới ghi đè.
        // Node vừa tự tắt vì bị gỡ vẫn ack lần cuối để leader biết nó đã nhận cấu hình và thôi gửi;
        // ack đó không được tính vào quorum nào vì node không còn trong cấu hình.
        var removedAndStopped = node.stopped && node.membership.removalHandled && !node.membership.conf.contains(node.nodeId);
        if ((node.stopped && !removedAndStopped) || node.persistent.getCurrentTerm() != response.term) {
            return new AppendEntriesResponse(node.persistent.getCurrentTerm(), false, 0);
        }
        return response;
    }

    // kiểm tra và append vào log nhưng chưa fsync
    private AppendEntriesResponse appendToLog(AppendEntriesRequest req) {
        node.lock.lock();
        try {
            node.ensureRunning();
            if (req.term < node.persistent.getCurrentTerm()) {
                return new AppendEntriesResponse(node.persistent.getCurrentTerm(), false, 0);
            }

            node.election.handleHeartbeat(req.term, req.leaderId);

            var term = node.persistent.getCurrentTerm();
            var logStore = node.persistent.getLogStore();
            try {
                // matchIndex khi thất bại là gợi ý để leader lùi nextIndex nhanh
                if (req.prevLogIndex > logStore.lastIndex()) {
                    return new AppendEntriesResponse(term, false, logStore.lastIndex());
                }
                // prevLogIndex < baseIndex: phần đó nằm trong snapshot, đã commit nên chắc chắn khớp
                if (req.prevLogIndex >= logStore.getBaseIndex() && node.termAt(req.prevLogIndex) != req.prevLogTerm) {
                    return new AppendEntriesResponse(term, false, req.prevLogIndex - 1);
                }

                List<LogEntry> entries = req.entries();
                storeEntries(entries);

                var lastNewIndex = req.prevLogIndex + entries.size();
                var newCommit = Math.min(req.leaderCommit, lastNewIndex);
                if (newCommit > node.commitIndex) {
                    node.commitIndex = newCommit;
                    node.applier.applyCommitted();
                }
                return new AppendEntriesResponse(term, true, lastNewIndex);
            } catch (Exception e) {
                log.error("Node {} failed to append entries", node.nodeId, e);
                return new AppendEntriesResponse(term, false, node.commitIndex);
            }
        } finally {
            node.unlock();
        }
    }

    // ghi các entry leader gửi vào log: bỏ qua entry đã có, chỉ truncate khi thực sự conflict term
    private void storeEntries(List<LogEntry> entries) {
        var logStore = node.persistent.getLogStore();
        // trường hợp thường gặp: mọi entry nối tiếp ngay sau log, không có gì để bỏ qua hay truncate
        if (!entries.isEmpty() && entries.get(0).getIndex() == logStore.lastIndex() + 1
                && entries.get(0).getIndex() > logStore.getBaseIndex()) {
            boolean confChanged = false;
            for (LogEntry e : entries) {
                confChanged |= e.isConfigurationEntry();
            }
            logStore.appendEntries(entries);
            if (confChanged) {
                node.membership.refreshConf();
            }
            return;
        }
        var toAppend = new ArrayList<LogEntry>(entries.size());
        var confChanged = false;
        for (LogEntry e : entries) {
            if (e.getIndex() <= logStore.getBaseIndex()) {
                continue;
            }
            if (toAppend.isEmpty()) {
                var existing = logStore.get(e.getIndex());
                if (existing != null) {
                    if (existing.getTerm() == e.getTerm()) {
                        // đã có sẵn (RPC cũ hoặc gửi trùng), không được truncate
                        continue;
                    }
                    if (e.getIndex() <= node.commitIndex) {
                        throw new IllegalStateException("Conflict at committed index " + e.getIndex());
                    }
                    confChanged |= node.membership.confIndex >= e.getIndex();
                    logStore.truncateSuffix(e.getIndex());
                }
            }
            toAppend.add(e);
            confChanged |= e.isConfigurationEntry();
        }
        logStore.appendEntries(toAppend);
        if (confChanged) {
            node.membership.refreshConf();
        }
    }
}

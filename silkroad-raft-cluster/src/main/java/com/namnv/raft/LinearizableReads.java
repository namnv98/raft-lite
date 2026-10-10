package com.namnv.raft;

import com.namnv.raft.state.LeaderState;
import com.namnv.rpc.model.request.ReadIndexRequest;
import com.namnv.rpc.model.response.ReadIndexResponse;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * Đọc nhất quán theo ReadIndex (xem {@link RaftNode#read}): leader xác nhận lại quyền với đa số, follower hỏi leader
 * readIndex cho cả nhóm lần đọc bằng một request, rồi lần đọc chờ state machine của node apply tới readIndex.
 */
final class LinearizableReads {
    private final RaftNode node;

    // leader: các yêu cầu đọc đang chờ đa số xác nhận lại quyền
    private final ArrayDeque<PendingRead> pendingReads = new ArrayDeque<>();
    // các yêu cầu đọc đã có readIndex, đang chờ state machine của node này apply tới đó
    private final List<AppliedWaiter> appliedWaiters = new ArrayList<>();
    // follower: các lần đọc chưa được hỏi readIndex, và nhóm đang chờ leader trả lời (null nếu không có request nào đang bay)
    private List<QueuedRead> queuedReads = new ArrayList<>();
    private ReadIndexBatch readIndexBatch;

    // bộ đếm cho metrics()
    long readsServed;

    LinearizableReads(RaftNode node) {
        this.node = node;
    }

    // một lần đọc trên follower đang chờ được hỏi readIndex
    private record QueuedRead(Runnable run, Consumer<Throwable> onFailure) {
    }

    // các lần đọc đi chung một ReadIndex đang chờ leader trả lời
    private record ReadIndexBatch(List<QueuedRead> reads, long deadlineNanos) {
        void fail(Throwable cause) {
            for (QueuedRead read : reads) {
                read.onFailure().accept(cause);
            }
        }
    }

    // một yêu cầu đọc đang chờ leader xác nhận quyền: được đa số trả lời một request gửi sau thời điểm stamp
    private record PendingRead(long stamp, long readIndex, long deadlineNanos, LongConsumer onConfirmed,
                               Consumer<Throwable> onFailure) {
    }

    // việc cần làm khi state machine của node này apply tới readIndex
    private record AppliedWaiter(long readIndex, long deadlineNanos, Runnable run, Consumer<Throwable> onFailure) {
    }

    int pendingCount() {
        return pendingReads.size();
    }

    // chạy trên thread của node
    <T> void read(Supplier<T> query, CompletableFuture<T> future) {
        // query chạy trong lock, kết quả của nó được báo sau khi nhả lock
        Runnable runQuery = () -> {
            try {
                readsServed++;
                node.complete(future, query.get());
            } catch (Throwable t) {
                node.fail(future, t);
            }
        };
        Consumer<Throwable> onFailure = cause -> node.fail(future, cause);
        var leaderId = node.leaderId;
        if (node.stopped || node.state == null) {
            onFailure.accept(new NotLeaderException(node.nodeId, leaderId));
        } else if (node.state == NodeState.LEADER) {
            confirmLeadership(readIndex -> awaitApplied(readIndex, runQuery, onFailure), onFailure);
        } else if (leaderId == null || leaderId.equals(node.nodeId)) {
            onFailure.accept(new NotLeaderException(node.nodeId, null));
        } else {
            queuedReads.add(new QueuedRead(runQuery, onFailure));
            if (readIndexBatch == null) {
                sendReadIndex();
            }
        }
    }

    /**
     * Follower hỏi leader readIndex cho mọi lần đọc đang chờ bằng một request duy nhất. Lần đọc đến trong lúc request
     * đang bay phải chờ request kế tiếp: readIndex chỉ đúng cho những lần đọc bắt đầu trước khi request được gửi.
     * Nhờ gom lại, số request tới leader không tăng theo số lần đọc đồng thời.
     */
    private void sendReadIndex() {
        if (queuedReads.isEmpty()) {
            return;
        }
        var batch = new ReadIndexBatch(queuedReads, deadline());
        queuedReads = new ArrayList<>();
        var nodeId = node.nodeId;
        var leaderId = node.leaderId;
        if (node.stopped) {
            batch.fail(new NotLeaderException(nodeId, null));
        } else if (node.state == NodeState.LEADER) {
            // vừa được bầu trong lúc các lần đọc này còn chờ
            confirmLeadership(readIndex -> awaitApplied(readIndex, batch), batch::fail);
        } else if (leaderId == null || leaderId.equals(nodeId)) {
            batch.fail(new NotLeaderException(nodeId, null));
        } else {
            readIndexBatch = batch;
            node.rpcProcessor.readIndex(leaderId, new ReadIndexRequest(nodeId)).whenComplete((response, error) -> {
                node.onNode(() -> {
                    if (readIndexBatch != batch) {
                        return; // đã quá hạn và được báo lỗi
                    }
                    readIndexBatch = null;
                    if (node.stopped || response == null || !response.success) {
                        var hint = response != null && !nodeId.equals(response.leaderId) ? response.leaderId : null;
                        batch.fail(new NotLeaderException(nodeId, hint));
                    } else {
                        awaitApplied(response.readIndex, batch);
                    }
                    sendReadIndex();
                });
            });
        }
    }

    private void awaitApplied(long readIndex, ReadIndexBatch batch) {
        for (QueuedRead read : batch.reads()) {
            awaitApplied(readIndex, read.run(), read.onFailure());
        }
    }

    // chạy trên thread của node: leader trả lời readIndex cho follower sau khi xác nhận lại quyền
    void handleReadIndexRequest(CompletableFuture<ReadIndexResponse> future) {
        if (node.state == null || node.stopped) {
            node.fail(future, new IllegalStateException("Node " + node.nodeId + " is not running"));
        } else if (node.state != NodeState.LEADER) {
            node.complete(future, new ReadIndexResponse(false, 0, node.leaderId));
        } else {
            confirmLeadership(readIndex -> node.complete(future, new ReadIndexResponse(true, readIndex, node.nodeId)),
                    error -> node.complete(future, new ReadIndexResponse(false, 0, null)));
        }
    }

    // leader: ghi nhận readIndex và bắt đầu một vòng heartbeat mới để chứng minh mình vẫn là leader sau thời điểm này
    private void confirmLeadership(LongConsumer onConfirmed, Consumer<Throwable> onFailure) {
        var leaderState = node.leaderState;
        // leader mới chưa biết commit index của mình có đủ mới không cho tới khi no-op của nó commit,
        // nên readIndex không bao giờ nhỏ hơn index của no-op đó
        var readIndex = Math.max(node.commitIndex, leaderState.getTermStartIndex());
        pendingReads.addLast(new PendingRead(leaderState.nextStamp(), readIndex, deadline(), onConfirmed, onFailure));
        // chỉ response của request gửi sau thời điểm này mới xác nhận được quyền leader
        for (String peer : node.membership.peers()) {
            node.replicator.replicateTo(peer, true);
        }
        completeReads(); // cluster một node không cần chờ ai
    }

    // báo cho các yêu cầu đọc đã được đa số xác nhận
    void completeReads() {
        if (node.state != NodeState.LEADER || pendingReads.isEmpty()) {
            return;
        }
        var ackedStamp = node.leaderState.getAckedStamp();
        var confirmed = node.membership.conf.quorumIndex(
                id -> id.equals(node.nodeId) ? Long.MAX_VALUE : ackedStamp.getOrDefault(id, 0L));
        // stamp tăng dần theo thứ tự đến, nên chỉ cần lấy dần từ đầu hàng
        while (!pendingReads.isEmpty() && pendingReads.peekFirst().stamp() <= confirmed) {
            var read = pendingReads.pollFirst();
            read.onConfirmed().accept(read.readIndex());
        }
    }

    // chạy run ngay nếu state machine đã apply tới readIndex, nếu không thì chờ
    private void awaitApplied(long readIndex, Runnable run, Consumer<Throwable> onFailure) {
        if (node.lastApplied >= readIndex && !node.snapshots.loadingSnapshot) {
            run.run();
            return;
        }
        appliedWaiters.add(new AppliedWaiter(readIndex, deadline(), run, onFailure));
    }

    void runAppliedWaiters() {
        if (appliedWaiters.isEmpty()) {
            return;
        }
        var applied = node.lastApplied;
        var ready = new ArrayList<AppliedWaiter>();
        appliedWaiters.removeIf(waiter -> waiter.readIndex() <= applied && ready.add(waiter));
        for (AppliedWaiter waiter : ready) {
            waiter.run().run();
        }
    }

    // còn yêu cầu đọc nào mà peer này chưa xác nhận không
    boolean awaitsReadConfirmation(LeaderState ls, String peer) {
        // yêu cầu đến sau cùng có stamp lớn nhất
        return !pendingReads.isEmpty() && pendingReads.peekLast().stamp() > ls.getAckedStamp().getOrDefault(peer, 0L);
    }

    // báo lỗi cho các lần đọc đã chờ quá clientTimeoutMs
    void expire(long now) {
        while (!pendingReads.isEmpty() && pendingReads.peekFirst().deadlineNanos() - now <= 0) {
            pendingReads.pollFirst().onFailure().accept(new TimeoutException("Read was not confirmed by a quorum in time"));
        }
        if (readIndexBatch != null && readIndexBatch.deadlineNanos() - now <= 0) {
            // leader không trả lời: không để các lần đọc đến sau chờ mãi sau request này
            var expired = readIndexBatch;
            readIndexBatch = null;
            expired.fail(new TimeoutException("Leader did not answer ReadIndex in time"));
            sendReadIndex();
        }
        var waiters = appliedWaiters.iterator();
        while (waiters.hasNext()) {
            var waiter = waiters.next();
            if (waiter.deadlineNanos() - now <= 0) {
                waiters.remove();
                waiter.onFailure().accept(new TimeoutException(
                        "State machine did not reach index " + waiter.readIndex() + " in time"));
            }
        }
    }

    // node mất quyền leader: các lần đọc chưa được xác nhận không còn được xác nhận nữa
    void failPendingConfirmations() {
        var notLeader = new NotLeaderException(node.nodeId, node.leaderId);
        for (PendingRead read : pendingReads) {
            read.onFailure().accept(notLeader);
        }
        pendingReads.clear();
    }

    // node dừng: báo lỗi cho mọi lần đọc còn chờ
    void failAll() {
        failPendingConfirmations();
        for (AppliedWaiter waiter : appliedWaiters) {
            waiter.onFailure().accept(new NotLeaderException(node.nodeId, null));
        }
        appliedWaiters.clear();
        sendReadIndex(); // node đã dừng: báo lỗi cho các lần đọc còn xếp hàng
        if (readIndexBatch != null) {
            readIndexBatch.fail(new NotLeaderException(node.nodeId, null));
            readIndexBatch = null;
        }
    }

    private long deadline() {
        return node.runtime.nanoTime() + TimeUnit.MILLISECONDS.toNanos(node.nodeOptions.getClientTimeoutMs());
    }
}

package com.namnv.raft;

import com.namnv.entity.LogEntry;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Lệnh của client trên leader: nhận lệnh, ghi vào log, giữ future của nó tới khi entry được apply, hết hạn hoặc bị leader
 * mới ghi đè, và báo kết quả theo đúng thứ tự commit.
 */
@Slf4j
final class ClientCommands {
    // kết quả trả về cho lệnh của state machine không có kết quả, hoặc lệnh trùng không được apply lại
    static final byte[] NO_RESULT = new byte[0];

    private static final UnknownOutcomeException NOT_LEADER = new UnknownOutcomeException("not the leader");
    private static final UnknownOutcomeException TIMED_OUT = new UnknownOutcomeException("not committed in time");
    private static final UnknownOutcomeException OVERLOADED = new UnknownOutcomeException("too many pending commands");
    private static final UnknownOutcomeException LOST = new UnknownOutcomeException("leadership changed before commit");
    private static final UnknownOutcomeException APPEND_FAILED = new UnknownOutcomeException("failed to append to the log");

    private final RaftNode node;

    // lệnh đang chờ commit, theo thứ tự index (cũng là thứ tự hết hạn)
    private final ArrayDeque<PendingCommand> pending = new ArrayDeque<>();

    // bộ đếm cho metrics()
    long commandsAccepted;
    long commandsRejected;
    long duplicateCommands;

    ClientCommands(RaftNode node) {
        this.node = node;
    }

    // lệnh đang chờ commit: đúng một trong hai future được dùng, tuỳ người gọi cần biết "đã apply" hay cần kết quả
    private record PendingCommand(long index, CompletableFuture<Boolean> applied, CompletableFuture<byte[]> result,
                                  long deadlineNanos) {
    }

    int pendingCount() {
        return pending.size();
    }

    // chạy trên thread của node
    void accept(String clientId, long sequence, byte[] command, boolean batch,
                CompletableFuture<Boolean> applied, CompletableFuture<byte[]> result) {
        if (node.stopped || node.state != NodeState.LEADER) {
            finish(applied, result, null, NOT_LEADER);
        } else if (node.applier.isDuplicate(clientId, sequence)) {
            duplicateCommands++;
            finish(applied, result, NO_RESULT, null); // lần gửi trước đã được apply; kết quả của nó không còn
        } else if (pending.size() >= node.nodeOptions.getMaxPendingCommands()) {
            // follower không theo kịp: từ chối sớm thay vì để hàng chờ lớn mãi
            commandsRejected++;
            finish(applied, result, null, OVERLOADED);
        } else {
            long nextIndex = node.persistent.getLogStore().lastIndex() + 1;
            var entry = new LogEntry(nextIndex, node.persistent.getCurrentTerm(), command, clientId, sequence);
            entry.setBatch(batch);
            appendAndTrack(entry, applied, result);
        }
    }

    // gọi khi đang giữ lock
    CompletableFuture<Boolean> closeSession(String clientId) {
        if (node.stopped || node.state != NodeState.LEADER) {
            return CompletableFuture.completedFuture(false);
        }
        var index = node.persistent.getLogStore().lastIndex() + 1;
        var future = new CompletableFuture<Boolean>();
        appendAndTrack(LogEntry.newSessionClose(index, node.persistent.getCurrentTerm(), clientId), future, null);
        return future;
    }

    // leader ghi entry vào log; người gọi được báo khi entry được apply hoặc khi không rõ kết quả
    private void appendAndTrack(LogEntry entry, CompletableFuture<Boolean> applied, CompletableFuture<byte[]> result) {
        node.stamped(entry);
        try {
            node.persistent.getLogStore().appendEntry(entry);
        } catch (Exception error) {
            log.error("Leader {} failed to append client command", node.nodeId, error);
            commandsRejected++;
            finish(applied, result, null, APPEND_FAILED);
            return;
        }
        commandsAccepted++;
        pending.addLast(new PendingCommand(entry.getIndex(), applied, result,
                node.runtime.nanoTime() + TimeUnit.MILLISECONDS.toNanos(node.nodeOptions.getClientTimeoutMs())));
        node.broadcastSoon();
    }

    // entry tại index vừa được apply với kết quả result; lệnh đứng trước nó mà còn chờ thì đã bị leader mới ghi đè
    void onApplied(long index, byte[] result) {
        // hàng chờ xếp theo index, và các entry được apply theo đúng thứ tự đó
        while (!pending.isEmpty() && pending.peekFirst().index() <= index) {
            var command = pending.pollFirst();
            if (command.index() == index) {
                finish(command.applied(), command.result(), result == null ? NO_RESULT : result, null);
            } else {
                finish(command.applied(), command.result(), null, LOST);
            }
        }
    }

    // báo "không rõ kết quả" cho các lệnh đã chờ quá clientTimeoutMs
    void expire(long now) {
        // hàng xếp theo thứ tự đến, nên phần tử đầu luôn hết hạn sớm nhất
        while (!pending.isEmpty() && pending.peekFirst().deadlineNanos() - now <= 0) {
            var expired = pending.pollFirst(); // timeout → không rõ kết quả
            finish(expired.applied(), expired.result(), null, TIMED_OUT);
        }
    }

    // node mất quyền leader hoặc dừng: không còn biết các lệnh đang chờ có được commit hay không
    void failPending() {
        for (PendingCommand command : pending) {
            finish(command.applied(), command.result(), null, LOST);
        }
        pending.clear();
    }

    // báo kết quả cho người gọi (sau khi nhả lock): đã apply kèm kết quả, hoặc không rõ
    private void finish(CompletableFuture<Boolean> applied, CompletableFuture<byte[]> result, byte[] value,
                        UnknownOutcomeException unknown) {
        if (applied != null) {
            node.complete(applied, unknown == null);
        } else if (unknown == null) {
            node.complete(result, value);
        } else {
            node.fail(result, unknown);
        }
    }
}

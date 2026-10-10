package com.namnv.raft;

import com.namnv.entity.ClientSession;
import com.namnv.entity.CommandBatch;
import com.namnv.entity.LogEntry;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Đưa các entry đã commit vào state machine theo đúng thứ tự index, chống apply trùng theo (clientId, sequence), lưu
 * commit index, và kích hoạt những việc chờ commit (thay đổi cấu hình, đọc nhất quán, snapshot).
 */
@Slf4j
final class Applier {
    private final RaftNode node;

    // clientId -> sequence lớn nhất đã apply; là một phần của state được replicate nên đi kèm snapshot
    final Map<String, ClientSession> sessions = new HashMap<>();
    // state machine đã ném lỗi khi apply: không apply gì nữa, node đang tự tắt
    boolean failed;
    private boolean commitIndexFlushScheduled;

    Applier(RaftNode node) {
        this.node = node;
    }

    void applyCommitted() {
        if (node.snapshots.loadingSnapshot || failed) {
            return; // sẽ apply sau khi snapshot load xong; hoặc state machine đã hỏng và node đang tắt
        }
        var commitIndex = node.commitIndex;
        var logStore = node.persistent.getLogStore();
        for (long idx = node.lastApplied + 1; idx <= commitIndex; idx++) {
            LogEntry logEntry = logStore.get(idx);
            byte[] result = null;
            // no-op và config entry không thuộc về state machine
            if (logEntry != null && logEntry.isSessionClose()) {
                sessions.remove(logEntry.getClientId());
            } else if (logEntry != null && !logEntry.isConfigurationEntry() && logEntry.getCommand() != null) {
                try {
                    result = applyCommand(logEntry);
                } catch (Throwable error) {
                    stateMachineFailed(idx, error);
                    return;
                }
            }
            node.lastApplied = idx;
            node.commands.onApplied(idx, result);
        }
        // commit index chỉ cập nhật trong bộ nhớ, ghi xuống đĩa sau theo nhịp ở thread IO
        node.persistent.setLastCommitIndex(commitIndex);
        scheduleCommitIndexFlush();
        node.membership.onConfCommitted();
        node.membership.maybeShutdownRemoved();
        node.reads.completeReads();
        node.reads.runAppliedWaiters();
        node.snapshots.maybeSnapshot();
    }

    // việc ghi commit index dùng chung thread IO với việc fsync log: ghi sau mỗi lần commit sẽ bắt lệnh kế tiếp
    // chờ thêm hai lần fsync, nên gom lại và ghi theo nhịp
    private void scheduleCommitIndexFlush() {
        var interval = node.nodeOptions.getCommitIndexFlushIntervalMs();
        if (interval <= 0) {
            node.runIo(node.persistent::flush);
            return;
        }
        if (commitIndexFlushScheduled) {
            return;
        }
        commitIndexFlushScheduled = true;
        node.runtime.schedule(() -> node.onNode(() -> {
            commitIndexFlushScheduled = false;
            node.runIo(node.persistent::flush);
        }), interval);
    }

    /**
     * State machine ném lỗi khi apply: state của nó có thể đã đổi một phần, và các node khác có thể không gặp lỗi đó
     * (ví dụ hết bộ nhớ). Chạy tiếp thì bản sao này lệch khỏi các bản khác mà không ai biết, nên node ngừng apply và tự
     * tắt (fail-stop, như etcd). Khởi động lại sẽ dựng state từ snapshot và log trên đĩa.
     */
    private void stateMachineFailed(long index, Throwable error) {
        failed = true;
        log.error("Node {} stops: the state machine failed to apply index {}. Restart rebuilds it from the snapshot "
                + "and the log", node.nodeId, index, error);
        if (node.state == NodeState.LEADER) {
            node.election.becomeFollower(node.persistent.getCurrentTerm()); // không nhận lệnh hay trả lời đọc nào nữa
        }
        node.runIo(node::shutdown);
    }

    boolean isDuplicate(String clientId, long sequence) {
        var session = clientId == null ? null : sessions.get(clientId);
        return session != null && session.applied(sequence);
    }

    // lệnh client gửi lại (cùng clientId và sequence đã apply) có thể nằm trong log hai lần nhưng chỉ được apply một lần.
    // Mọi node quyết định giống nhau vì bảng sessions chỉ phụ thuộc vào các entry đã apply trước đó.
    private byte[] applyCommand(LogEntry entry) {
        if (isDuplicate(entry.getClientId(), entry.getSequence())) {
            return null;
        }
        var stateMachine = node.stateMachine;
        byte[] result;
        if (entry.isBatch()) {
            // state machine thấy từng lệnh như một entry riêng, cùng index với lô
            var results = new ArrayList<byte[]>();
            var anyResult = new boolean[1];
            CommandBatch.forEach(entry.getCommand(), command -> {
                var single = new LogEntry(entry.getIndex(), entry.getTerm(), command, entry.getClientId(), entry.getSequence());
                single.setTimestamp(entry.getTimestamp());
                byte[] one = stateMachine.onApplyWithResult(node.nodeId, single);
                anyResult[0] |= one != null;
                results.add(one == null ? ClientCommands.NO_RESULT : one);
            });
            result = anyResult[0] ? CommandBatch.encode(results) : null;
        } else {
            result = stateMachine.onApplyWithResult(node.nodeId, entry);
        }
        if (entry.getClientId() != null) {
            sessions.computeIfAbsent(entry.getClientId(), id -> new ClientSession()).markApplied(entry.getSequence());
        }
        return result;
    }

    void restoreSessions(Map<String, ClientSession> snapshotSessions) {
        sessions.clear();
        if (snapshotSessions != null) {
            sessions.putAll(copySessions(snapshotSessions));
        }
    }

    static Map<String, ClientSession> copySessions(Map<String, ClientSession> source) {
        Map<String, ClientSession> copy = new HashMap<>();
        for (var session : source.entrySet()) {
            copy.put(session.getKey(), new ClientSession(session.getValue()));
        }
        return copy;
    }
}

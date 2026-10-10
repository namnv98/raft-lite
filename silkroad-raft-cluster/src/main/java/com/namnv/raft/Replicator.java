package com.namnv.raft;

import com.namnv.entity.LogEntry;
import com.namnv.raft.state.LeaderState;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.InstallSnapshotRequest;
import com.namnv.rpc.model.response.AppendEntriesResponse;
import com.namnv.rpc.model.response.InstallSnapshotResponse;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Phía leader của replication: heartbeat và check quorum, fsync log của leader, gửi log cho từng peer (dò rồi pipeline,
 * xem {@link LeaderState.Progress}), gửi snapshot cho peer tụt quá xa, và đẩy commit index.
 */
@Slf4j
final class Replicator {
    private final RaftNode node;

    // đã có một yêu cầu fsync log của leader đang xếp hàng
    private boolean logSyncQueued;

    Replicator(RaftNode node) {
        this.node = node;
    }

    void onHeartbeatTick() {
        node.lock.lock();
        try {
            if (node.stopped || node.state != NodeState.LEADER) {
                return;
            }
            // check quorum: leader bị cô lập phải tự step-down thay vì giữ vai trò mãi
            if (!hasContactWithQuorum()) {
                log.warn("Leader {} lost contact with quorum, step down.", node.nodeId);
                node.election.becomeFollower(node.persistent.getCurrentTerm());
                return;
            }
            node.membership.expireDepartingNodes();
            node.membership.expirePendingConf();
            var logStore = node.persistent.getLogStore();
            if (logStore.durableIndex() < logStore.lastIndex()) {
                requestLogSync(); // lần fsync trước thất bại, hoặc chưa kịp chạy
            }
            var leaderState = node.leaderState;
            var now = node.runtime.nanoTime();
            var stuck = TimeUnit.MILLISECONDS.toNanos(2L * node.nodeOptions.getElectionTimeoutMaxMs());
            for (String peer : node.membership.replicationTargets()) {
                var progress = leaderState.progress(peer);
                if (progress.inflight > 0 && now - progress.lastActivityNanos > stuck) {
                    // Request đang bay mà lâu không có câu trả lời nào: có thể nó đã mất mà transport không báo (máy của
                    // peer mất điện thì kết nối không bị reset, transport không có timeout...). Không có lối thoát này thì
                    // cửa sổ của peer đầy mãi và leader không bao giờ gửi gì cho nó nữa. Dò lại từ phần đã khớp; câu trả
                    // lời muộn của lần cũ (nếu có) bị bỏ qua nhờ generation.
                    log.warn("Leader {} got no answer from {} for {} ms with {} request(s) in flight, probing again", node.nodeId,
                            peer, TimeUnit.NANOSECONDS.toMillis(now - progress.lastActivityNanos), progress.inflight);
                    restartProbe(leaderState, peer, leaderState.getMatchIndex().getOrDefault(peer, 0L) + 1);
                    continue;
                }
                replicateTo(peer, true);
            }
        } finally {
            node.unlock();
        }
    }

    private boolean hasContactWithQuorum() {
        var now = node.runtime.nanoTime();
        var timeoutNanos = TimeUnit.MILLISECONDS.toNanos(node.nodeOptions.getElectionTimeoutMaxMs());
        var alive = node.membership.selfOnly();
        for (var ack : node.leaderState.getLastAck().entrySet()) {
            if (now - ack.getValue() < timeoutNanos) {
                alive.add(ack.getKey());
            }
        }
        return node.membership.conf.hasQuorum(alive);
    }

    // gọi sau khi leader append: gửi cho follower song song với việc fsync log của chính mình
    void broadcast() {
        for (String peer : node.membership.replicationTargets()) {
            replicateTo(peer);
        }
        requestLogSync();
    }

    /**
     * Yêu cầu thread IO fsync log của leader. Mỗi lúc chỉ có nhiều nhất một yêu cầu đang xếp hàng: một lần fsync
     * bao trọn mọi entry đã append tới lúc nó chạy, nên xếp thêm yêu cầu cho từng lệnh chỉ làm hàng đợi dài ra
     * (ở tải cao, hàng đợi đó lớn dần không giới hạn và làm GC dừng hàng trăm mili giây).
     */
    private void requestLogSync() {
        if (logSyncQueued) {
            return;
        }
        logSyncQueued = true;
        var logStore = node.persistent.getLogStore();
        if (!node.nodeOptions.isLogSync()) {
            // sync() chỉ ghi xuống file: làm ngay trên thread của node, trong cùng lô sự kiện đang xử lý. Ngoài việc bớt
            // hai lần chuyển thread, cách này còn hãm leader lại khi hệ điều hành ghi xuống đĩa không kịp (write() bị
            // chặn): đẩy việc ghi sang thread IO thì leader nhận lệnh tiếp không giới hạn rồi khựng cả giây một lần
            node.onNode(() -> {
                logSyncQueued = false;
                try {
                    logStore.sync();
                } catch (RuntimeException e) {
                    log.error("Node {} failed to write log", node.nodeId, e);
                    return; // nhịp heartbeat sau thử lại
                }
                maybeAdvanceCommitIndex();
            });
            return;
        }
        node.runIo(() -> {
            try {
                logStore.sync();
            } finally {
                node.onNode(() -> {
                    logSyncQueued = false;
                    if (!node.stopped && node.state == NodeState.LEADER) {
                        maybeAdvanceCommitIndex(); // leader chỉ tự tính mình vào quorum khi entry đã nằm trên đĩa
                        if (logStore.durableIndex() < logStore.lastIndex()) {
                            requestLogSync(); // có entry mới đến trong lúc đang fsync
                        }
                    }
                });
            }
        });
    }

    // gửi phần log mới cho peer nếu còn chỗ trong cửa sổ
    void replicateTo(String peer) {
        replicateTo(peer, false);
    }

    /**
     * @param force gửi cả khi không có entry mới (heartbeat rỗng): nhịp heartbeat, đọc nhất quán cần một request mới,
     *              hay bắt đầu dò. Không force thì chỉ gửi khi có entry peer chưa được gửi.
     */
    void replicateTo(String peer, boolean force) {
        if (node.state != NodeState.LEADER) {
            return;
        }
        var ls = node.leaderState;
        var progress = ls.progress(peer);
        int window = progress.pipelining ? node.nodeOptions.getMaxInflightAppends() : 1;
        if (progress.inflight >= window) {
            return; // khi một request được trả lời sẽ gửi tiếp phần còn thiếu
        }
        try {
            var logStore = node.persistent.getLogStore();
            var lastIndex = logStore.lastIndex();
            var nextIdx = Math.min(ls.getNextIndex().getOrDefault(peer, lastIndex + 1), lastIndex + 1);
            if (nextIdx <= logStore.getBaseIndex()) {
                // phần follower cần đã bị compact vào snapshot; snapshot đi từng mẩu một
                if (progress.inflight == 0) {
                    progress.lastActivityNanos = node.runtime.nanoTime();
                    progress.inflight++;
                    sendSnapshot(ls, peer, progress.generation);
                }
                return;
            }
            if (nextIdx > lastIndex && !force) {
                return; // không có gì mới; các request đang bay đã mang mọi entry
            }
            if (progress.inflight == 0) {
                progress.lastActivityNanos = node.runtime.nanoTime();
            }
            progress.inflight++;
            sendEntries(ls, peer, nextIdx, progress);
        } catch (Exception e) {
            log.error("Leader {} failed to replicate to {}", node.nodeId, peer, e);
            progress.inflight = Math.max(0, progress.inflight - 1);
        }
    }

    // gửi mọi entry từ nextIdx, hoặc heartbeat rỗng nếu follower đã đủ log
    private void sendEntries(LeaderState ls, String peer, long nextIdx, LeaderState.Progress progress) {
        var logStore = node.persistent.getLogStore();
        var prevIndex = nextIdx - 1;
        // phần còn lại (nếu có) được gửi tiếp ngay khi request này được trả lời
        var maxEntries = node.nodeOptions.getMaxEntriesPerRequest();
        // term của prevIndex cũng nằm trong log, trừ khi prevIndex chính là mốc của snapshot
        var prevInLog = prevIndex > logStore.getBaseIndex();
        var readStart = prevInLog ? prevIndex : nextIdx;
        var readCount = prevInLog ? maxEntries + 1 : maxEntries;
        if (logStore.isCached(readStart, readCount)) {
            // log lưu sẵn entry dưới dạng khung thì gửi nguyên vùng byte đó, không dựng lại từng entry cho từng follower
            var block = logStore.readBlock(nextIdx, maxEntries);
            if (block != null) {
                send(ls, peer, new AppendEntriesRequest(node.persistent.getCurrentTerm(), node.nodeId, prevIndex,
                        node.termAt(prevIndex), block, node.commitIndex));
            } else {
                sendEntries(ls, peer, prevIndex, node.termAt(prevIndex), logStore.readFrom(nextIdx, maxEntries));
            }
            return;
        }
        // follower tụt xa hơn phần log còn trong bộ nhớ: đọc từ đĩa ở thread khác, node không phải chờ.
        // Mỗi lúc chỉ một lần đọc như vậy cho mỗi peer; khi bắt kịp phần trong bộ nhớ thì mới pipeline
        if (progress.inflight > 1) {
            progress.inflight--;
            return;
        }
        var generation = progress.generation;
        var baseTerm = logStore.getBaseTerm();
        node.runtime.executeRead(() -> {
            List<LogEntry> read;
            try {
                read = logStore.readFrom(readStart, readCount);
            } catch (Exception e) {
                log.error("Leader {} failed to read log from index {} for {}", node.nodeId, readStart, peer, e);
                read = null;
            }
            var entries = read;
            node.onNode(() -> sendReadEntries(ls, peer, readStart, prevIndex, prevInLog, baseTerm, entries, generation));
        });
    }

    private void sendReadEntries(LeaderState ls, String peer, long readStart, long prevIndex, boolean prevInLog,
                                 long baseTerm, List<LogEntry> read, long generation) {
        var progress = ls.progress(peer);
        if (progress.generation != generation) {
            return; // peer đã quay về dò trong lúc đọc: lần đọc này không còn được tính
        }
        try {
            // Chừng nào node còn là leader của đúng nhiệm kỳ đó thì log của nó chỉ dài thêm, nên phần vừa đọc vẫn đúng.
            // Nếu snapshot vừa compact mất đoạn này thì bỏ, nhịp sau sẽ gửi snapshot.
            if (read == null || read.isEmpty() || read.get(0).getIndex() != readStart || node.stopped || node.leaderState != ls) {
                progress.inflight = Math.max(0, progress.inflight - 1);
                return;
            }
            var prevTerm = prevInLog ? read.get(0).getTerm() : baseTerm;
            sendEntries(ls, peer, prevIndex, prevTerm, prevInLog ? new ArrayList<>(read.subList(1, read.size())) : read);
        } catch (Exception e) {
            log.error("Leader {} failed to replicate to {}", node.nodeId, peer, e);
            progress.inflight = Math.max(0, progress.inflight - 1);
        }
    }

    private void sendEntries(LeaderState ls, String peer, long prevIndex, long prevTerm, List<LogEntry> entries) {
        send(ls, peer, new AppendEntriesRequest(node.persistent.getCurrentTerm(), node.nodeId, prevIndex, prevTerm, entries,
                node.commitIndex));
    }

    private void send(LeaderState ls, String peer, AppendEntriesRequest req) {
        var sentStamp = ls.nextStamp();
        var progress = ls.progress(peer);
        var generation = progress.generation;
        if (progress.pipelining) {
            // không chờ câu trả lời: request kế tiếp nối ngay sau request này
            ls.getNextIndex().put(peer, req.prevLogIndex + req.entryCount() + 1);
        }
        node.rpcProcessor.appendEntries(peer, req).whenComplete((resp, error) ->
                node.onNode(() -> onAppendEntriesResponse(ls, peer, req, resp, sentStamp, generation)));
    }

    private void onAppendEntriesResponse(LeaderState ls, String peer, AppendEntriesRequest req,
                                         AppendEntriesResponse resp, long sentStamp, long generation) {
        var progress = ls.progress(peer);
        // response của lần dò/pipeline trước vẫn dùng được để cập nhật matchIndex, nhưng không còn là request đang bay
        boolean current = progress.generation == generation;
        if (current) {
            progress.inflight = Math.max(0, progress.inflight - 1);
            progress.lastActivityNanos = node.runtime.nanoTime();
        }
        if (resp == null) {
            // request bị mất hoặc hết hạn: các request pipeline sau nó sẽ không khớp, nên dò lại từ phần đã chắc chắn
            if (current && node.leaderState == ls && !node.stopped && progress.pipelining) {
                restartProbe(ls, peer, ls.getMatchIndex().getOrDefault(peer, 0L) + 1);
            }
            return;
        }
        if (!isCurrentLeader(ls, resp.term)) {
            return;
        }
        ls.getLastAck().put(peer, node.runtime.nanoTime());
        // dù thành công hay bị từ chối vì log chưa khớp, peer trả lời ở đúng term này tức là nó vẫn coi node này là leader
        ls.getAckedStamp().merge(peer, sentStamp, Math::max);
        node.reads.completeReads();
        advanceReplication(ls, peer, req, resp, current);
        // yêu cầu đọc đến sau khi request này được gửi cần một vòng mới, không chờ tới nhịp heartbeat kế tiếp
        if (node.leaderState == ls && node.reads.awaitsReadConfirmation(ls, peer)) {
            replicateTo(peer, true);
        }
    }

    private void advanceReplication(LeaderState ls, String peer, AppendEntriesRequest req, AppendEntriesResponse resp,
                                    boolean current) {
        var match = ls.getMatchIndex().getOrDefault(peer, 0L);
        if (!resp.success) {
            if (!current) {
                return; // lời từ chối cho một request của lần trước: lần dò hiện tại đã tính tới chuyện đó
            }
            // lùi theo gợi ý của follower, không bao giờ lùi quá phần đã match
            var sentNext = req.prevLogIndex + 1;
            restartProbe(ls, peer, Math.max(match + 1, Math.min(sentNext - 1, resp.matchIndex + 1)));
            return;
        }

        match = Math.max(match, req.prevLogIndex + req.entryCount());
        ls.getMatchIndex().put(peer, match);
        var progress = ls.progress(peer);
        // pipeline: nextIndex đã đi trước, chỉ không để nó tụt sau phần đã match
        ls.getNextIndex().merge(peer, match + 1, Math::max);
        if (current && !progress.pipelining) {
            // log đã khớp: từ giờ gửi liên tiếp không chờ
            progress.pipelining = true;
            ls.getNextIndex().put(peer, match + 1);
        }
        node.membership.onLearnerProgress(ls, peer, match);
        // node bị gỡ cần cả C(new) lẫn commit index phủ tới nó thì mới biết chắc mình đã rời cluster
        var needed = ls.getDeparting().get(peer);
        if (needed != null && match >= needed && req.leaderCommit >= needed) {
            node.membership.removeDeparting(ls, peer);
        }
        maybeAdvanceCommitIndex();
        // commit có thể vừa khiến node step-down, hoặc leader đã append thêm trong lúc chờ
        if (node.leaderState == ls && node.membership.replicationTargets().contains(peer)) {
            replicateTo(peer);
        }
    }

    // quay về gửi từng request một, bắt đầu từ next; response của các request đã gửi trước đó không còn được chờ
    private void restartProbe(LeaderState ls, String peer, long next) {
        ls.progress(peer).restartProbe();
        ls.getNextIndex().put(peer, next);
        if (node.membership.replicationTargets().contains(peer)) {
            replicateTo(peer, true);
        }
    }

    // gửi mẩu kế tiếp của snapshot cho peer; mỗi lần chỉ một mẩu nằm trong bộ nhớ
    private void sendSnapshot(LeaderState ls, String peer, long generation) {
        var snapshotStore = node.persistent.getSnapshotStore();
        var meta = snapshotStore.getMeta();
        var existing = ls.getSnapshotTransfers().get(peer);
        // snapshot mới hơn đã thay snapshot đang gửi dở: bắt đầu lại với bản mới
        var transfer = existing != null && existing.index == meta.getLastIncludedIndex()
                ? existing : new LeaderState.SnapshotTransfer(meta.getLastIncludedIndex(), snapshotStore.currentPath());
        ls.getSnapshotTransfers().put(peer, transfer);
        var term = node.persistent.getCurrentTerm();
        var fileIndex = transfer.fileIndex;
        var offset = transfer.offset;
        // đọc file snapshot ở thread IO, ngoài lock
        node.runIo(() -> {
            try {
                var files = meta.getFiles();
                String fileName = files.isEmpty() ? null : files.get(fileIndex);
                byte[] data = new byte[0];
                var endOfFile = true;
                if (fileName != null) {
                    try (FileChannel channel = FileChannel.open(Path.of(transfer.path, fileName), StandardOpenOption.READ)) {
                        var buffer = ByteBuffer.allocate((int) Math.min(node.nodeOptions.getSnapshotChunkBytes(), channel.size() - offset));
                        while (buffer.hasRemaining() && channel.read(buffer, offset + buffer.position()) >= 0) {
                            // đọc cho đầy mẩu
                        }
                        data = Arrays.copyOf(buffer.array(), buffer.position());
                        endOfFile = offset + data.length >= channel.size();
                    }
                }
                var lastChunkOfFile = endOfFile;
                var done = fileName == null || (lastChunkOfFile && fileIndex == files.size() - 1);
                var req = new InstallSnapshotRequest(term, node.nodeId, meta.getLastIncludedIndex(), meta.getLastIncludedTerm(),
                        meta.getConf(), meta.getSessions(), files, fileName, offset, data, done);
                req.setLastIncludedTimestamp(meta.getLastIncludedTimestamp());
                node.rpcProcessor.installSnapshot(peer, req).whenComplete((resp, error) ->
                        node.onNode(() -> onInstallSnapshotResponse(ls, peer, req, resp, transfer, lastChunkOfFile, generation)));
            } catch (Exception e) {
                // thường là snapshot vừa bị thay bằng bản mới hơn: lần sau gửi lại từ đầu
                log.error("Leader {} failed to send snapshot to {}", node.nodeId, peer, e);
                node.onNode(() -> {
                    var progress = ls.progress(peer);
                    if (progress.generation == generation) {
                        progress.inflight = Math.max(0, progress.inflight - 1);
                    }
                    ls.getSnapshotTransfers().remove(peer);
                });
            }
        });
    }

    private void onInstallSnapshotResponse(LeaderState ls, String peer, InstallSnapshotRequest req, InstallSnapshotResponse resp,
                                           LeaderState.SnapshotTransfer transfer, boolean lastChunkOfFile, long generation) {
        var progress = ls.progress(peer);
        if (progress.generation == generation) {
            progress.inflight = Math.max(0, progress.inflight - 1);
            progress.lastActivityNanos = node.runtime.nanoTime();
        }
        if (resp == null || !isCurrentLeader(ls, resp.getTerm())) {
            return; // không rõ mẩu đã tới chưa: nhịp sau gửi lại đúng mẩu này
        }
        ls.getLastAck().put(peer, node.runtime.nanoTime());
        if (!resp.isSuccess()) {
            ls.getSnapshotTransfers().remove(peer); // follower không nối được mẩu này: gửi lại từ đầu
            return;
        }
        if (!resp.isComplete()) {
            if (lastChunkOfFile) {
                transfer.fileIndex++;
                transfer.offset = 0;
            } else {
                transfer.offset += req.getData().length;
            }
            if (node.leaderState == ls) {
                replicateTo(peer); // mẩu kế tiếp
            }
            return;
        }
        ls.getSnapshotTransfers().remove(peer);
        var match = Math.max(ls.getMatchIndex().getOrDefault(peer, 0L), req.getLastIncludedIndex());
        ls.getMatchIndex().put(peer, match);
        ls.getNextIndex().put(peer, match + 1);
        node.membership.onLearnerProgress(ls, peer, match);
        maybeAdvanceCommitIndex();
        if (node.leaderState == ls) {
            replicateTo(peer, true); // gửi tiếp phần log sau snapshot, và dò xem log sau đó có khớp không
        }
    }

    // response chỉ còn giá trị nếu node vẫn là leader của đúng nhiệm kỳ đã gửi request.
    // Thấy term cao hơn thì step-down luôn tại đây.
    private boolean isCurrentLeader(LeaderState ls, long responseTerm) {
        if (node.stopped) {
            return false;
        }
        if (responseTerm > node.persistent.getCurrentTerm()) {
            node.election.becomeFollower(responseTerm);
            return false;
        }
        return node.leaderState == ls;
    }

    private void maybeAdvanceCommitIndex() {
        if (node.state != NodeState.LEADER) {
            return;
        }
        var matchIndex = node.leaderState.getMatchIndex();
        // leader chỉ tính chính nó tới phần log đã nằm trên đĩa
        var durableIndex = node.persistent.getLogStore().durableIndex();
        var candidate = node.membership.conf.quorumIndex(
                id -> id.equals(node.nodeId) ? durableIndex : matchIndex.getOrDefault(id, 0L));
        // chỉ commit trực tiếp entry của term hiện tại; entry của term cũ đứng trước nó được commit theo.
        // Term trong log không giảm, nên nếu candidate thuộc term cũ thì mọi index nhỏ hơn cũng vậy.
        if (candidate > node.commitIndex && node.termAt(candidate) == node.persistent.getCurrentTerm()) {
            node.commitIndex = candidate;
            node.applier.applyCommitted();
        }
    }
}

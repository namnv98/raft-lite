package com.namnv.raft;

import com.namnv.rpc.model.request.InstallSnapshotRequest;
import com.namnv.rpc.model.response.InstallSnapshotResponse;
import com.namnv.storage.snapshot.SnapshotMeta;
import com.namnv.storage.snapshot.SnapshotReader;
import com.namnv.storage.snapshot.SnapshotWriter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Snapshot của node: khôi phục state machine lúc khởi động, tạo snapshot theo chu kỳ rồi compact log, và nhận snapshot
 * leader gửi từng mẩu (InstallSnapshot). Phía leader gửi snapshot nằm trong {@link Replicator}.
 */
@Slf4j
final class Snapshots {
    private final RaftNode node;

    // đang tạo hoặc cài snapshot: thư mục temp của snapshot store chỉ dùng được cho một việc
    private boolean snapshotting;
    // state machine đang load snapshot ngoài lock, tạm hoãn apply
    boolean loadingSnapshot;
    // snapshot đang nhận dần từ leader; trong lúc đó cờ snapshotting cũng được bật
    private IncomingSnapshot incomingSnapshot;

    // bộ đếm cho metrics()
    long snapshotsCreated;
    long snapshotsInstalled;

    Snapshots(RaftNode node) {
        this.node = node;
    }

    // lúc khởi động: dựng state machine từ snapshot trên đĩa rồi apply phần log đã commit sau nó
    void restoreStateMachine() {
        var persistent = node.persistent;
        var snapshotStore = persistent.getSnapshotStore();
        var meta = snapshotStore.getMeta();
        if (meta != null) {
            if (!node.stateMachine.onSnapshotLoad(new SnapshotReader(snapshotStore.currentPath()))) {
                throw new IllegalStateException("Node " + node.nodeId + " failed to load snapshot at index " + meta.getLastIncludedIndex());
            }
            node.lastApplied = meta.getLastIncludedIndex();
            node.commitIndex = meta.getLastIncludedIndex();
            node.applier.restoreSessions(meta.getSessions());
        }

        var durableCommit = Math.min(persistent.getLastCommitIndex(), persistent.getLogStore().lastIndex());
        if (durableCommit > node.commitIndex) {
            node.commitIndex = durableCommit;
            node.applier.applyCommitted();
        }
    }

    void maybeSnapshot() {
        var interval = node.nodeOptions.getSnapshotIntervalEntries();
        var uncompacted = node.lastApplied - node.persistent.getLogStore().getBaseIndex();
        if (interval > 0 && node.state != null && uncompacted >= interval) {
            create();
        }
    }

    // xem RaftNode.createSnapshot
    void create() {
        node.lock.lock();
        try {
            abortStaleIncomingSnapshot();
            if (node.stopped || snapshotting) {
                return;
            }
            var snapshotStore = node.persistent.getSnapshotStore();
            var snapshotIndex = node.lastApplied;
            if (snapshotIndex <= node.persistent.getLogStore().getBaseIndex()) {
                return;
            }
            var meta = new SnapshotMeta(snapshotIndex, node.termAt(snapshotIndex), node.membership.confAt(snapshotIndex),
                    new ArrayList<>(), Applier.copySessions(node.applier.sessions));
            meta.setLastIncludedTimestamp(node.timestampAt(snapshotIndex));
            snapshotting = true;
            try {
                // state machine ghi vào thư mục temp, chỉ khi commit() mới thay thế snapshot hiện tại
                var writer = new SnapshotWriter(snapshotStore.prepareTemp(), meta.getFiles());
                node.stateMachine.onSnapshotSave(writer).whenComplete((saved, error) -> {
                    if (error == null) {
                        // Chốt snapshot đọc lại mọi file của nó để tính checksum, có thể hàng trăm MB: làm ở thread dành cho
                        // việc đĩa không gấp, không phải thread IO đang fsync log, để các lệnh ghi không phải chờ nó
                        node.runtime.executeRead(() -> {
                            try {
                                finishSnapshot(meta);
                            } catch (RuntimeException e) {
                                log.error("Node {} failed to commit snapshot at index {}", node.nodeId, meta.getLastIncludedIndex(), e);
                            }
                        });
                    } else {
                        log.error("Node {} failed to save snapshot", node.nodeId, error);
                        endSnapshotting();
                    }
                });
            } catch (Exception e) {
                snapshotting = false;
                log.error("Node {} failed to create snapshot", node.nodeId, e);
            }
        } finally {
            node.unlock();
        }
    }

    // leader chết giữa lúc gửi snapshot thì lần nhận dở không bao giờ xong: bỏ nó để node tự tạo snapshot được
    private void abortStaleIncomingSnapshot() {
        var stale = TimeUnit.MILLISECONDS.toNanos(10L * node.nodeOptions.getElectionTimeoutMaxMs());
        if (incomingSnapshot != null && !incomingSnapshot.busy
                && node.runtime.nanoTime() - incomingSnapshot.lastChunkNanos > stale) {
            abortIncomingSnapshot();
        }
    }

    // chạy ở thread IO: ghi meta + rename ngoài lock, sau đó mới bỏ phần log đã nằm trong snapshot
    private void finishSnapshot(SnapshotMeta meta) {
        try {
            node.persistent.getSnapshotStore().commit(meta);
        } catch (IOException e) {
            endSnapshotting();
            throw new UncheckedIOException(e);
        }
        node.lock.lock();
        try {
            snapshotting = false;
            snapshotsCreated++;
            if (!node.stopped) {
                node.persistent.getLogStore().truncatePrefix(meta.getLastIncludedIndex() + 1);
                cleanupLog();
            }
        } finally {
            node.unlock();
        }
    }

    // Xoá file của phần log vừa được compact ở thread nền. Làm ngay trong lock thì cả node đứng hàng chục mili giây mỗi lần
    // snapshot, và mọi node của cluster snapshot ở cùng một index nên chúng đứng cùng lúc.
    private void cleanupLog() {
        node.runtime.executeRead(() -> {
            try {
                node.persistent.getLogStore().cleanup();
            } catch (Exception e) {
                log.error("Node {} failed to delete compacted log segments", node.nodeId, e);
            }
        });
    }

    private void endSnapshotting() {
        node.lock.lock();
        try {
            snapshotting = false;
        } finally {
            node.unlock();
        }
    }

    // ---------- InstallSnapshot từ leader ----------

    InstallSnapshotResponse handleInstallSnapshotRequest(InstallSnapshotRequest req) {
        var response = installSnapshot(req);
        try {
            node.persistent.syncVote();
        } catch (Exception e) {
            log.error("Node {} failed to persist term", node.nodeId, e);
            return snapshotResponse(false);
        }
        return response;
    }

    // success=true ở đây nghĩa là follower đã có đủ state của snapshot
    private InstallSnapshotResponse snapshotResponse(boolean success) {
        return new InstallSnapshotResponse(node.persistent.getCurrentTerm(), success, success);
    }

    // snapshot mà node đang nhận dần từ leader
    private static final class IncomingSnapshot {
        final String leaderId;
        final long index;
        final List<String> files;
        String tempPath;
        // mẩu kế tiếp phải thuộc file này, tại vị trí này (hoặc là mẩu đầu của file kế tiếp)
        int fileIndex;
        long offset;
        // một mẩu đang được ghi xuống đĩa ngoài lock
        boolean busy;
        long lastChunkNanos;

        IncomingSnapshot(String leaderId, long index, List<String> files, long nowNanos) {
            this.leaderId = leaderId;
            this.index = index;
            this.files = files;
            this.lastChunkNanos = nowNanos;
        }

        // nhận mẩu nếu nó nối tiếp đúng chỗ; chuyển sang file kế tiếp khi cần
        boolean accept(InstallSnapshotRequest req) {
            if (req.getLastIncludedIndex() != index || !leaderId.equals(req.getLeaderId()) || fileIndex >= files.size()) {
                return false;
            }
            if (files.get(fileIndex).equals(req.getFileName()) && req.getOffset() == offset) {
                return true;
            }
            if (fileIndex + 1 < files.size() && files.get(fileIndex + 1).equals(req.getFileName()) && req.getOffset() == 0) {
                fileIndex++;
                offset = 0;
                return true;
            }
            return false;
        }
    }

    private void abortIncomingSnapshot() {
        incomingSnapshot = null;
        snapshotting = false;
    }

    /**
     * Nhận một mẩu snapshot, chỉ giữ lock ở các bước đổi state trong bộ nhớ. Khi nhận mẩu cuối:
     * state machine load từ temp → commit snapshot → cập nhật log và index.
     * Load trước rồi mới commit: nếu load hỏng thì snapshot và log cũ trên đĩa còn nguyên để restart dựng lại.
     */
    private InstallSnapshotResponse installSnapshot(InstallSnapshotRequest req) {
        var rejected = acceptSnapshotChunk(req);
        if (rejected != null) {
            return rejected;
        }

        var incoming = incomingSnapshot;
        var index = req.getLastIncludedIndex();
        var snapshotStore = node.persistent.getSnapshotStore();
        try {
            if (incoming.tempPath == null) {
                incoming.tempPath = snapshotStore.prepareTemp();
            }
            writeSnapshotChunk(incoming.tempPath, req);
        } catch (Exception e) {
            log.error("Node {} failed to store snapshot chunk", node.nodeId, e);
            node.lock.lock();
            try {
                abortIncomingSnapshot();
            } finally {
                node.unlock();
            }
            return snapshotResponse(false);
        }

        node.lock.lock();
        try {
            incoming.busy = false;
            incoming.offset += req.getData().length;
            incoming.lastChunkNanos = node.runtime.nanoTime();
            if (node.stopped || incomingSnapshot != incoming) {
                return snapshotResponse(false);
            }
            if (!req.isDone()) {
                return new InstallSnapshotResponse(node.persistent.getCurrentTerm(), true, false); // chờ mẩu kế tiếp
            }
            // đã đủ mọi mẩu: phần còn lại vẫn giữ cờ snapshotting cho tới khi cài xong
            incomingSnapshot = null;
        } finally {
            node.unlock();
        }

        var meta = new SnapshotMeta(index, req.getLastIncludedTerm(), req.getConf(), req.getFiles(), req.getSessions());
        meta.setLastIncludedTimestamp(req.getLastIncludedTimestamp());
        var needLoad = beginSnapshotLoad(index);
        if (needLoad && !loadSnapshot(incoming.tempPath)) {
            // không kiểm chứng được state machine còn nguyên hay không, nên false cũng dừng như exception
            return failStop("state machine could not load snapshot at index " + index);
        }
        try {
            // snapshot chỉ chứa dữ liệu đã commit nên lưu lại luôn an toàn, kể cả khi term đổi trong lúc ghi
            snapshotStore.commit(meta);
        } catch (Exception e) {
            log.error("Node {} failed to commit snapshot", node.nodeId, e);
            if (needLoad) {
                return failStop("snapshot at index " + index + " was loaded but could not be saved");
            }
            endSnapshotting();
            return snapshotResponse(false);
        }
        return finishInstallSnapshot(req, needLoad);
    }

    // trả về response nếu request kết thúc ngay tại đây, null nếu mẩu này cần được ghi xuống đĩa
    private InstallSnapshotResponse acceptSnapshotChunk(InstallSnapshotRequest req) {
        node.lock.lock();
        try {
            node.ensureRunning();
            if (req.getTerm() < node.persistent.getCurrentTerm()) {
                return snapshotResponse(false);
            }
            node.election.handleHeartbeat(req.getTerm(), req.getLeaderId());

            if (req.getLastIncludedIndex() <= node.commitIndex) {
                // đã có sẵn toàn bộ dữ liệu của snapshot này
                return snapshotResponse(true);
            }
            var files = req.getFiles();
            var firstChunk = req.getOffset() == 0 && (files.isEmpty() || files.get(0).equals(req.getFileName()));
            if (firstChunk) {
                // bắt đầu (lại) một lần truyền; lần truyền dở trước đó của leader cũ bị bỏ
                var busy = incomingSnapshot != null ? incomingSnapshot.busy : snapshotting;
                if (busy) {
                    return snapshotResponse(false); // thư mục temp đang được dùng
                }
                incomingSnapshot = new IncomingSnapshot(req.getLeaderId(), req.getLastIncludedIndex(), files, node.runtime.nanoTime());
                snapshotting = true;
            } else if (incomingSnapshot == null || incomingSnapshot.busy || !incomingSnapshot.accept(req)) {
                // mẩu không nối tiếp đúng chỗ (mất mẩu, gửi trùng, đổi leader): leader sẽ gửi lại từ đầu
                if (incomingSnapshot != null && !incomingSnapshot.busy) {
                    abortIncomingSnapshot();
                }
                return snapshotResponse(false);
            }
            incomingSnapshot.busy = true;
            return null;
        } finally {
            node.unlock();
        }
    }

    private void writeSnapshotChunk(String folder, InstallSnapshotRequest req) throws IOException {
        var name = req.getFileName();
        if (name == null) {
            return; // snapshot không có file nào
        }
        if (name.contains("/") || name.contains("\\") || name.equals("..")) {
            throw new IOException("Illegal snapshot file name: " + name);
        }
        var options = req.getOffset() == 0
                ? new StandardOpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING}
                : new StandardOpenOption[]{StandardOpenOption.WRITE};
        try (FileChannel channel = FileChannel.open(Path.of(folder, name), options)) {
            var buffer = ByteBuffer.wrap(req.getData());
            long position = req.getOffset();
            while (buffer.hasRemaining()) {
                position += channel.write(buffer, position);
            }
            // từng mẩu đều được fsync, nên khi mẩu cuối về tới thì cả file đã nằm trên đĩa
            channel.force(true);
        }
    }

    // hoãn applyCommitted để không có onApply nào chen vào lúc state machine load ngoài lock.
    // Trả về false nếu trong lúc ghi file node đã tự commit qua điểm này, khi đó không cần load nữa.
    private boolean beginSnapshotLoad(long snapshotIndex) {
        node.lock.lock();
        try {
            loadingSnapshot = !node.stopped && snapshotIndex > node.commitIndex;
            return loadingSnapshot;
        } finally {
            node.unlock();
        }
    }

    private boolean loadSnapshot(String path) {
        try {
            return node.stateMachine.onSnapshotLoad(new SnapshotReader(path));
        } catch (Exception e) {
            log.error("Node {} failed to load snapshot", node.nodeId, e);
            return false;
        }
    }

    private InstallSnapshotResponse finishInstallSnapshot(InstallSnapshotRequest req, boolean loaded) {
        var index = req.getLastIncludedIndex();
        node.lock.lock();
        try {
            snapshotting = false;
            loadingSnapshot = false;
            if (node.stopped) {
                return snapshotResponse(false);
            }
            var logStore = node.persistent.getLogStore();
            if (loaded) {
                var local = logStore.get(index);
                if (local != null && local.getTerm() == req.getLastIncludedTerm()) {
                    // log khớp với snapshot thì giữ lại phần phía sau
                    logStore.truncatePrefix(index + 1);
                    cleanupLog();
                } else {
                    logStore.reset(index, req.getLastIncludedTerm());
                }
                node.lastApplied = index;
                node.commitIndex = Math.max(node.commitIndex, index);
                node.applier.restoreSessions(req.getSessions());
            } else {
                // state machine đã đi qua điểm này: chỉ cần bỏ phần log mà snapshot đã bao phủ
                logStore.truncatePrefix(index + 1);
                cleanupLog();
            }
            node.membership.refreshConf();
            if (loaded) {
                snapshotsInstalled++;
                log.info("Node {} installed snapshot at index {} from {}.", node.nodeId, index, req.getLeaderId());
                // apply các entry được commit trong lúc load, đồng thời lưu commit index
                node.applier.applyCommitted();
            }
            return snapshotResponse(true);
        } catch (Exception e) {
            log.error("Node {} failed to install snapshot", node.nodeId, e);
            // state machine đã mang state mới mà log/index chưa theo kịp thì không chạy tiếp được
            return loaded ? failStop("snapshot at index " + index + " was loaded but could not be installed")
                    : snapshotResponse(false);
        } finally {
            node.unlock();
        }
    }

    // state machine trong bộ nhớ không còn đáng tin: dừng hẳn, restart sẽ dựng lại từ snapshot và log trên đĩa
    private InstallSnapshotResponse failStop(String reason) {
        log.error("Node {} stops: {}.", node.nodeId, reason);
        node.shutdown();
        return snapshotResponse(false);
    }
}

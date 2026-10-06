package com.namnv.state;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.namnv.config.NodeOptions;
import com.namnv.entity.ConfigurationEntry;
import com.namnv.storage.Checksum;
import com.namnv.storage.DiskFaultInjector;
import com.namnv.storage.FileUtil;
import com.namnv.storage.LogStorage;
import com.namnv.storage.SnapshotStore;
import com.namnv.storage.binary.BinaryLogStorage;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Setter chỉ đổi giá trị trong bộ nhớ nên gọi được trong lock của node.
 * Việc ghi đĩa nằm ở syncVote()/flush(), gọi ngoài lock.
 */
public class PersistentState {

    private long currentTerm = 0;
    private String votedFor = null;
    private long lastCommitIndex = 0;

    // tăng mỗi lần term/vote đổi; term/vote đã bền vững khi persistedVoteVersion đuổi kịp
    private long voteVersion;
    private long persistedVoteVersion;
    private boolean commitIndexDirty;
    // chỉ để các lần ghi file không chồng lên nhau, không giữ cùng lúc với monitor của this khi đang ghi
    private final ReentrantLock writeLock = new ReentrantLock();

    @Getter
    private final SnapshotStore snapshotStore;
    @Getter
    private final LogStorage logStore;
    private final File stateFile;
    // Commit index nằm ở file riêng, ghi đè không fsync. Nó được ghi theo nhịp (mỗi giây khi có tải), và fsync một file
    // bất kỳ trên ext4 kéo theo việc đẩy cả lượng log đang chờ ghi xuống đĩa: mọi lệnh đang append bị treo hàng trăm
    // mili giây mỗi lần. Mất hay hỏng file này sau khi mất điện không sao: node chỉ apply lại chậm hơn lúc khởi động.
    private final File commitIndexFile;
    private final DiskFaultInjector faults;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    @SneakyThrows
    public PersistentState(NodeOptions nodeOptions) {
        var folder = new File(nodeOptions.getRaftMetaUri());
        if (!folder.exists()) {
            folder.mkdirs();
        }
        this.stateFile = new File(folder, "raft_meta.json");
        this.commitIndexFile = new File(folder, "commit_index");
        load();
        this.faults = nodeOptions.getDiskFaults();
        this.snapshotStore = new SnapshotStore(nodeOptions.getSnapshotUri(), faults);
        this.logStore = openLog(nodeOptions);
    }

    private LogStorage openLog(NodeOptions nodeOptions) throws IOException {
        // Log dạng JSON của các phiên bản trước không còn đọc được. Mở thư mục đó sẽ thấy một log rỗng và node sẽ chạy
        // tiếp như thể chưa từng ghi gì, nên từ chối thay vì bỏ qua.
        var folder = new File(nodeOptions.getLogUri());
        var legacy = folder.list((dir, name) -> name.startsWith("log_") && name.endsWith(".jsonl"));
        if (legacy != null && legacy.length > 0) {
            throw new IOException("Log in " + folder + " was written in the old JSON format, which is no longer supported"
                    + " (found " + legacy[0] + ")");
        }
        return new BinaryLogStorage(nodeOptions, getLastSnapshotIndex(), getLastSnapshotTerm());
    }

    private synchronized void load() throws IOException {
        if (stateFile.length() != 0) {
            var bytes = Files.readAllBytes(stateFile.toPath());
            var data = objectMapper.readValue(Checksum.unwrap(bytes, stateFile.toString()), StateData.class);
            this.currentTerm = data.currentTerm;
            this.votedFor = data.votedFor;
            this.lastCommitIndex = data.lastCommitIndex;
        }
        // commit index không bao giờ lùi, nên giá trị lớn hơn trong hai file là giá trị mới hơn
        this.lastCommitIndex = Math.max(lastCommitIndex, loadCommitIndex());
    }

    // 0 nếu chưa có file, hoặc file bị ghi dở khi crash (sai checksum)
    private long loadCommitIndex() {
        try {
            if (commitIndexFile.length() == 0) {
                return 0;
            }
            var payload = Checksum.unwrap(Files.readAllBytes(commitIndexFile.toPath()), commitIndexFile.toString());
            return Long.parseLong(new String(payload, StandardCharsets.US_ASCII));
        } catch (IOException | NumberFormatException e) {
            return 0;
        }
    }

    /**
     * Ghi term và votedFor xuống đĩa nếu chúng vừa đổi. Phải gọi xong trước khi một phiếu bầu
     * (kể cả phiếu tự bầu của candidate) được gửi đi hay được tính.
     */
    // term/vote đã đổi mà chưa nằm trên đĩa
    public synchronized boolean hasUnsyncedVote() {
        return voteVersion != persistedVoteVersion;
    }

    public void syncVote() {
        write(true);
    }

    // ghi mọi thay đổi còn treo, kể cả commit index
    public void flush() {
        write(false);
    }

    private void write(boolean voteOnly) {
        writeLock.lock();
        try {
            StateData data = new StateData();
            long version;
            boolean hadCommitIndex;
            boolean voteChanged;
            synchronized (this) {
                boolean voteDirty = voteVersion != persistedVoteVersion;
                voteChanged = voteDirty;
                if (!voteDirty && (voteOnly || !commitIndexDirty)) {
                    return;
                }
                data.currentTerm = this.currentTerm;
                data.votedFor = this.votedFor;
                data.lastCommitIndex = this.lastCommitIndex;
                version = voteVersion;
                hadCommitIndex = commitIndexDirty;
                commitIndexDirty = false;
            }
            try {
                faults.beforeWrite("meta.write");
                if (voteChanged) {
                    // term và phiếu bầu phải bền vững: ghi file tạm, fsync, rename
                    FileUtil.atomicWrite(stateFile.toPath(), Checksum.wrap(objectMapper.writeValueAsBytes(data)));
                } else {
                    Files.write(commitIndexFile.toPath(),
                            Checksum.wrap(Long.toString(data.lastCommitIndex).getBytes(StandardCharsets.US_ASCII)));
                }
            } catch (IOException e) {
                synchronized (this) {
                    commitIndexDirty |= hadCommitIndex;
                }
                // không được ack khi term/vote chưa nằm trên đĩa
                throw new UncheckedIOException(e);
            }
            synchronized (this) {
                persistedVoteVersion = version;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public synchronized long getCurrentTerm() {
        return currentTerm;
    }

    public synchronized String getVotedFor() {
        return votedFor;
    }

    public synchronized void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
        this.voteVersion++;
    }

    public synchronized void setTermAndVote(long term, String votedFor) {
        this.currentTerm = term;
        this.votedFor = votedFor;
        this.voteVersion++;
    }

    public synchronized long getLastCommitIndex() {
        return lastCommitIndex;
    }

    // commit index không cần bền vững để đảm bảo an toàn, chỉ giúp restart apply lại nhanh hơn
    public synchronized void setLastCommitIndex(long idx) {
        if (this.lastCommitIndex == idx) {
            return;
        }
        this.lastCommitIndex = idx;
        this.commitIndexDirty = true;
    }

    public long getLastSnapshotIndex() {
        var meta = snapshotStore.getMeta();
        return meta == null ? 0 : meta.getLastIncludedIndex();
    }

    public long getLastSnapshotTerm() {
        var meta = snapshotStore.getMeta();
        return meta == null ? 0 : meta.getLastIncludedTerm();
    }

    public ConfigurationEntry getSnapshotConf() {
        var meta = snapshotStore.getMeta();
        return meta == null ? null : meta.getConf();
    }

    private static class StateData {
        public long currentTerm;
        public String votedFor;
        public long lastCommitIndex;
    }
}

package com.namnv.state;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.namnv.config.NodeOptions;
import com.namnv.entity.ConfigurationEntry;
import com.namnv.storage.Checksum;
import com.namnv.storage.DiskFaultInjector;
import com.namnv.storage.FileLogStorage;
import com.namnv.storage.FileUtil;
import com.namnv.storage.LogStorage;
import com.namnv.storage.SnapshotStore;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
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
        load();
        this.faults = nodeOptions.getDiskFaults();
        this.snapshotStore = new SnapshotStore(nodeOptions.getSnapshotUri(), faults);
        this.logStore = new FileLogStorage(nodeOptions, getLastSnapshotIndex(), getLastSnapshotTerm());
    }

    private synchronized void load() throws IOException {
        if (stateFile.length() == 0) {
            return;
        }
        var bytes = Files.readAllBytes(stateFile.toPath());
        var data = objectMapper.readValue(Checksum.unwrap(bytes, stateFile.toString()), StateData.class);
        this.currentTerm = data.currentTerm;
        this.votedFor = data.votedFor;
        this.lastCommitIndex = data.lastCommitIndex;
    }

    /**
     * Ghi term và votedFor xuống đĩa nếu chúng vừa đổi. Phải gọi xong trước khi một phiếu bầu
     * (kể cả phiếu tự bầu của candidate) được gửi đi hay được tính.
     */
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
            synchronized (this) {
                boolean voteDirty = voteVersion != persistedVoteVersion;
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
                FileUtil.atomicWrite(stateFile.toPath(), Checksum.wrap(objectMapper.writeValueAsBytes(data)));
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

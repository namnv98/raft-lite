package com.namnv.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.namnv.config.NodeOptions;
import com.namnv.storage.FileLogStorage;
import com.namnv.storage.LogStorage;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.File;
import java.io.IOException;

@Data
public class PersistentState {

    private long currentTerm = 0;
    private String votedFor = null;
    private long lastCommitIndex = 0;

    private long lastSnapshotIndex = 0;
    private long lastSnapshotTerm = 0;

    private final LogStorage logStore;
    private final File stateFile;

    private final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    @SneakyThrows
    public PersistentState(NodeOptions nodeOptions) {
        var folder = new File(nodeOptions.getRaftMetaUri());
        if (!folder.exists()) {
            folder.mkdirs();
        }
        this.stateFile = new File(folder, "raft_meta.json");
        if (!stateFile.exists()) {
            stateFile.createNewFile();
        }
        load();
        this.logStore = new FileLogStorage(nodeOptions, lastSnapshotIndex, lastSnapshotTerm);
    }

    private synchronized void load() throws IOException {
        if (stateFile.length() == 0) {
            return;
        }
        var data = objectMapper.readValue(stateFile, StateData.class);
        this.currentTerm = data.currentTerm;
        this.votedFor = data.votedFor;
        this.lastCommitIndex = data.lastCommitIndex;
        this.lastSnapshotIndex = data.lastSnapshotIndex;
        this.lastSnapshotTerm = data.lastSnapshotTerm;
    }

    public synchronized void persist() {
        try {
            StateData data = new StateData();
            data.currentTerm = this.currentTerm;
            data.votedFor = this.votedFor;
            data.lastCommitIndex = this.lastCommitIndex;
            data.lastSnapshotIndex = this.lastSnapshotIndex;
            data.lastSnapshotTerm = this.lastSnapshotTerm;
            objectMapper.writeValue(stateFile, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized long getCurrentTerm() {
        return currentTerm;
    }

    public synchronized void setCurrentTerm(long term) {
        this.currentTerm = term;
        persist();
    }

    public synchronized String getVotedFor() {
        return votedFor;
    }

    public synchronized void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
        persist();
    }

    public synchronized long getLastCommitIndex() {
        return lastCommitIndex;
    }

    public synchronized void setLastCommitIndex(long idx) {
        this.lastCommitIndex = idx;
        persist();
    }

    public synchronized long getLastSnapshotIndex() {
        return lastSnapshotIndex;
    }

    public synchronized void setLastSnapshotIndex(long idx) {
        this.lastSnapshotIndex = idx;
        persist();
    }

    public synchronized long getLastSnapshotTerm() {
        return lastSnapshotTerm;
    }

    public synchronized void setLastSnapshotTerm(long term) {
        this.lastSnapshotTerm = term;
        persist();
    }

    @Data
    private static class StateData {
        public long currentTerm;
        public String votedFor;
        public long lastCommitIndex;
        public long lastSnapshotIndex;
        public long lastSnapshotTerm;
    }
}

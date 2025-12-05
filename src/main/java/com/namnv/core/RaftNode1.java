//package com.namnv.core;
//
//import com.namnv.config.RaftConfig;
//import com.namnv.entity.LogEntry;
//import com.namnv.rpc.InProcessRPC;
//import com.namnv.rpc.RaftNodeRPCHandler;
//import com.namnv.rpc.RaftRPC;
//import com.namnv.rpc.model.*;
//import com.namnv.state.LeaderState;
//import com.namnv.state.PersistentState;
//import com.namnv.state.Snapshot;
//import com.namnv.state.VolatileState;
//import com.namnv.statemachine.StateMachine;
//import com.namnv.storage.ConfigurationEntry;
//import com.namnv.storage.LogStore;
//import com.namnv.timer.ElectionTimer;
//import com.namnv.timer.HeartbeatTimer;
//import lombok.Getter;
//import lombok.extern.slf4j.Slf4j;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.locks.Lock;
//import java.util.concurrent.locks.ReentrantLock;
//
//@Slf4j
//@Getter
//public class RaftNode1 implements RaftNodeRPCHandler {
//
//    public enum NodeState {
//        LEADER,// It's a leader
//        CANDIDATE,// It's a candidate
//        FOLLOWER,// It's a follower
//    }
//
//    private final RaftConfig config;
//    private final RaftRPC rpc;
//
//    private final PersistentState persistent;
//    private final VolatileState volatileState = new VolatileState();
//
//    private final String nodeId;
//    private volatile NodeState state;
//
//    private String leaderId;
//    private LeaderState leaderState;
//
//    private final ElectionTimer electionTimer;
//    private final HeartbeatTimer heartbeatTimer;
//
//    private final StateMachine stateMachine;
//
//    private final Lock lock = new ReentrantLock();
//
//    private final ConfigurationEntry conf;
//
//    private final Map<Long, CompletableFuture<Boolean>> pendingFutures = new HashMap<>();
//    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
//    private final long CLIENT_TIMEOUT_MS = 5000;
//
//    public RaftNode1(String nodeId, List<String> cluster, RaftConfig config, RaftRPC rpc, LogStore logStore, String nodeFolder, StateMachine sm) {
//        this.nodeId = nodeId;
//        this.config = config;
//        this.rpc = rpc;
//        this.persistent = new PersistentState(logStore, nodeFolder);
//        this.stateMachine = sm;
//        this.electionTimer = new ElectionTimer(config.getElectionTimeoutMinMs(), config.getElectionTimeoutMaxMs(), this::onElectionTimeout);
//        this.heartbeatTimer = new HeartbeatTimer(config.getHeartbeatIntervalMs(), this::sendHeartbeats);
//        this.conf = new ConfigurationEntry(
//                new ArrayList<>(cluster),     // oldNodes = cluster hiện tại
//                new ArrayList<>(),
//                false                    // isJoint = false, vì đây là final config ban đầu
//        );
//    }
//
//    public void start() {
//        if (rpc instanceof InProcessRPC) {
//            ((InProcessRPC) rpc).register(nodeId, this);
//        }
//        this.state = NodeState.FOLLOWER;
//        // recover stateMachine từ Snapshot or lastCommitIndex
//        recoverStateMachineFromSnapshot();
//        electionTimer.start();
//    }
//
//    public void onJoinPeerCluster(String newNodeId) {
//        lock.lock();
//        try {
//            if (state != RaftNode1.NodeState.LEADER || leaderState == null) {
//                return;
//            }
//
//            // Phase 1: joint configuration (old + new nodes)
//            List<String> oldNodes = new ArrayList<>(conf.getOldNodes()); // old cluster = current cluster
//            List<String> newNodes = new ArrayList<>(conf.getOldNodes());
//            if (!newNodes.contains(newNodeId)) {
//                newNodes.add(newNodeId);
//            }
//
//            ConfigurationEntry jointConfig = new ConfigurationEntry(oldNodes, newNodes, true);
//
//            long nextIndex = persistent.getLogStore().lastIndex() + 1;
//            LogEntry jointEntry = LogEntry.newConfigurationEntry(nextIndex, persistent.getCurrentTerm(), jointConfig);
//            persistent.getLogStore().append(jointEntry);
//
//            this.conf.setOldNodes(newNodes);
//
//            if (!leaderState.getNextIndex().containsKey(newNodeId)) {
//                leaderState.getNextIndex().put(newNodeId, nextIndex);
//                leaderState.getMatchIndex().put(newNodeId, 0L);
//                replicateLogToNewNode(newNodeId);
//            }
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    private void replicateLogToNewNode(String nodeId) {
//        lock.lock();
//        try {
//            if (state != NodeState.LEADER) {
//                return;
//            }
//            long nextIdx = leaderState.getNextIndex().get(nodeId);
//            List<LogEntry> entries = persistent.getLogStore().readFrom(nextIdx);
//            long prevIndex = nextIdx - 1;
//            long prevTerm = prevIndex == 0 ? 0 : persistent.getLogStore().get(prevIndex).getTerm();
//
//            AppendEntriesRequest req = new AppendEntriesRequest(
//                    persistent.getCurrentTerm(), this.nodeId, prevIndex, prevTerm, entries, volatileState.getCommitIndex()
//            );
//
//            rpc.appendEntries(nodeId, req).thenAccept(resp -> {
//                lock.lock();
//                try {
//                    if (resp.term > persistent.getCurrentTerm()) {
//                        becomeFollower(resp.term);
//                        return;
//                    }
//                    if (resp.success) {
//                        leaderState.getMatchIndex().put(nodeId, prevIndex + entries.size());
//                        leaderState.getNextIndex().put(nodeId, prevIndex + entries.size() + 1);
//                        maybeAdvanceCommitIndex();
//                        checkPromoteNode(nodeId);
//                    } else {
//                        long ni = leaderState.getNextIndex().get(nodeId);
//                        if (ni > 1) leaderState.getNextIndex().put(nodeId, ni - 1);
//                        replicateLogToNewNode(nodeId); // retry
//                    }
//                } finally {
//                    lock.unlock();
//                }
//            });
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    private void checkPromoteNode(String newNodeId) {
//        lock.lock();
//        try {
//            long matchIndex = leaderState.getMatchIndex().getOrDefault(newNodeId, 0L);
//            long lastIndex = persistent.getLogStore().lastIndex();
//
//            if (matchIndex >= lastIndex) {
//                // Phase 2: final configuration (joint=false)
//                List<String> oldNodes = new ArrayList<>(conf.getOldNodes());
//                List<String> newNodes = new ArrayList<>();
//
//                ConfigurationEntry finalConf = new ConfigurationEntry(oldNodes, newNodes, false);
//                long idx = lastIndex + 1;
//                LogEntry finalEntry = LogEntry.newConfigurationEntry(idx, persistent.getCurrentTerm(), finalConf);
//
//                persistent.getLogStore().append(finalEntry);
//
//            }
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    private void recoverStateMachineFromSnapshot() {
//        lock.lock();
//        try {
//            try {
//                Snapshot snapshot = persistent.loadSnapshot();
//                if (snapshot != null) {
//                    stateMachine.onSnapshotLoad(snapshot.getState());
//                    volatileState.setLastApplied(snapshot.getLastIncludedIndex());
//                    volatileState.setCommitIndex(snapshot.getLastIncludedIndex());
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//            // Apply các log sau snapshot
//            long lastApplied = volatileState.getLastApplied();
//            long commitIndex = persistent.getLastCommitIndex();
//            LogStore log = persistent.getLogStore();
//
//            for (long i = lastApplied + 1; i <= commitIndex; i++) {
//                LogEntry logEntry = log.get(i);
//                if (logEntry != null) {
//                    stateMachine.onApply(nodeId, logEntry);
//                }
//                volatileState.setLastApplied(i);
//                volatileState.setCommitIndex(i);
//            }
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    public void createSnapshot() {
//        lock.lock();
//        try {
//            long commitIndex = volatileState.getCommitIndex();
//            LogStore log = persistent.getLogStore();
//
//            // Serialize state machine
//            byte[] state = stateMachine.onSnapshotSave(); // stateMachine cần có serialize()
//
//            Snapshot snapshot = new Snapshot();
//            snapshot.setLastIncludedIndex(commitIndex);
//            snapshot.setLastIncludedTerm(log.get(commitIndex).getTerm());
//            snapshot.setState(state);
//
//            try {
//                persistent.saveSnapshot(snapshot);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            // Truncate log trước snapshot
//            log.truncatePrefix(commitIndex + 1);
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    @Override
//    public PreVoteResponse handlePreVoteRequest(PreVoteRequest request) {
//        lock.lock();
//        try {
//            if (!conf.getOldNodes().contains(request.candidateId)) {
//                log.warn("Node {} ignore PreVoteRequest from {} as it is not in conf <{}>.", getNodeId(), request.candidateId, this.conf);
//                return new PreVoteResponse(persistent.getCurrentTerm(), false);
//            }
//            if (request.term < persistent.getCurrentTerm()) {
//                return new PreVoteResponse(persistent.getCurrentTerm(), false);
//            }
//
//            long lastLogIndex = persistent.getLogStore().lastIndex();
//            long lastLogTerm = persistent.getLogStore().lastTerm();
//
//            boolean upToDate = (request.lastLogTerm > lastLogTerm) ||
//                    (request.lastLogTerm == lastLogTerm && request.lastLogIndex >= lastLogIndex);
//
//            return new PreVoteResponse(persistent.getCurrentTerm(), upToDate);
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    @Override
//    public RequestVoteResponse handleRequestVoteRequest(RequestVoteRequest req) {
//        lock.lock();
//        try {
//            if (!conf.getOldNodes().contains(req.candidateId)) {
//                return new RequestVoteResponse(persistent.getCurrentTerm(), false);
//            }
//            if (req.term < persistent.getCurrentTerm()) {
//                return new RequestVoteResponse(persistent.getCurrentTerm(), false);
//            }
//            if (req.term > persistent.getCurrentTerm()) {
//                becomeFollower(req.term);
//            }
//
//            boolean voteGranted = false;
//            String votedFor = persistent.getVotedFor();
//            long lastLogIndex = persistent.getLogStore().lastIndex();
//            long lastLogTerm = persistent.getLogStore().lastTerm();
//
//            boolean upToDate = (req.lastLogTerm > lastLogTerm) || (req.lastLogTerm == lastLogTerm && req.lastLogIndex >= lastLogIndex);
//
//            if ((votedFor == null || votedFor.equals(req.candidateId)) && upToDate) {
//                persistent.setVotedFor(req.candidateId);
//                voteGranted = true;
//                electionTimer.reset();
//            }
//            return new RequestVoteResponse(persistent.getCurrentTerm(), voteGranted);
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    @Override
//    public AppendEntriesResponse handleAppendEntriesRequest(AppendEntriesRequest req) {
//        lock.lock();
//        try {
//            if (req.term < persistent.getCurrentTerm()) {
//                return new AppendEntriesResponse(persistent.getCurrentTerm(), false, req.prevLogIndex - 1);
//            }
//
//            onHeartbeatReceived(req.term, req.leaderId);
//
//            LogStore log = persistent.getLogStore();
//            if (req.prevLogIndex > 0) {
//                LogEntry prev = log.get(req.prevLogIndex);
//                if (prev == null || prev.getTerm() != req.prevLogTerm) {
//                    return new AppendEntriesResponse(persistent.getCurrentTerm(), false, req.prevLogIndex - 1);
//                }
//            }
//
//            if (req.entries != null && !req.entries.isEmpty()) {
//                long start = req.entries.get(0).getIndex();
//                log.truncateSuffix(start);
//                for (LogEntry e : req.entries) {
//                    log.append(e);
//                    if (e.isConfigurationEntry()) {
//                        ConfigurationEntry newConf = e.getConfiguration();
//                        if (newConf.isJoint()) {
//                            conf.setOldNodes(newConf.getOldNodes());
//                            conf.setNewNodes(newConf.getNewNodes());
//                            conf.setJoint(true);
//                        } else {
//                            conf.setOldNodes(newConf.getOldNodes());
//                            conf.setNewNodes(newConf.getNewNodes());
//                            conf.setJoint(false);
//                        }
//                    }
//                }
//            }
//            if (req.leaderCommit > volatileState.getCommitIndex()) {
//                long newCommit = Math.min(req.leaderCommit, log.lastIndex());
//                volatileState.setCommitIndex(newCommit);
//                applyCommitted();
//            }
//
//            return new AppendEntriesResponse(persistent.getCurrentTerm(), true, log.lastIndex());
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    private void onHeartbeatReceived(long term, String leaderId) {
//        lock.lock();
//        try {
//            if (term < persistent.getCurrentTerm()) {
//                return;
//            }
//            if (state == NodeState.CANDIDATE) {
//                if (term >= persistent.getCurrentTerm()) {
//                    becomeFollower(term); // luôn step-down khi nhận leader heartbeat
//                }
//            }
//            if (term > persistent.getCurrentTerm()) {
//                if (state == NodeState.LEADER) {
//                    System.out.println("Leader " + nodeId + " step down");
//                }
//                becomeFollower(term);
//            }
//
//            this.leaderId = leaderId;
//            electionTimer.reset();
//        } finally {
//            lock.unlock();
//        }
//    }
//
//
//    private void onElectionTimeout() {
//        lock.lock();
//        try {
//            if (state == NodeState.LEADER) {
//                return;
//            }
//            AtomicInteger granted = new AtomicInteger(1); // vote cho chính mình
//            int majority = (conf.getOldNodes().size() / 2) + 1;
//            long lastIndex = persistent.getLogStore().lastIndex();
//            long lastTerm = persistent.getLogStore().lastTerm();
//
//            for (String peer : conf.getOldNodes()) {
//                if (peer.equals(nodeId)) continue;
//                PreVoteRequest req = new PreVoteRequest(persistent.getCurrentTerm(), nodeId, lastIndex, lastTerm);
//                rpc.preVote(peer, req).thenAccept(response -> {
//                    lock.lock();
//                    try {
//                        if (state != NodeState.FOLLOWER && state != NodeState.CANDIDATE) {
//                            return;
//                        }
//
//                        if (response.term > persistent.getCurrentTerm()) {
//                            becomeFollower(response.term);
//                            return;
//                        }
//
//                        log.info("Node {} received PreVoteResponse from {}, term={}, granted={}.", getNodeId(), peer, response.term, response.voteGranted);
//
//                        if (response.voteGranted) {
//                            int g = granted.incrementAndGet();
//                            if (g >= majority && state == NodeState.FOLLOWER) {
//                                becomeCandidate(); // chỉ bầu cử nếu majority pre-vote thành công
//                            }
//                        }
//                    } finally {
//                        lock.unlock();
//                    }
//                });
//            }
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    private void becomeFollower(long term) {
//        boolean changedTerm = false;
//        // Cập nhật term nếu term mới lớn hơn
//        if (term > persistent.getCurrentTerm()) {
//            persistent.setCurrentTerm(term);
//            persistent.setVotedFor(null);
//            persistent.persist();
//            changedTerm = true;
//        }
//        System.out.println(nodeId + " current state " + state + " becomes FOLLOWER at term " + persistent.getCurrentTerm());
//        // Dù term bằng currentTerm, nếu node không phải follower thì vẫn step-down
//        if (state != NodeState.FOLLOWER || changedTerm) {
//
//            state = NodeState.FOLLOWER;
//            leaderState = null;
//            leaderId = null;
//            heartbeatTimer.stop();
//
//            // Learner node sẽ không trigger election timer
//            electionTimer.reset();
//
//            // Hủy tất cả pending client futures
//            for (CompletableFuture<Boolean> future : pendingFutures.values()) {
//                future.complete(false);
//            }
//            pendingFutures.clear();
//        }
//    }
//
//    private void becomeCandidate() {
//        if (state == NodeState.LEADER) {
//            return; // tránh race khi heartbeat tới đúng lúc
//        }
//
//        state = NodeState.CANDIDATE;
//        persistent.setCurrentTerm(persistent.getCurrentTerm() + 1);
//        persistent.setVotedFor(nodeId);
//        electionTimer.reset();
//
//        final long termStarted = persistent.getCurrentTerm();
//        AtomicInteger granted = new AtomicInteger(1);
//        int majority = (conf.getOldNodes().size() / 2) + 1;
//
//        long lastIndex = persistent.getLogStore().lastIndex();
//        long lastTerm = persistent.getLogStore().lastTerm();
//
//        for (String peer : conf.getOldNodes()) {
//            if (peer.equals(nodeId)) {
//                continue;
//            }
//            RequestVoteRequest req = new RequestVoteRequest(termStarted, nodeId, lastIndex, lastTerm);
//            rpc.requestVote(peer, req).thenAccept(resp -> {
////                System.out.println(nodeId + " current state " + state + " request vote to " + peer + " -> " + (granted.incrementAndGet() >= majority));
//
//                lock.lock();
//                try {
//                    if (state != NodeState.CANDIDATE) return;
//                    if (resp.term > persistent.getCurrentTerm()) {
//                        becomeFollower(resp.term);
//                        return;
//                    }
//                    if (persistent.getCurrentTerm() != termStarted) {
//                        return;
//                    }
//                    if (resp.voteGranted) {
//                        int g = granted.incrementAndGet();
//                        if (g >= majority && state == NodeState.CANDIDATE) {
//                            becomeLeader();
//                        }
//                    }
//                } finally {
//                    lock.unlock();
//                }
//            });
//        }
//    }
//
//    private void becomeLeader() {
//        state = NodeState.LEADER;
//        leaderState = new LeaderState(conf.getOldNodes(), persistent.getLogStore().lastIndex() + 1);
//        leaderId = nodeId;
//        heartbeatTimer.start();
//        sendHeartbeats();
//        System.out.println("Leader " + nodeId + " elected at term " + persistent.getCurrentTerm());
//    }
//
//    private void sendHeartbeats() {
//        lock.lock();
//        try {
//            if (state != NodeState.LEADER) {
//                return;
//            }
//
//            long term = persistent.getCurrentTerm();
//            long leaderCommit = volatileState.getCommitIndex();
//
//            for (String peer : conf.getOldNodes()) {
//                if (peer.equals(nodeId)) {
//                    continue;
//                }
//                replicateLogToFollower(peer, term, leaderCommit);
//            }
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    private void replicateLogToFollower(String followerId, long term, long leaderCommit) {
//        lock.lock();
//        try {
//            if (state != NodeState.LEADER) {
//                return;
//            }
//
//            LogStore log = persistent.getLogStore();
//            long nextIdx = leaderState.getNextIndex().getOrDefault(followerId, 1L);
//            long lastIndex = log.lastIndex();
//            if (nextIdx > lastIndex + 1) nextIdx = lastIndex + 1;
//
//            long prevIndex = nextIdx - 1;
//            long prevTerm = prevIndex == 0 ? 0 : (log.get(prevIndex) != null ? log.get(prevIndex).getTerm() : 0);
//
//            // Chỉ replicate batch logs, hoặc heartbeat rỗng nếu không có log mới
//            List<LogEntry> entries = nextIdx <= lastIndex ? log.readFrom(nextIdx) : List.of();
//
//            AppendEntriesRequest req = new AppendEntriesRequest(term, nodeId, prevIndex, prevTerm, entries, leaderCommit);
//
//            rpc.appendEntries(followerId, req).thenAccept(resp -> {
//                lock.lock();
//                try {
//                    if (resp.term > persistent.getCurrentTerm()) {
//                        becomeFollower(resp.term);
//                        return;
//                    }
//
//                    if (resp.success) {
//                        if (!entries.isEmpty()) {
//                            long match = prevIndex + entries.size();
//                            long prevMatch = leaderState.getMatchIndex().getOrDefault(followerId, 0L);
//                            leaderState.getMatchIndex().put(followerId, Math.max(prevMatch, match));
//                            leaderState.getNextIndex().put(followerId, match + 1);
//                            maybeAdvanceCommitIndex();
//                        }
//
//                    } else {
//                        // Giảm nextIndex an toàn, tránh giảm quá mức
//                        long ni = leaderState.getNextIndex().getOrDefault(followerId, 1L);
//                        long decrement = Math.max(1, (ni - 1) / 2);
//                        leaderState.getNextIndex().put(followerId, Math.max(1, ni - decrement));
//                        if (state == NodeState.LEADER) {
//                            replicateLogToFollower(followerId, term, leaderCommit);
//                        }
//                    }
//                } finally {
//                    lock.unlock();
//                }
//            });
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    private void maybeAdvanceCommitIndex() {
//        long N = persistent.getLogStore().lastIndex();
//        long currentCommit = volatileState.getCommitIndex();
//        for (long k = currentCommit + 1; k <= N; k++) {
//            final long candidate = k;
//            int count = 1;
//            for (String peer : conf.getOldNodes()) {
//                if (peer.equals(nodeId)) continue;
//                Long m = leaderState.getMatchIndex().get(peer);
//                if (m != null && m >= candidate) count++;
//            }
//            int majority = (conf.getOldNodes().size() / 2) + 1;
//            if (count >= majority) {
//                LogEntry e = persistent.getLogStore().get(candidate);
//                if (e != null && e.getTerm() == persistent.getCurrentTerm()) {
//                    volatileState.setCommitIndex(candidate);
//                    applyCommitted();
//                }
//            }
//        }
//    }
//
//    private void applyCommitted() {
//        long lastApplied = volatileState.getLastApplied();
//        long commitIndex = volatileState.getCommitIndex();
//        LogStore log = persistent.getLogStore();
//
//        for (long idx = lastApplied + 1; idx <= commitIndex; idx++) {
//            LogEntry logEntry = log.get(idx);
//            if (logEntry != null) {
//                stateMachine.onApply(nodeId, logEntry);
//            }
//            volatileState.setLastApplied(idx);
//
//            CompletableFuture<Boolean> future = pendingFutures.remove(idx);
//            if (future != null) {
//                future.complete(true);
//            }
//        }
//
//        // persist commitIndex
//        persistent.setLastCommitIndex(commitIndex);
//    }
//
//    public CompletableFuture<Boolean> clientAppend(byte[] command) {
//        lock.lock();
//        try {
//            if (state != NodeState.LEADER) {
//                return CompletableFuture.completedFuture(false);
//            }
//            long nextIndex = persistent.getLogStore().lastIndex() + 1;
//            LogEntry e = new LogEntry(nextIndex, persistent.getCurrentTerm(), command);
//            persistent.getLogStore().append(e);
//
//            CompletableFuture<Boolean> future = new CompletableFuture<>();
//            pendingFutures.put(nextIndex, future);
//            scheduler.schedule(() -> {
//                lock.lock();
//                try {
//                    CompletableFuture<Boolean> f = pendingFutures.remove(nextIndex);
//                    if (f != null && !f.isDone()) {
//                        f.complete(false); // timeout → fail client
//                    }
//                } finally {
//                    lock.unlock();
//                }
//            }, CLIENT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
//
//            // replicate log đến followers
//            sendHeartbeats();
//
//            return future;
//        } finally {
//            lock.unlock();
//        }
//    }
//}
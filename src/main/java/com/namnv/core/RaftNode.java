package com.namnv.core;

import com.namnv.config.NodeOptions;
import com.namnv.config.RaftConfig;
import com.namnv.entity.LogEntry;
import com.namnv.rpc.*;
import com.namnv.rpc.client.InMemoryRpcClient;
import com.namnv.rpc.client.RpcProcessor;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.InstallSnapshotRequest;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.RequestVoteRequest;
import com.namnv.rpc.model.response.AppendEntriesResponse;
import com.namnv.rpc.model.response.InstallSnapshotResponse;
import com.namnv.rpc.model.response.PreVoteResponse;
import com.namnv.rpc.model.response.RequestVoteResponse;
import com.namnv.rpc.server.SocketRpcServer;
import com.namnv.state.LeaderState;
import com.namnv.state.PersistentState;
import com.namnv.state.VolatileState;
import com.namnv.statemachine.StateMachine;
import com.namnv.entity.ConfigurationEntry;
import com.namnv.statemachine.snapshot.SnapshotWriter;
import com.namnv.timer.ElectionTimer;
import com.namnv.timer.HeartbeatTimer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.isNull;

@Slf4j
@Getter
public class RaftNode implements RaftServerService {

    public enum NodeState {
        LEADER,
        CANDIDATE,
        FOLLOWER,
        LEARNER,
    }

    private final RaftConfig config;

    private final RpcProcessor rpcProcessor;
    private final SocketRpcServer rpcServer;

    private final PersistentState persistent;
    private final VolatileState volatileState = new VolatileState();

    private final String nodeId;
    private volatile NodeState state;

    private String leaderId;
    private LeaderState leaderState;

    private final ElectionTimer electionTimer;
    private final HeartbeatTimer heartbeatTimer;

    private final StateMachine stateMachine;

    private final Lock lock = new ReentrantLock();

    private final ConfigurationEntry conf;

    private final Map<Long, CompletableFuture<Boolean>> pendingFutures = new HashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final long CLIENT_TIMEOUT_MS = 5000;

    private final NodeOptions nodeOptions;

    public RaftNode(NodeOptions nodeOptions, RpcProcessor rpcProcessor) {
        this.rpcServer = new SocketRpcServer(Integer.parseInt(nodeOptions.getRaftConfig().getSelf().split(":")[1]), this);
        this.rpcProcessor = rpcProcessor;

        this.nodeId = nodeOptions.getRaftConfig().getSelf();
        this.nodeOptions = nodeOptions;
        this.config = nodeOptions.getRaftConfig();
        this.persistent = new PersistentState(nodeOptions);
        this.stateMachine = nodeOptions.getStateMachine();
        this.electionTimer = new ElectionTimer(nodeOptions.getElectionTimeoutMinMs(), nodeOptions.getElectionTimeoutMaxMs(), this::onElectionTimeout);
        this.heartbeatTimer = new HeartbeatTimer(nodeOptions.getHeartbeatIntervalMs(), this::sendHeartbeats);
        this.conf = new ConfigurationEntry(nodeOptions.getRaftConfig().getPeers(), new ArrayList<>(), false);
    }

    public void start() {
        if (rpcProcessor instanceof InMemoryRpcClient) {
            ((InMemoryRpcClient) rpcProcessor).register(nodeId, this);
        }
        rpcServer.start();
        restoreStateMachineFromSnapshot();
        this.state = NodeState.FOLLOWER;
        electionTimer.start();
    }

    public void onJoinPeerCluster(String newNodeId) {
        lock.lock();
        try {
            if (state != RaftNode.NodeState.LEADER || leaderState == null) {
                return;
            }
            // Phase 1: joint configuration (old + new nodes)
            var newNodes = new ArrayList<>(conf.getOldNodes());
            newNodes.add(newNodeId);
            this.conf.setNewNodes(newNodes);

            var jointConfig = new ConfigurationEntry(conf.getOldNodes(), conf.getNewNodes(), true);

            var nextIndex = persistent.getLogStore().lastIndex() + 1;
            var jointEntry = LogEntry.newConfigurationEntry(nextIndex, persistent.getCurrentTerm(), jointConfig);
            persistent.getLogStore().appendEntry(jointEntry);

            if (!leaderState.getNextIndex().containsKey(newNodeId)) {
                leaderState.getNextIndex().put(newNodeId, nextIndex);
                leaderState.getMatchIndex().put(newNodeId, 0L);
                replicateLogsForJoiningNode(newNodeId);
            }
        } finally {
            lock.unlock();
        }
    }

    private void replicateLogsForJoiningNode(String nodeId) {
        lock.lock();
        try {
            if (state != NodeState.LEADER) {
                return;
            }
            var nextIdx = leaderState.getNextIndex().get(nodeId);
            var entries = persistent.getLogStore().readFrom(nextIdx);
            var prevIndex = nextIdx - 1;
            if (isNull(persistent.getLogStore().get(prevIndex))) {
                sendSnapshotToNode(nodeId);
                return;
            }
            var prevTerm = prevIndex == 0 ? 0 : persistent.getLogStore().get(prevIndex).getTerm();

            var req = new AppendEntriesRequest(
                    persistent.getCurrentTerm(), this.nodeId, prevIndex, prevTerm, entries, volatileState.getCommitIndex()
            );

            rpcProcessor.appendEntries(nodeId, req).thenAccept(resp -> {
                lock.lock();
                try {
                    if (resp.term > persistent.getCurrentTerm()) {
                        becomeFollower(resp.term);
                        return;
                    }
                    if (resp.success) {
                        leaderState.getMatchIndex().put(nodeId, prevIndex + entries.size());
                        leaderState.getNextIndex().put(nodeId, prevIndex + entries.size() + 1);
                        maybeAdvanceCommitIndex();
                        finalizeJoiningNodeIfCaughtUp(nodeId);
                    } else {
                        long ni = leaderState.getNextIndex().get(nodeId);
                        if (ni > 1) {
                            leaderState.getNextIndex().put(nodeId, ni - 1);
                        }
                        replicateLogsForJoiningNode(nodeId); // retry
                    }
                } finally {
                    lock.unlock();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    private void sendSnapshotToNode(String nodeId) {
        lock.lock();
        try {
            var req = new InstallSnapshotRequest(
                    persistent.getCurrentTerm(),
                    this.nodeId,
                    persistent.getLogStore().getBaseIndex(),
                    persistent.getLogStore().getBaseTerm(),
                    nodeOptions.getSnapshotUri() + "/snapshot.data"
            );

            rpcProcessor.installSnapshot(nodeId, req).thenAccept(resp -> {
                lock.lock();
                try {
                    if (resp.getTerm() > persistent.getCurrentTerm()) {
                        becomeFollower(resp.getTerm());
                        return;
                    }
                    // Sau khi snapshot áp dụng xong → cập nhật nextIndex và matchIndex
                    leaderState.getNextIndex().put(nodeId, persistent.getLogStore().getBaseIndex() + 1);
                    leaderState.getMatchIndex().put(nodeId, persistent.getLogStore().getBaseIndex());

                    finalizeJoiningNodeIfCaughtUp(nodeId);

                    sendLogs(nodeId, persistent.getLogStore().getBaseIndex() + 1, persistent.getLogStore().getBaseIndex(), persistent.getLogStore().getBaseTerm());

                } finally {
                    lock.unlock();
                }
            });
        } finally {
            lock.unlock();
        }
    }

    private void sendLogs(String nodeId, long nextIdx, long prevIndex, long prevTerm) {
        lock.lock();
        try {
            var entries = persistent.getLogStore().readFrom(nextIdx);
            var req = new AppendEntriesRequest(persistent.getCurrentTerm(), this.nodeId, prevIndex, prevTerm, entries, volatileState.getCommitIndex());
            rpcProcessor.appendEntries(nodeId, req).thenAccept(resp -> {
                lock.lock();
                try {
                    if (resp.term > persistent.getCurrentTerm()) {
                        becomeFollower(resp.term);
                        return;
                    }
                    if (resp.success) {
                        leaderState.getMatchIndex().put(nodeId, prevIndex + entries.size());
                        leaderState.getNextIndex().put(nodeId, prevIndex + entries.size() + 1);
                        maybeAdvanceCommitIndex();
                        finalizeJoiningNodeIfCaughtUp(nodeId);
                    } else {
                        throw new RuntimeException();
                    }
                } finally {
                    lock.unlock();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    private void finalizeJoiningNodeIfCaughtUp(String newNodeId) {
        lock.lock();
        try {
            var matchIndex = leaderState.getMatchIndex().getOrDefault(newNodeId, 0L);
            var lastIndex = persistent.getLogStore().lastIndex();

            if (matchIndex >= lastIndex) {
                // Phase 2: final configuration (joint=false)
                this.conf.setOldNodes(conf.getNewNodes());
                this.conf.setNewNodes(new ArrayList<>());

                var finalConf = new ConfigurationEntry(this.conf.getOldNodes(), this.conf.getNewNodes(), false);
                var idx = lastIndex + 1;
                var finalEntry = LogEntry.newConfigurationEntry(idx, persistent.getCurrentTerm(), finalConf);

                persistent.getLogStore().appendEntry(finalEntry);
            }
        } finally {
            lock.unlock();
        }
    }

    private void restoreStateMachineFromSnapshot() {
        lock.lock();
        try {
            try {
                if (persistent.getLastSnapshotIndex() > -1) {
                    volatileState.setLastApplied(persistent.getLastSnapshotIndex());
                    volatileState.setCommitIndex(persistent.getLastSnapshotIndex());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            // Apply các log sau snapshot
            var lastApplied = volatileState.getLastApplied();
            var commitIndex = persistent.getLastCommitIndex();
            var log = persistent.getLogStore();

            for (var i = lastApplied + 1; i <= commitIndex; i++) {
                LogEntry logEntry = log.get(i);
                if (logEntry != null) {
                    stateMachine.onApply(nodeId, logEntry);
                }
                volatileState.setLastApplied(i);
                volatileState.setCommitIndex(i);
            }
        } finally {
            lock.unlock();
        }
    }

    public void createSnapshot() {
        lock.lock();
        try {
            var commitIndex = volatileState.getCommitIndex();
            var logStore = persistent.getLogStore();
            stateMachine.onSnapshotSave(new SnapshotWriter(nodeOptions.getSnapshotUri(), new ArrayList<>()), status -> {
                if (!status.isOk()) {
                    log.error("Snapshot failed: {}", status);
                    return;
                }
                persistent.setLastSnapshotTerm(persistent.getCurrentTerm());
                persistent.setLastSnapshotIndex(commitIndex);
                persistent.persist();
                logStore.truncatePrefix(commitIndex + 1);
            });
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PreVoteResponse handlePreVoteRequest(PreVoteRequest request) {
        lock.lock();
        try {
            if (!conf.getOldNodes().contains(request.candidateId)) {
                log.warn("Node {} ignore PreVoteRequest from {} as it is not in conf <{}>.", getNodeId(), request.candidateId, this.conf);
                return new PreVoteResponse(persistent.getCurrentTerm(), false);
            }
            if (request.term < persistent.getCurrentTerm()) {
                return new PreVoteResponse(persistent.getCurrentTerm(), false);
            }

            var lastLogIndex = persistent.getLogStore().lastIndex();
            var lastLogTerm = persistent.getLogStore().lastTerm();

            var upToDate = (request.lastLogTerm > lastLogTerm) ||
                    (request.lastLogTerm == lastLogTerm && request.lastLogIndex >= lastLogIndex);

            return new PreVoteResponse(persistent.getCurrentTerm(), upToDate);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public InstallSnapshotResponse handleInstallSnapshotRequest(InstallSnapshotRequest req) {
        lock.lock();
        try {
            if (req.getLastIncludedTerm() < persistent.getCurrentTerm()) {
                return new InstallSnapshotResponse(persistent.getCurrentTerm());
            }
            volatileState.setCommitIndex(Math.max(volatileState.getCommitIndex(), req.getLastIncludedIndex()));
            volatileState.setLastApplied(req.getLastIncludedIndex());

            persistent.getLogStore().setBaseTerm(req.getLastIncludedTerm());
            persistent.getLogStore().setBaseIndex(req.getLastIncludedIndex());
            return new InstallSnapshotResponse(persistent.getCurrentTerm());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public RequestVoteResponse handleRequestVoteRequest(RequestVoteRequest req) {
        lock.lock();
        try {
            if (!conf.getOldNodes().contains(req.candidateId)) {
                return new RequestVoteResponse(persistent.getCurrentTerm(), false);
            }
            if (req.term < persistent.getCurrentTerm()) {
                return new RequestVoteResponse(persistent.getCurrentTerm(), false);
            }
            if (req.term > persistent.getCurrentTerm()) {
                becomeFollower(req.term);
            }

            var voteGranted = false;
            var votedFor = persistent.getVotedFor();
            var lastLogIndex = persistent.getLogStore().lastIndex();
            var lastLogTerm = persistent.getLogStore().lastTerm();

            var upToDate = (req.lastLogTerm > lastLogTerm) || (req.lastLogTerm == lastLogTerm && req.lastLogIndex >= lastLogIndex);

            if ((votedFor == null || votedFor.equals(req.candidateId)) && upToDate) {
                persistent.setVotedFor(req.candidateId);
                voteGranted = true;
                electionTimer.reset();
            }
            return new RequestVoteResponse(persistent.getCurrentTerm(), voteGranted);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public AppendEntriesResponse handleAppendEntriesRequest(AppendEntriesRequest req) {
        lock.lock();
        try {
            if (req.term < persistent.getCurrentTerm()) {
                return new AppendEntriesResponse(persistent.getCurrentTerm(), false, req.prevLogIndex - 1);
            }

            handleHeartbeat(req.term, req.leaderId);

            var log = persistent.getLogStore();
            if (req.prevLogIndex > 0) {
                LogEntry prev = log.get(req.prevLogIndex);
                if (prev != null) {
                    if (prev.getTerm() != req.prevLogTerm) {
                        return new AppendEntriesResponse(persistent.getCurrentTerm(), false, req.prevLogIndex - 1);
                    }
                } else {
                    // prev is not in local logs -> maybe truncated by snapshot
                    long baseIndex = log.getBaseIndex();
                    long baseTerm = log.getBaseTerm();

                    if (baseIndex == 0) {
                        return new AppendEntriesResponse(persistent.getCurrentTerm(), false, req.prevLogIndex - 1);
                    } else if (req.prevLogIndex == baseIndex) {
                        if (req.prevLogTerm != baseTerm) {
                            return new AppendEntriesResponse(persistent.getCurrentTerm(), false, req.prevLogIndex - 1);
                        }
                    } else {
                        // requested prevIndex < baseIndex OR > lastIndex -> cannot accept, ask leader to send snapshot
                        return new AppendEntriesResponse(persistent.getCurrentTerm(), false, baseIndex);
                    }
                }
            }

            if (req.entries != null && !req.entries.isEmpty()) {
                long start = req.entries.get(0).getIndex();
                log.truncateSuffix(start);
                for (LogEntry e : req.entries) {
                    log.appendEntry(e);
                    if (e.isConfigurationEntry()) {
                        ConfigurationEntry newConf = e.getConfiguration();
                        if (newConf.isJoint()) {
                            conf.setOldNodes(newConf.getOldNodes());
                            conf.setNewNodes(newConf.getNewNodes());
                            conf.setJoint(true);
                        } else {
                            conf.setOldNodes(newConf.getOldNodes());
                            conf.setNewNodes(newConf.getNewNodes());
                            conf.setJoint(false);
                        }
                    }
                }
            }
            if (req.leaderCommit > volatileState.getCommitIndex()) {
                long newCommit = Math.min(req.leaderCommit, log.lastIndex());
                volatileState.setCommitIndex(newCommit);
                applyCommitted();
            }
            return new AppendEntriesResponse(persistent.getCurrentTerm(), true, log.lastIndex());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        return null;
    }

    private void handleHeartbeat(long term, String leaderId) {
        lock.lock();
        try {
            if (term < persistent.getCurrentTerm()) {
                return;
            }
            if (state == NodeState.CANDIDATE) {
                if (term >= persistent.getCurrentTerm()) {
                    becomeFollower(term); // luôn step-down khi nhận leader heartbeat
                }
            }
            if (term > persistent.getCurrentTerm()) {
                if (state == NodeState.LEADER) {
                    log.info("Leader " + nodeId + " step down");
                }
                becomeFollower(term);
            }
            this.leaderId = leaderId;
            electionTimer.reset();
        } finally {
            lock.unlock();
        }
    }

    private void onElectionTimeout() {
        lock.lock();
        try {
            if (state == NodeState.LEADER) {
                return;
            }
            var granted = new AtomicInteger(1); // vote cho chính mình
            var majority = (conf.getOldNodes().size() / 2) + 1;
            var lastIndex = persistent.getLogStore().lastIndex();
            var lastTerm = persistent.getLogStore().lastTerm();

            for (var peer : conf.getOldNodes()) {
                if (peer.equals(nodeId)) {
                    continue;
                }
                var req = new PreVoteRequest(persistent.getCurrentTerm(), nodeId, lastIndex, lastTerm);
                rpcProcessor.preVote(peer, req).thenAccept(response -> {
                    lock.lock();
                    try {
                        if (state != NodeState.FOLLOWER && state != NodeState.CANDIDATE) {
                            return;
                        }

                        if (response.term > persistent.getCurrentTerm()) {
                            becomeFollower(response.term);
                            return;
                        }

                        log.info("Node {} received PreVoteResponse from {}, term={}, granted={}.", getNodeId(), peer, response.term, response.voteGranted);

                        if (response.voteGranted) {
                            int g = granted.incrementAndGet();
                            if (g >= majority && state == NodeState.FOLLOWER) {
                                becomeCandidate(); // chỉ bầu cử nếu majority pre-vote thành công
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                });
            }
        } finally {
            lock.unlock();
        }
    }

    private void becomeFollower(long term) {
        var changedTerm = false;
        // Cập nhật term nếu term mới lớn hơn
        if (term > persistent.getCurrentTerm()) {
            persistent.setCurrentTerm(term);
            persistent.setVotedFor(null);
            persistent.persist();
            changedTerm = true;
        }
        log.info(nodeId + " current state " + state + " becomes FOLLOWER at term " + persistent.getCurrentTerm());
        // Dù term bằng currentTerm, nếu node không phải follower thì vẫn step-down
        if (state != NodeState.FOLLOWER || changedTerm) {
            state = NodeState.FOLLOWER;
            leaderState = null;
            leaderId = null;
            heartbeatTimer.stop();
            // Learner node sẽ không trigger election timer
            electionTimer.reset();
            // Hủy tất cả pending client futures
            for (CompletableFuture<Boolean> future : pendingFutures.values()) {
                future.complete(false);
            }
            pendingFutures.clear();
        }
    }

    private void becomeCandidate() {
        if (state == NodeState.LEADER) {
            return; // tránh race khi heartbeat tới đúng lúc
        }
        state = NodeState.CANDIDATE;
        persistent.setCurrentTerm(persistent.getCurrentTerm() + 1);
        persistent.setVotedFor(nodeId);
        electionTimer.reset();

        var termStarted = persistent.getCurrentTerm();
        var granted = new AtomicInteger(1);
        var majority = (conf.getOldNodes().size() / 2) + 1;

        var lastIndex = persistent.getLogStore().lastIndex();
        var lastTerm = persistent.getLogStore().lastTerm();

        for (String peer : conf.getOldNodes()) {
            if (peer.equals(nodeId)) {
                continue;
            }
            RequestVoteRequest req = new RequestVoteRequest(termStarted, nodeId, lastIndex, lastTerm);
            rpcProcessor.requestVote(peer, req).thenAccept(resp -> {
                lock.lock();
                try {
                    if (state != NodeState.CANDIDATE) return;
                    if (resp.term > persistent.getCurrentTerm()) {
                        becomeFollower(resp.term);
                        return;
                    }
                    if (persistent.getCurrentTerm() != termStarted) {
                        return;
                    }
                    if (resp.voteGranted) {
                        int g = granted.incrementAndGet();
                        if (g >= majority && state == NodeState.CANDIDATE) {
                            becomeLeader();
                        }
                    }
                } finally {
                    lock.unlock();
                }
            });
        }
    }

    private void becomeLeader() {
        state = NodeState.LEADER;
        leaderState = new LeaderState(conf.getOldNodes(), persistent.getLogStore().lastIndex() + 1);
        leaderId = nodeId;
        heartbeatTimer.start();
        sendHeartbeats();
        log.info("Leader " + nodeId + " elected at term " + persistent.getCurrentTerm());
    }

    private void sendHeartbeats() {
        lock.lock();
        try {
            if (state != NodeState.LEADER) {
                return;
            }
            var term = persistent.getCurrentTerm();
            var leaderCommit = volatileState.getCommitIndex();

            for (String peer : conf.getOldNodes()) {
                if (peer.equals(nodeId)) {
                    continue;
                }
                replicateLogsToFollower(peer, term, leaderCommit);
            }
        } finally {
            lock.unlock();
        }
    }

    private void replicateLogsToFollower(String followerId, long term, long leaderCommit) {
        lock.lock();
        try {
            if (state != NodeState.LEADER) {
                return;
            }
            var log = persistent.getLogStore();
            var nextIdx = leaderState.getNextIndex().getOrDefault(followerId, 1L);
            var lastIndex = persistent.getLogStore().lastIndex();
            if (nextIdx > lastIndex + 1) nextIdx = lastIndex + 1;

            var prevIndex = nextIdx - 1;
            var prevTerm = prevIndex == 0 ? 0 : (log.get(prevIndex) != null ? log.get(prevIndex).getTerm() : 0);

            // Chỉ replicate batch logs, hoặc heartbeat rỗng nếu không có log mới
            List<LogEntry> entries = nextIdx <= lastIndex ? log.readFrom(nextIdx) : List.of();

            var req = new AppendEntriesRequest(term, nodeId, prevIndex, prevTerm, entries, leaderCommit);
            rpcProcessor.appendEntries(followerId, req).thenAccept(resp -> {
                lock.lock();
                try {
                    if (resp.term > persistent.getCurrentTerm()) {
                        becomeFollower(resp.term);
                        return;
                    }
                    if (resp.success) {
                        if (!entries.isEmpty()) {
                            var match = resp.matchIndex;
                            var prevMatch = leaderState.getMatchIndex().getOrDefault(followerId, 0L);
                            leaderState.getMatchIndex().put(followerId, Math.max(prevMatch, match));
                            leaderState.getNextIndex().put(followerId, match + 1);
                            maybeAdvanceCommitIndex();
                        }
                    } else {
                        // Giảm nextIndex an toàn, tránh giảm quá mức
                        var ni = leaderState.getNextIndex().getOrDefault(followerId, 1L);
                        var decrement = Math.max(1, (ni - 1) / 2);
                        leaderState.getNextIndex().put(followerId, Math.max(1, ni - decrement));
                        if (state == NodeState.LEADER) {
                            replicateLogsToFollower(followerId, term, leaderCommit);
                        }
                    }
                } finally {
                    lock.unlock();
                }
            });
        } finally {
            lock.unlock();
        }
    }

    private void maybeAdvanceCommitIndex() {
        var N = persistent.getLogStore().lastIndex();
        var currentCommit = volatileState.getCommitIndex();
        for (var candidate = currentCommit + 1; candidate <= N; candidate++) {
            var count = 1;
            for (var peer : conf.getOldNodes()) {
                if (peer.equals(nodeId)) {
                    continue;
                }
                var m = leaderState.getMatchIndex().get(peer);
                if (m != null && m >= candidate) {
                    count++;
                }
            }
            var majority = (conf.getOldNodes().size() / 2) + 1;
            if (count >= majority) {
                LogEntry e = persistent.getLogStore().get(candidate);
                if (e != null && e.getTerm() == persistent.getCurrentTerm()) {
                    volatileState.setCommitIndex(candidate);
                    applyCommitted();
                }
            }
        }
    }

    private void applyCommitted() {
        var lastApplied = volatileState.getLastApplied();
        var commitIndex = volatileState.getCommitIndex();
        var log = persistent.getLogStore();
        for (long idx = lastApplied + 1; idx <= commitIndex; idx++) {
            LogEntry logEntry = log.get(idx);
            if (logEntry != null) {
                stateMachine.onApply(nodeId, logEntry);
            }
            volatileState.setLastApplied(idx);
            CompletableFuture<Boolean> future = pendingFutures.remove(idx);
            if (future != null) {
                future.complete(true);
            }
        }
        // persist commitIndex
        persistent.setLastCommitIndex(commitIndex);
    }

    public CompletableFuture<Boolean> appendClientCommand(byte[] command) {
        lock.lock();
        try {
            if (state != NodeState.LEADER) {
                return CompletableFuture.completedFuture(false);
            }
            long nextIndex = persistent.getLogStore().lastIndex() + 1;
            LogEntry e = new LogEntry(nextIndex, persistent.getCurrentTerm(), command);
            persistent.getLogStore().appendEntry(e);

            CompletableFuture<Boolean> future = new CompletableFuture<>();
            pendingFutures.put(nextIndex, future);
            scheduler.schedule(() -> {
                lock.lock();
                try {
                    CompletableFuture<Boolean> f = pendingFutures.remove(nextIndex);
                    if (f != null && !f.isDone()) {
                        f.complete(false); // timeout → fail client
                    }
                } finally {
                    lock.unlock();
                }
            }, CLIENT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            // replicate log đến followers
            sendHeartbeats();
            return future;
        } finally {
            lock.unlock();
        }
    }
}
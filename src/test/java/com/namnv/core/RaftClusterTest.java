package com.namnv.core;

import com.namnv.ListStateMachine;
import com.namnv.config.NodeOptions;
import com.namnv.config.RaftConfig;
import com.namnv.entity.LogEntry;
import com.namnv.rpc.client.InMemoryRpcClient;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.TimeoutNowRequest;
import com.namnv.rpc.model.response.TimeoutNowResponse;
import com.namnv.statemachine.snapshot.SnapshotReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

class RaftClusterTest {

    @TempDir
    Path dataDir;

    // RPC in-memory, thêm khả năng làm TimeoutNow thất bại và ghi lại các lần gọi
    static class TestRpc extends InMemoryRpcClient {
        final Set<String> timeoutNowBlocked = ConcurrentHashMap.newKeySet();
        final List<String> timeoutNowCalls = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<TimeoutNowResponse> timeoutNow(String target, TimeoutNowRequest req) {
            timeoutNowCalls.add(target);
            if (timeoutNowBlocked.contains(target)) {
                return CompletableFuture.failedFuture(new IOException("TimeoutNow to " + target + " blocked"));
            }
            return super.timeoutNow(target, req);
        }
    }

    static class FlakyMachine extends ListStateMachine {
        volatile boolean rejectLoad;
        volatile boolean breakOnLoad;

        @Override
        public boolean onSnapshotLoad(SnapshotReader reader) {
            if (breakOnLoad) {
                throw new IllegalStateException("state machine broken");
            }
            return !rejectLoad && super.onSnapshotLoad(reader);
        }
    }

    private TestRpc rpc;
    private Supplier<ListStateMachine> machineFactory = ListStateMachine::new;
    private boolean shutdownOnRemoved = true;
    private int departingTimeoutMs = 10_000;
    private final Map<String, RaftNode> nodes = new HashMap<>();
    private final Map<String, ListStateMachine> machines = new HashMap<>();

    @BeforeEach
    void setUp() {
        rpc = new TestRpc();
        rpc.setReachable(new ConcurrentHashMap<>());
    }

    @AfterEach
    void tearDown() {
        nodes.values().forEach(RaftNode::shutdown);
    }

    // ---------- helpers ----------

    private RaftNode startNode(String id, List<String> peers) {
        var machine = machineFactory.get();
        var folder = dataDir.resolve(id).toString();
        var options = NodeOptions.builder()
                .raftMetaUri(folder)
                .logUri(folder)
                .snapshotUri(folder)
                .electionTimeoutMinMs(150)
                .electionTimeoutMaxMs(300)
                .heartbeatIntervalMs(50)
                .shutdownOnRemoved(shutdownOnRemoved)
                .departingTimeoutMs(departingTimeoutMs)
                .stateMachine(machine)
                .raftConfig(RaftConfig.builder().self(id).peers(peers).build())
                .build();
        var node = new RaftNode(options, rpc);
        nodes.put(id, node);
        machines.put(id, machine);
        rpc.register(id, node);
        node.start();
        return node;
    }

    private void startCluster(String... ids) {
        for (String id : ids) {
            connect(id);
        }
        for (String id : ids) {
            startNode(id, List.of(ids));
        }
    }

    private void stopNode(String id) {
        rpc.unregister(id);
        nodes.remove(id).shutdown();
    }

    // nối hai chiều giữa id và mọi node đang có trong mạng
    private void connect(String id) {
        var network = rpc.getReachable();
        network.computeIfAbsent(id, k -> ConcurrentHashMap.newKeySet()).add(id);
        for (var other : network.keySet()) {
            network.get(other).add(id);
            network.get(id).add(other);
        }
    }

    private void isolate(String id) {
        var network = rpc.getReachable();
        for (var entry : network.entrySet()) {
            if (!entry.getKey().equals(id)) {
                entry.getValue().remove(id);
            }
        }
        Set<String> self = ConcurrentHashMap.newKeySet();
        self.add(id);
        network.put(id, self);
    }

    private static void await(String what, BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail(e);
            }
        }
        fail("Timed out waiting for: " + what);
    }

    private RaftNode awaitLeader(String... excluding) {
        var excluded = Set.of(excluding);
        var found = new RaftNode[1];
        await("a leader", () -> {
            for (var node : nodes.values()) {
                if (!excluded.contains(node.getNodeId()) && isEstablishedLeader(node)) {
                    found[0] = node;
                    return true;
                }
            }
            return false;
        });
        return found[0];
    }

    // leader đã commit được entry của chính term mình: từ lúc này node tụt hậu không thể giành quyền nữa,
    // khác với leader vừa đắc cử còn có thể bị một candidate trùng thời điểm thay thế
    private static boolean isEstablishedLeader(RaftNode node) {
        if (node.getState() != RaftNode.NodeState.LEADER) {
            return false;
        }
        var log = node.getPersistent().getLogStore();
        var commitIndex = node.getVolatileState().getCommitIndex();
        var committed = log.get(commitIndex);
        var commitTerm = committed != null ? committed.getTerm()
                : (commitIndex == log.getBaseIndex() ? log.getBaseTerm() : -1);
        return commitTerm == node.getPersistent().getCurrentTerm();
    }

    private void write(RaftNode leader, String... commands) throws Exception {
        for (String command : commands) {
            assertTrue(leader.appendClientCommand(command.getBytes(StandardCharsets.UTF_8)).get(5, TimeUnit.SECONDS),
                    "command " + command + " was not committed");
        }
    }

    // createSnapshot() hoàn tất ở thread IO, chờ tới khi log thực sự được compact
    private void snapshot(RaftNode node) {
        var appliedIndex = node.getVolatileState().getLastApplied();
        node.createSnapshot();
        await("snapshot of " + node.getNodeId(),
                () -> node.getPersistent().getLogStore().getBaseIndex() >= appliedIndex);
    }

    private void awaitFinalConf(List<String> members, List<String> onNodes) {
        for (String id : onNodes) {
            await("final configuration " + members + " on " + id, () -> {
                var conf = nodes.get(id).getConf();
                return !conf.isJoint() && conf.getOldNodes().equals(members);
            });
        }
    }

    private List<String> others(String... excluded) {
        var skip = Set.of(excluded);
        return nodes.keySet().stream().filter(id -> !skip.contains(id)).sorted().toList();
    }

    private Set<String> departing(RaftNode leader) {
        leader.getLock().lock();
        try {
            var leaderState = leader.getLeaderState();
            return leaderState == null ? Set.of() : Set.copyOf(leaderState.getDeparting().keySet());
        } finally {
            leader.getLock().unlock();
        }
    }

    private void awaitStore(String id, List<String> expected) {
        await("store of " + id + " == " + expected + " but was " + machines.get(id).getStore(),
                () -> machines.get(id).getStore().equals(expected));
    }

    private static LogEntry entry(long index, long term) {
        return new LogEntry(index, term, ("cmd" + index).getBytes(StandardCharsets.UTF_8));
    }

    // ---------- tests ----------

    @Test
    void electsOneLeaderAndReplicates() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();

        write(leader, "1", "2", "3", "4", "5");

        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("1", "2", "3", "4", "5"));
        }
        assertEquals(1, nodes.values().stream().filter(n -> n.getState() == RaftNode.NodeState.LEADER).count());
    }

    @Test
    void singleNodeClusterElectsItselfAndCommits() throws Exception {
        startCluster("A");
        write(awaitLeader(), "1");
        awaitStore("A", List.of("1"));
    }

    @Test
    void staleAppendEntriesDoesNotTruncateNewerEntries() {
        // B không nối được tới ai nên luôn là follower, test gọi thẳng handler như một leader "L"
        var follower = startNode("B", List.of("L", "B"));

        var full = new AppendEntriesRequest(1, "L", 0, 0, List.of(entry(1, 1), entry(2, 1), entry(3, 1)), 0);
        var stale = new AppendEntriesRequest(1, "L", 0, 0, List.of(entry(1, 1)), 0);

        assertEquals(3, follower.handleAppendEntriesRequest(full).matchIndex);
        var response = follower.handleAppendEntriesRequest(stale);

        assertTrue(response.success);
        assertEquals(1, response.matchIndex);
        assertEquals(3, follower.getPersistent().getLogStore().lastIndex());
    }

    @Test
    void conflictingEntriesAreReplaced() {
        var follower = startNode("B", List.of("L", "B"));
        follower.handleAppendEntriesRequest(
                new AppendEntriesRequest(1, "L", 0, 0, List.of(entry(1, 1), entry(2, 1), entry(3, 1)), 0));

        var response = follower.handleAppendEntriesRequest(
                new AppendEntriesRequest(2, "L", 1, 1, List.of(entry(2, 2)), 0));

        assertTrue(response.success);
        var log = follower.getPersistent().getLogStore();
        assertEquals(2, log.lastIndex());
        assertEquals(2, log.get(2).getTerm());

        // prevLogTerm sai thì phải từ chối
        assertFalse(follower.handleAppendEntriesRequest(
                new AppendEntriesRequest(2, "L", 2, 1, List.of(entry(3, 2)), 0)).success);
    }

    @Test
    void isolatedLeaderStepsDownAndRejoins() throws Exception {
        startCluster("A", "B", "C");
        var oldLeader = awaitLeader();
        write(oldLeader, "1");

        isolate(oldLeader.getNodeId());
        var newLeader = awaitLeader(oldLeader.getNodeId());
        assertNotEquals(oldLeader.getNodeId(), newLeader.getNodeId());
        await("old leader to step down", () -> oldLeader.getState() != RaftNode.NodeState.LEADER);

        write(newLeader, "2");
        connect(oldLeader.getNodeId());

        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("1", "2"));
        }
    }

    @Test
    void twoRemainingNodesKeepElectingLeaders() throws Exception {
        // sau mỗi lần leader chết, hai node còn lại phải bầu được leader mới (kể cả khi split vote)
        startCluster("A", "B", "C");
        var expected = new ArrayList<String>();
        for (int round = 0; round < 5; round++) {
            var leader = awaitLeader();
            var id = leader.getNodeId();
            stopNode(id);

            var next = awaitLeader();
            expected.add("r" + round);
            write(next, "r" + round);

            startNode(id, List.of("A", "B", "C"));
            awaitStore(id, expected);
        }
    }

    @Test
    void restartRestoresSnapshotAndLog() throws Exception {
        startCluster("A", "B", "C");
        write(awaitLeader(), "1", "2", "3");
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("1", "2", "3"));
        }
        nodes.values().forEach(this::snapshot);
        write(awaitLeader(), "4", "5");
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("1", "2", "3", "4", "5"));
        }

        for (String id : List.of("A", "B", "C")) {
            stopNode(id);
        }
        for (String id : List.of("A", "B", "C")) {
            startNode(id, List.of("A", "B", "C"));
        }

        write(awaitLeader(), "6");
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("1", "2", "3", "4", "5", "6"));
        }
    }

    @Test
    void laggingFollowerCatchesUpFromSnapshot() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = nodes.keySet().stream().filter(id -> !id.equals(leader.getNodeId())).findFirst().orElseThrow();

        isolate(lagging);
        write(leader, "1", "2", "3");
        snapshot(leader);
        write(leader, "4");

        connect(lagging);
        awaitStore(lagging, List.of("1", "2", "3", "4"));
        assertTrue(nodes.get(lagging).getPersistent().getLogStore().getBaseIndex() > 0);

        // replication tiếp tục bình thường sau snapshot
        write(awaitLeader(), "5");
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("1", "2", "3", "4", "5"));
        }
    }

    @Test
    void newNodeJoinsThroughJointConsensus() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1", "2");
        snapshot(leader);

        connect("D");
        startNode("D", List.of());
        assertTrue(leader.onJoinPeerCluster("D"));

        var members = List.of("A", "B", "C", "D");
        for (String id : members) {
            await("final configuration on " + id, () -> {
                var conf = nodes.get(id).getConf();
                return !conf.isJoint() && conf.getOldNodes().equals(members);
            });
        }
        write(awaitLeader(), "3");
        for (String id : members) {
            awaitStore(id, List.of("1", "2", "3"));
        }

        // cấu hình mới phải còn sau khi restart, không quay về danh sách peers ban đầu
        stopNode("A");
        startNode("A", List.of("A", "B", "C"));
        assertEquals(members, nodes.get("A").getConf().getOldNodes());
    }

    @Test
    void preVoteIsRejectedWhileLeaderIsAlive() {
        var follower = startNode("B", List.of("L", "B", "C"));
        follower.handleAppendEntriesRequest(new AppendEntriesRequest(1, "L", 0, 0, List.of(), 0));

        var preVote = new PreVoteRequest(1, "C", 0, 0);
        assertFalse(follower.handlePreVoteRequest(preVote).voteGranted);

        // hết thời gian tối thiểu mà không nghe leader thì mới đồng ý
        await("pre-vote to be granted", () -> follower.handlePreVoteRequest(preVote).voteGranted);
    }

    @Test
    void followerCanBeRemoved() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1");
        var removed = nodes.keySet().stream().filter(id -> !id.equals(leader.getNodeId())).findFirst().orElseThrow();
        var remaining = nodes.keySet().stream().filter(id -> !id.equals(removed)).sorted().toList();

        assertTrue(leader.onLeavePeerCluster(removed));
        for (String id : remaining) {
            await("final configuration on " + id, () -> {
                var conf = nodes.get(id).getConf();
                return !conf.isJoint() && conf.getOldNodes().equals(remaining);
            });
        }

        // leader tiếp tục gửi log cho node bị gỡ tới khi nó nhận được cấu hình cuối
        await("removed node to learn the final configuration",
                () -> nodes.get(removed).getConf().getOldNodes().equals(remaining));

        // biết chắc mình đã bị gỡ thì node tự tắt
        await("removed node to shut itself down", () -> nodes.get(removed).isStopped());

        // node bị gỡ không còn được tính vào quorum và không thể giành quyền leader
        stopNode(removed);
        write(awaitLeader(), "2");
        for (String id : remaining) {
            awaitStore(id, List.of("1", "2"));
        }
        assertFalse(leader.onLeavePeerCluster(removed));
    }

    @Test
    void leaderCanRemoveItself() throws Exception {
        startCluster("A", "B", "C");
        var oldLeader = awaitLeader();
        write(oldLeader, "1");
        var remaining = nodes.keySet().stream().filter(id -> !id.equals(oldLeader.getNodeId())).sorted().toList();

        assertTrue(oldLeader.onLeavePeerCluster(oldLeader.getNodeId()));

        var newLeader = awaitLeader(oldLeader.getNodeId());
        assertEquals(remaining, newLeader.getConf().getOldNodes());
        write(newLeader, "2");
        for (String id : remaining) {
            awaitStore(id, List.of("1", "2"));
        }

        // node bị gỡ nhận được cấu hình cuối nên tự biết mình đã rời cluster
        await("old leader to learn the final configuration",
                () -> oldLeader.getConf().getOldNodes().equals(remaining));

        // leader cũ trao quyền xong thì tự tắt, cluster còn lại không bị gián đoạn
        await("old leader to shut itself down", oldLeader::isStopped);
        TimeUnit.MILLISECONDS.sleep(1000);
        assertNotEquals(RaftNode.NodeState.LEADER, oldLeader.getState());
        assertEquals(RaftNode.NodeState.LEADER, newLeader.getState());
    }

    @Test
    void crashBetweenSnapshotAndLogTruncationDoesNotReapply() throws Exception {
        startCluster("A");
        write(awaitLeader(), "1", "2", "3");

        var folder = dataDir.resolve("A");
        var logFile = folder.resolve("log_1.jsonl");
        var logBeforeSnapshot = folder.resolve("log.backup");
        Files.copy(logFile, logBeforeSnapshot);

        snapshot(nodes.get("A"));
        stopNode("A");

        // giả lập crash ngay sau khi snapshot được commit: log chưa truncate, còn sót một snapshot ghi dở
        Files.copy(logBeforeSnapshot, logFile, StandardCopyOption.REPLACE_EXISTING);
        Files.createDirectories(folder.resolve("temp"));
        Files.writeString(folder.resolve("temp").resolve("snapshot.data"), "garbage");

        startNode("A", List.of("A"));
        write(awaitLeader(), "4");
        awaitStore("A", List.of("1", "2", "3", "4"));
        assertFalse(Files.exists(folder.resolve("temp")));
    }

    @Test
    void timeoutNowStartsElectionImmediately() {
        var follower = startNode("B", List.of("L", "B", "C"));
        follower.handleAppendEntriesRequest(new AppendEntriesRequest(1, "L", 0, 0, List.of(), 0));

        // yêu cầu của term cũ bị từ chối
        assertFalse(follower.handleTimeoutNowRequest(new TimeoutNowRequest(0, "L")).success);

        assertTrue(follower.handleTimeoutNowRequest(new TimeoutNowRequest(1, "L")).success);
        assertEquals(RaftNode.NodeState.CANDIDATE, follower.getState());
        assertEquals(2, follower.getPersistent().getCurrentTerm());
    }

    @Test
    void leadershipTransferTriesNextFollowerWhenOneFails() {
        startCluster("A", "B", "C");
        var oldLeader = awaitLeader();
        var remaining = others(oldLeader.getNodeId());
        rpc.timeoutNowBlocked.addAll(remaining);

        assertTrue(oldLeader.onLeavePeerCluster(oldLeader.getNodeId()));

        // follower đầu không nhận được thì phải hỏi tiếp follower sau, mỗi node đúng một lần
        await("both followers to be asked", () -> rpc.timeoutNowCalls.size() == 2);
        assertEquals(Set.copyOf(remaining), Set.copyOf(rpc.timeoutNowCalls));

        // không ai nhận lời thì cluster vẫn tự bầu theo election timeout, và leader cũ vẫn tự tắt
        awaitLeader(oldLeader.getNodeId());
        await("old leader to shut itself down", oldLeader::isStopped);
        assertEquals(2, rpc.timeoutNowCalls.size());
    }

    @Test
    void newLeaderFinishesRemovalStartedByOldLeader() {
        startCluster("A", "B", "C", "D");
        var oldLeader = awaitLeader();
        var removed = others(oldLeader.getNodeId()).get(0);
        var remaining = others(removed);

        // node bị gỡ không nhận được gì từ leader cũ
        isolate(removed);
        assertTrue(oldLeader.onLeavePeerCluster(removed));
        awaitFinalConf(remaining, remaining);
        assertTrue(nodes.get(removed).getConf().contains(removed));

        stopNode(oldLeader.getNodeId());
        var newLeader = awaitLeader();
        assertEquals(Set.of(removed), departing(newLeader));

        connect(removed);
        await("removed node to shut itself down", () -> nodes.get(removed).isStopped());
        await("new leader to stop tracking the removed node", () -> departing(newLeader).isEmpty());
    }

    @Test
    void leaderGivesUpOnUnreachableRemovedNode() {
        departingTimeoutMs = 1500;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var removed = others(leader.getNodeId()).get(0);
        var remaining = others(removed);

        isolate(removed);
        assertTrue(leader.onLeavePeerCluster(removed));
        awaitFinalConf(remaining, remaining);
        assertEquals(Set.of(removed), departing(leader));

        await("leader to give up on the removed node", () -> departing(leader).isEmpty());
        assertEquals(RaftNode.NodeState.LEADER, leader.getState());
        assertFalse(nodes.get(removed).isStopped());
    }

    @Test
    void removedNodeStaysUpWhenShutdownOnRemovedIsOff() throws Exception {
        shutdownOnRemoved = false;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var removed = others(leader.getNodeId()).get(0);
        var remaining = others(removed);

        assertTrue(leader.onLeavePeerCluster(removed));
        awaitFinalConf(remaining, List.of("A", "B", "C"));
        await("leader to finish with the removed node", () -> departing(leader).isEmpty());

        TimeUnit.MILLISECONDS.sleep(700);
        assertFalse(nodes.get(removed).isStopped());
        assertEquals(RaftNode.NodeState.FOLLOWER, nodes.get(removed).getState());
        assertEquals(RaftNode.NodeState.LEADER, leader.getState());
    }

    @Test
    void rejectedSnapshotLoadStopsNodeAndRestartRecovers() throws Exception {
        assertFailedLoadStopsNode(machine -> machine.rejectLoad = true);
    }

    @Test
    void brokenSnapshotLoadStopsNodeAndRestartRecovers() throws Exception {
        assertFailedLoadStopsNode(machine -> machine.breakOnLoad = true);
    }

    // onSnapshotLoad trả về false hay ném exception thì node đều dừng hẳn
    private void assertFailedLoadStopsNode(Consumer<FlakyMachine> failure) throws Exception {
        machineFactory = FlakyMachine::new;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = others(leader.getNodeId()).get(0);

        isolate(lagging);
        write(leader, "1", "2", "3");
        snapshot(leader);
        write(leader, "4");

        failure.accept((FlakyMachine) machines.get(lagging));
        connect(lagging);
        await("lagging node to stop after the failed load", () -> nodes.get(lagging).isStopped());

        // snapshot hỏng chưa được commit nên restart dựng lại từ đĩa rồi nhận snapshot lại từ leader
        stopNode(lagging);
        startNode(lagging, List.of("A", "B", "C"));
        awaitStore(lagging, List.of("1", "2", "3", "4"));
    }
}

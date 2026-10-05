package com.namnv.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.namnv.ListStateMachine;
import com.namnv.config.NodeOptions;
import com.namnv.config.RaftConfig;
import com.namnv.entity.ConfigurationEntry;
import com.namnv.entity.LogEntry;
import com.namnv.storage.Checksum;
import com.namnv.storage.DiskFaultInjector;
import com.namnv.rpc.RaftServerService;
import com.namnv.rpc.client.InMemoryRpcClient;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.InstallSnapshotRequest;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.ReadIndexRequest;
import com.namnv.rpc.model.request.RequestVoteRequest;
import com.namnv.rpc.model.request.TimeoutNowRequest;
import com.namnv.rpc.model.response.AppendEntriesResponse;
import com.namnv.rpc.model.response.InstallSnapshotResponse;
import com.namnv.rpc.model.response.PreVoteResponse;
import com.namnv.rpc.model.response.ReadIndexResponse;
import com.namnv.rpc.model.response.RequestVoteResponse;
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
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        // gọi ngay trước khi một RequestVote rời khỏi candidate
        volatile Consumer<RequestVoteRequest> onRequestVote = req -> {
        };

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(String target, RequestVoteRequest req) {
            onRequestVote.accept(req);
            return super.requestVote(target, req);
        }

        // response của AppendEntries do leader này gửi bị giữ lại (request vẫn tới nơi) cho tới khi heldResponses hoàn tất
        volatile String holdResponsesTo;
        volatile CompletableFuture<Void> heldResponses;
        final AtomicInteger held = new AtomicInteger();

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(String target, AppendEntriesRequest req) {
            var response = super.appendEntries(target, req);
            var gate = heldResponses;
            if (gate != null && req.leaderId.equals(holdResponsesTo)) {
                held.incrementAndGet();
                return response.thenCombine(gate, (resp, released) -> resp);
            }
            return response;
        }

        // số ReadIndex đã gửi; response của chúng bị giữ lại cho tới khi heldReadIndex hoàn tất
        final AtomicInteger readIndexCalls = new AtomicInteger();
        volatile CompletableFuture<Void> heldReadIndex;

        @Override
        public CompletableFuture<ReadIndexResponse> readIndex(String target, ReadIndexRequest req) {
            readIndexCalls.incrementAndGet();
            var response = super.readIndex(target, req);
            var gate = heldReadIndex;
            return gate == null ? response : response.thenCombine(gate, (resp, released) -> resp);
        }

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

    // runtime thật nhưng có thể giữ lại mọi thao tác ghi đĩa của node
    static class PausableRuntime extends ThreadedRuntime {
        private final Queue<Runnable> held = new ConcurrentLinkedQueue<>();
        private volatile boolean paused;
        // true: mọi timer của node (heartbeat, election, timeout) bị bỏ qua, node đứng im ở trạng thái hiện tại
        volatile boolean timersFrozen;

        @Override
        public ScheduledTask schedule(Runnable task, long delayMs) {
            return super.schedule(() -> {
                if (!timersFrozen) {
                    task.run();
                }
            }, delayMs);
        }

        @Override
        public void executeIo(Runnable task) {
            if (paused) {
                held.add(task);
            } else {
                super.executeIo(task);
            }
        }

        void pauseIo() {
            paused = true;
        }

        void resumeIo() {
            paused = false;
            for (Runnable task = held.poll(); task != null; task = held.poll()) {
                super.executeIo(task);
            }
        }
    }

    // peer giả: luôn bỏ phiếu thuận và không bao giờ nhận log
    static class StubPeer implements RaftServerService {
        @Override
        public RequestVoteResponse handleRequestVoteRequest(RequestVoteRequest req) {
            return new RequestVoteResponse(req.term, true);
        }

        @Override
        public AppendEntriesResponse handleAppendEntriesRequest(AppendEntriesRequest req) {
            return new AppendEntriesResponse(req.term, false, 0);
        }

        @Override
        public PreVoteResponse handlePreVoteRequest(PreVoteRequest req) {
            return new PreVoteResponse(req.term, true);
        }

        @Override
        public InstallSnapshotResponse handleInstallSnapshotRequest(InstallSnapshotRequest req) {
            return new InstallSnapshotResponse(req.getTerm(), false, false);
        }

        @Override
        public TimeoutNowResponse handleTimeoutNowRequest(TimeoutNowRequest req) {
            return new TimeoutNowResponse(req.term, false);
        }

        @Override
        public CompletableFuture<ReadIndexResponse> handleReadIndexRequest(ReadIndexRequest req) {
            return CompletableFuture.completedFuture(new ReadIndexResponse(false, 0, null));
        }
    }

    private TestRpc rpc;
    private final Map<String, PausableRuntime> runtimes = new HashMap<>();
    private DiskFaultInjector diskFaults = DiskFaultInjector.NONE;
    private int maxPendingCommands = 100_000;
    private int maxEntriesPerRequest = 1024;
    private int logCacheEntries = 16_384;
    private long snapshotIntervalEntries = 0;
    private int catchUpTimeoutMs = 30_000;
    private int snapshotChunkBytes = 1 << 20;
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
        var runtime = new PausableRuntime();
        runtimes.put(id, runtime);
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
                .runtime(runtime)
                .diskFaults(diskFaults)
                .maxPendingCommands(maxPendingCommands)
                .maxEntriesPerRequest(maxEntriesPerRequest)
                .logCacheEntries(logCacheEntries)
                .logSegmentBytes(4096)
                .snapshotIntervalEntries(snapshotIntervalEntries)
                .catchUpTimeoutMs(catchUpTimeoutMs)
                .snapshotChunkBytes(snapshotChunkBytes)
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
        if (node.getState() != NodeState.LEADER) {
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
        assertEquals(1, nodes.values().stream().filter(n -> n.getState() == NodeState.LEADER).count());
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
        await("old leader to step down", () -> oldLeader.getState() != NodeState.LEADER);

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
        assertNotEquals(NodeState.LEADER, oldLeader.getState());
        assertEquals(NodeState.LEADER, newLeader.getState());
    }

    @Test
    void crashBetweenSnapshotAndLogTruncationDoesNotReapply() throws Exception {
        startCluster("A");
        write(awaitLeader(), "1", "2", "3");

        var folder = dataDir.resolve("A");
        var logFile = folder.resolve("log_1.rec");
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
        assertEquals(NodeState.CANDIDATE, follower.getState());
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
        assertEquals(NodeState.LEADER, leader.getState());
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
        assertEquals(NodeState.FOLLOWER, nodes.get(removed).getState());
        assertEquals(NodeState.LEADER, leader.getState());
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

    // ---------- durability: mọi thứ node hứa với node khác phải nằm trên đĩa trước khi lời hứa được gửi đi ----------

    private JsonNode metaOnDisk(String id) {
        try {
            var file = dataDir.resolve(id).resolve("raft_meta.json");
            return new ObjectMapper().readTree(Checksum.unwrap(Files.readAllBytes(file), file.toString()));
        } catch (IOException e) {
            return fail(e);
        }
    }

    @Test
    void voteIsOnDiskBeforeItIsGranted() {
        var follower = startNode("B", List.of("L", "B", "C"));

        assertTrue(follower.handleRequestVoteRequest(new RequestVoteRequest(5, "C", 0, 0)).voteGranted);

        var meta = metaOnDisk("B");
        assertEquals(5, meta.get("currentTerm").asLong());
        assertEquals("C", meta.get("votedFor").asText());
    }

    @Test
    void entriesAreOnDiskBeforeTheyAreAcknowledged() {
        var follower = startNode("B", List.of("L", "B"));

        var response = follower.handleAppendEntriesRequest(
                new AppendEntriesRequest(1, "L", 0, 0, List.of(entry(1, 1), entry(2, 1), entry(3, 1)), 0));

        assertEquals(3, response.matchIndex);
        assertEquals(3, follower.getPersistent().getLogStore().durableIndex());
        assertEquals(1, metaOnDisk("B").get("currentTerm").asLong());
    }

    @Test
    void candidateSelfVoteIsOnDiskBeforeVotesAreRequested() {
        var metaWhenAsked = new CopyOnWriteArrayList<JsonNode>();
        rpc.onRequestVote = req -> metaWhenAsked.add(metaOnDisk(req.candidateId));
        var follower = startNode("B", List.of("L", "B", "C"));
        follower.handleAppendEntriesRequest(new AppendEntriesRequest(1, "L", 0, 0, List.of(), 0));

        assertTrue(follower.handleTimeoutNowRequest(new TimeoutNowRequest(1, "L")).success);

        await("vote requests to be sent", () -> metaWhenAsked.size() == 2);
        for (JsonNode meta : metaWhenAsked) {
            assertEquals(2, meta.get("currentTerm").asLong());
            assertEquals("B", meta.get("votedFor").asText());
        }
    }

    private long matchIndex(RaftNode leader, String peer) {
        leader.getLock().lock();
        try {
            var leaderState = leader.getLeaderState();
            return leaderState == null ? 0 : leaderState.getMatchIndex().getOrDefault(peer, 0L);
        } finally {
            leader.getLock().unlock();
        }
    }

    private void setMatchIndex(RaftNode leader, String peer, long index) {
        leader.getLock().lock();
        try {
            leader.getLeaderState().getMatchIndex().put(peer, index);
        } finally {
            leader.getLock().unlock();
        }
    }

    @Test
    void leaderDoesNotCountItselfBeforeItsLogIsOnDisk() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1");
        var followers = others(leader.getNodeId());

        // chỉ còn leader và một follower: quorum buộc phải tính cả leader
        isolate(followers.get(1));
        runtimes.get(leader.getNodeId()).pauseIo();
        var future = leader.appendClientCommand("2".getBytes(StandardCharsets.UTF_8));
        var index = leader.getPersistent().getLogStore().lastIndex();
        await("follower to store the entry", () -> matchIndex(leader, followers.get(0)) >= index);

        // follower đã có entry trên đĩa, nhưng leader chưa fsync nên chưa được commit
        TimeUnit.MILLISECONDS.sleep(300);
        assertFalse(future.isDone());
        assertTrue(leader.getVolatileState().getCommitIndex() < index);

        runtimes.get(leader.getNodeId()).resumeIo();
        assertTrue(future.get(5, TimeUnit.SECONDS));
    }

    @Test
    void oldTermEntryIsNotCommittedByCountingReplicas() throws Exception {
        // B và C là peer giả: bầu cho A nhưng không nhận log, để test tự đặt matchIndex
        for (String id : List.of("A", "B", "C")) {
            connect(id);
        }
        rpc.register("B", new StubPeer());
        rpc.register("C", new StubPeer());
        var node = startNode("A", List.of("A", "B", "C"));

        // A nhận một entry của term 1 (chưa commit), rồi thành leader của term 2
        node.handleAppendEntriesRequest(new AppendEntriesRequest(1, "B", 0, 0, List.of(entry(1, 1)), 0));
        assertTrue(node.handleTimeoutNowRequest(new TimeoutNowRequest(1, "B")).success);
        await("A to become leader", () -> node.getState() == NodeState.LEADER);
        assertEquals(2, node.getPersistent().getCurrentTerm());

        // như thể B đã có entry của term 1 nhưng chưa có entry nào của term 2: đủ quorum cho index 1
        setMatchIndex(node, "B", 1);
        node.appendClientCommand("y".getBytes(StandardCharsets.UTF_8)); // buộc leader tính lại commit index
        var log = node.getPersistent().getLogStore();
        await("leader log to be on disk", () -> log.durableIndex() == log.lastIndex());
        TimeUnit.MILLISECONDS.sleep(300);
        assertEquals(0, node.getVolatileState().getCommitIndex());
        assertEquals(List.of(), machines.get("A").getStore());

        // khi một entry của term 2 đạt quorum thì entry cũ được commit theo
        setMatchIndex(node, "B", log.lastIndex());
        node.appendClientCommand("z".getBytes(StandardCharsets.UTF_8));
        awaitStore("A", List.of("cmd1", "y"));
    }

    @Test
    void configurationRollsBackWhenItsEntryIsTruncated() {
        var follower = startNode("B", List.of("L", "B", "C"));
        var joint = new ConfigurationEntry(List.of("L", "B", "C"), List.of("L", "B", "C", "D"), true);

        // cấu hình có hiệu lực ngay khi entry vào log, dù chưa commit
        follower.handleAppendEntriesRequest(new AppendEntriesRequest(1, "L", 0, 0,
                List.of(entry(1, 1), LogEntry.newConfigurationEntry(2, 1, joint)), 0));
        assertTrue(follower.getConf().isJoint());
        assertTrue(follower.getConf().contains("D"));

        // leader mới ghi đè entry đó: cấu hình phải quay về bản trước
        follower.handleAppendEntriesRequest(new AppendEntriesRequest(2, "C", 1, 1, List.of(entry(2, 2)), 0));
        assertFalse(follower.getConf().isJoint());
        assertEquals(List.of("L", "B", "C"), follower.getConf().getOldNodes());
        assertFalse(follower.getConf().contains("D"));
    }

    @Test
    void secondConfigurationChangeIsRejectedWhileOneIsInProgress() {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var follower = others(leader.getNodeId()).get(0);

        // giữ lock để thay đổi thứ nhất chưa thể commit trước khi thử thay đổi thứ hai
        leader.getLock().lock();
        try {
            assertTrue(leader.onJoinPeerCluster("D"));
            assertFalse(leader.onLeavePeerCluster(follower));
            assertFalse(leader.onJoinPeerCluster("E"));
        } finally {
            leader.getLock().unlock();
        }
    }

    @Test
    void leaderStepsDownWhenItCannotWriteItsNoOp() {
        var failNextAppend = new boolean[1];
        diskFaults = operation -> {
            if (failNextAppend[0] && operation.equals("log.append")) {
                failNextAppend[0] = false;
                throw new IOException("injected failure of " + operation);
            }
        };
        for (String id : List.of("A", "B", "C")) {
            connect(id);
        }
        rpc.register("B", new StubPeer());
        rpc.register("C", new StubPeer());
        var node = startNode("A", List.of("A", "B", "C"));
        node.handleAppendEntriesRequest(new AppendEntriesRequest(1, "B", 0, 0, List.of(), 0));

        // thắng cử ở term 2 nhưng không ghi được no-op: không được kẹt ở trạng thái leader mà không gửi heartbeat
        failNextAppend[0] = true;
        assertTrue(node.handleTimeoutNowRequest(new TimeoutNowRequest(1, "B")).success);
        await("the failed leader to step down", () -> !failNextAppend[0] && node.getState() == NodeState.FOLLOWER);
        assertEquals(0, node.getPersistent().getLogStore().lastIndex());

        // đĩa hoạt động lại thì bầu lại và làm leader bình thường
        await("node to be elected again", () -> node.getState() == NodeState.LEADER);
        assertTrue(node.getPersistent().getCurrentTerm() > 2);
        assertEquals(1, node.getPersistent().getLogStore().lastIndex());
    }

    @Test
    void nodeWithCorruptedLogRefusesToStartAndClusterCarriesOn() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1", "2", "3");
        var broken = others(leader.getNodeId()).get(0);
        awaitStore(broken, List.of("1", "2", "3"));
        stopNode(broken);

        // một bit hỏng giữa log của node đang tắt
        var segment = dataDir.resolve(broken).resolve("log_1.rec");
        var data = Files.readAllBytes(segment);
        data[32 + 10] ^= 0x01; // trong khung đầu tiên, ngay sau header 32 byte của segment
        Files.write(segment, data);

        assertThrows(Exception.class, () -> startNode(broken, List.of("A", "B", "C")));
        runtimes.get(broken).shutdown();

        // hai node còn lại vẫn là đa số
        write(awaitLeader(), "4");
        for (String id : others(broken)) {
            awaitStore(id, List.of("1", "2", "3", "4"));
        }
    }

    @Test
    void replicatesTenThousandCommandsAndCatchesUpFromSnapshot() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = others(leader.getNodeId()).get(0);
        stopNode(lagging);

        // gửi dồn dập không chờ từng lệnh: leader phải gom lại và commit kịp trong thời hạn của client
        var expected = new ArrayList<String>();
        var pending = new ArrayList<CompletableFuture<Boolean>>();
        for (int i = 0; i < 10_000; i++) {
            expected.add("k" + i);
            pending.add(leader.appendClientCommand(("k" + i).getBytes(StandardCharsets.UTF_8)));
        }
        for (var future : pending) {
            assertTrue(future.get(30, TimeUnit.SECONDS));
        }
        snapshot(leader);

        // node tụt lại 10.000 lệnh nhận snapshot rồi bắt kịp phần còn lại
        expected.add("last");
        write(leader, "last");
        startNode(lagging, List.of("A", "B", "C"));
        for (String id : List.of("A", "B", "C")) {
            await("store of " + id + " to hold " + expected.size() + " commands",
                    () -> machines.get(id).getStore().equals(expected));
        }
        assertTrue(nodes.get(lagging).getPersistent().getLogStore().getBaseIndex() >= 10_000);
    }

    @Test
    void nodeWithCorruptedMetaRefusesToStart() throws Exception {
        startCluster("A");
        write(awaitLeader(), "1");
        stopNode("A");

        // term hoặc phiếu bầu bị đổi âm thầm còn nguy hiểm hơn mất hẳn: node có thể bỏ phiếu hai lần
        var meta = dataDir.resolve("A").resolve("raft_meta.json");
        var text = Files.readString(meta);
        assertTrue(text.contains("\"currentTerm\" : 1"));
        Files.writeString(meta, text.replace("\"currentTerm\" : 1", "\"currentTerm\" : 0"));

        assertThrows(Exception.class, () -> startNode("A", List.of("A")));
        runtimes.get("A").shutdown();
    }

    // ---------- chống ghi trùng ----------

    private CompletableFuture<Boolean> send(RaftNode node, String clientId, long sequence, String command) {
        return node.appendClientCommand(clientId, sequence, command.getBytes(StandardCharsets.UTF_8));
    }

    // clientId -> mốc "mọi sequence tới đây đã apply"
    private Map<String, Long> sessions(RaftNode node) {
        node.getLock().lock();
        try {
            var watermarks = new HashMap<String, Long>();
            node.getSessions().forEach((client, session) -> watermarks.put(client, session.getWatermark()));
            return watermarks;
        } finally {
            node.getLock().unlock();
        }
    }

    @Test
    void retriedCommandIsAppliedOnce() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();

        // gửi lại sau khi đã có kết quả, và gửi hai lần liền nhau khi lần đầu còn chưa commit
        assertTrue(send(leader, "client-1", 1, "x").get(5, TimeUnit.SECONDS));
        assertTrue(send(leader, "client-1", 1, "x").get(5, TimeUnit.SECONDS));
        var first = send(leader, "client-1", 2, "y");
        var second = send(leader, "client-1", 2, "y");
        assertTrue(first.get(5, TimeUnit.SECONDS));
        assertTrue(second.get(5, TimeUnit.SECONDS));

        // client khác dùng lại cùng sequence thì không liên quan
        assertTrue(send(leader, "client-2", 1, "z").get(5, TimeUnit.SECONDS));
        // lần gửi lại rất muộn của một lệnh cũ cũng bị bỏ qua
        assertTrue(send(leader, "client-1", 1, "x").get(5, TimeUnit.SECONDS));
        write(leader, "end");

        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("x", "y", "z", "end"));
        }
    }

    @Test
    void retryAfterLeaderChangeIsAppliedOnce() throws Exception {
        startCluster("A", "B", "C");
        var oldLeader = awaitLeader();
        assertTrue(send(oldLeader, "client-1", 1, "x").get(5, TimeUnit.SECONDS));

        // client không nhận được kết quả và gửi lại cho leader mới
        isolate(oldLeader.getNodeId());
        var newLeader = awaitLeader(oldLeader.getNodeId());
        assertTrue(send(newLeader, "client-1", 1, "x").get(5, TimeUnit.SECONDS));
        assertTrue(send(newLeader, "client-1", 2, "y").get(5, TimeUnit.SECONDS));
        connect(oldLeader.getNodeId());

        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("x", "y"));
        }
    }

    @Test
    void deduplicationSurvivesSnapshotRestartAndInstallSnapshot() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = others(leader.getNodeId()).get(0);
        isolate(lagging);
        assertTrue(send(leader, "client-1", 1, "x").get(5, TimeUnit.SECONDS));
        assertTrue(send(leader, "client-1", 2, "y").get(5, TimeUnit.SECONDS));
        snapshot(leader);
        write(leader, "after");

        // node tụt lại nhận bảng sessions qua InstallSnapshot
        connect(lagging);
        awaitStore(lagging, List.of("x", "y", "after"));
        assertEquals(Map.of("client-1", 2L), sessions(nodes.get(lagging)));

        // sau khi cả cluster restart, bảng được dựng lại từ snapshot nên lệnh gửi lại vẫn bị nhận ra
        for (String id : List.of("A", "B", "C")) {
            stopNode(id);
        }
        for (String id : List.of("A", "B", "C")) {
            startNode(id, List.of("A", "B", "C"));
        }
        var newLeader = awaitLeader();
        assertTrue(send(newLeader, "client-1", 2, "y").get(5, TimeUnit.SECONDS));
        assertTrue(send(newLeader, "client-1", 3, "w").get(5, TimeUnit.SECONDS));
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("x", "y", "after", "w"));
        }
    }

    // ---------- đọc nhất quán ----------

    @Test
    void readSeesEveryAcknowledgedWrite() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var machine = machines.get(leader.getNodeId());

        for (int i = 0; i < 50; i++) {
            write(leader, "w" + i);
            // lệnh vừa được xác nhận phải có trong lần đọc ngay sau đó
            assertEquals(i + 1, leader.read(machine::getStore).get(5, TimeUnit.SECONDS).size());
        }
    }

    @Test
    void singleNodeClusterServesReads() throws Exception {
        startCluster("A");
        var leader = awaitLeader();
        write(leader, "1");
        assertEquals(List.of("1"), leader.read(machines.get("A")::getStore).get(5, TimeUnit.SECONDS));
    }

    @Test
    void followerServesConsistentReads() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var followers = others(leader.getNodeId());

        for (int i = 0; i < 30; i++) {
            write(leader, "w" + i);
            // follower chưa chắc đã apply lệnh vừa commit, nhưng lần đọc nhất quán trên nó vẫn phải thấy
            for (String id : followers) {
                assertEquals(i + 1, nodes.get(id).read(machines.get(id)::getStore).get(5, TimeUnit.SECONDS).size());
            }
        }
    }

    @Test
    void readFailsWithoutAReachableLeader() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1");
        var id = others(leader.getNodeId()).get(0);
        var follower = nodes.get(id);
        await("follower to learn the leader", () -> leader.getNodeId().equals(follower.getLeaderId()));

        // follower bị cô lập không hỏi được leader: thà báo lỗi còn hơn trả dữ liệu có thể đã cũ
        isolate(id);
        var failure = assertThrows(ExecutionException.class,
                () -> follower.read(machines.get(id)::getStore).get(5, TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof NotLeaderException);

        // node chưa từng biết leader nào
        connect("D");
        var loner = startNode("D", List.of());
        failure = assertThrows(ExecutionException.class,
                () -> loner.read(machines.get("D")::getStore).get(5, TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof NotLeaderException);
    }

    @Test
    void isolatedLeaderCannotServeStaleRead() throws Exception {
        startCluster("A", "B", "C");
        var oldLeader = awaitLeader();
        write(oldLeader, "1");

        // leader cũ vẫn tưởng mình là leader trong một lúc, nhưng không còn xác nhận được với đa số
        isolate(oldLeader.getNodeId());
        var staleRead = oldLeader.read(machines.get(oldLeader.getNodeId())::getStore);
        var newLeader = awaitLeader(oldLeader.getNodeId());
        write(newLeader, "2");

        // nếu trả lời, nó sẽ trả về dữ liệu thiếu lệnh "2" đã được xác nhận
        var failure = assertThrows(ExecutionException.class, () -> staleRead.get(10, TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof NotLeaderException);
        assertEquals(List.of("1", "2"),
                newLeader.read(machines.get(newLeader.getNodeId())::getStore).get(5, TimeUnit.SECONDS));
    }

    @Test
    void readIsNotConfirmedByResponsesToEarlierHeartbeats() throws Exception {
        startCluster("A", "B", "C");
        var oldLeader = awaitLeader();
        var id = oldLeader.getNodeId();
        write(oldLeader, "1");

        // giữ lại response của một vòng heartbeat, rồi đóng băng và cô lập leader: nó sẽ không tự step-down
        rpc.holdResponsesTo = id;
        rpc.heldResponses = new CompletableFuture<>();
        await("a heartbeat to each follower to be in flight", () -> rpc.held.get() >= 2);
        runtimes.get(id).timersFrozen = true;
        isolate(id);

        // yêu cầu đọc đến SAU khi các heartbeat đó được gửi đi
        var staleRead = oldLeader.read(machines.get(id)::getStore);
        var newLeader = awaitLeader(id);
        write(newLeader, "2");

        // response cũ về tới nơi: chúng chỉ chứng minh node này là leader trước khi có yêu cầu đọc, không phải sau
        rpc.heldResponses.complete(null);
        TimeUnit.MILLISECONDS.sleep(500);
        assertFalse(staleRead.isDone() && !staleRead.isCompletedExceptionally(),
                "an isolated leader answered a read with stale data");
    }

    @Test
    void concurrentCommandsOfOneClientAreEachAppliedOnce() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();

        // 200 lệnh của cùng một client được gửi dồn dập không chờ nhau, mỗi lệnh gửi hai lần
        var pending = new ArrayList<CompletableFuture<Boolean>>();
        var expected = new ArrayList<String>();
        for (int sequence = 1; sequence <= 200; sequence++) {
            expected.add("cmd" + sequence);
            pending.add(send(leader, "client-1", sequence, "cmd" + sequence));
            pending.add(send(leader, "client-1", sequence, "cmd" + sequence));
        }
        for (var future : pending) {
            assertTrue(future.get(10, TimeUnit.SECONDS));
        }
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, expected);
        }
        // không còn sequence lẻ nào phải nhớ: bảng chỉ giữ một mốc cho mỗi client
        assertEquals(Map.of("client-1", 200L), sessions(leader));
        assertTrue(leader.getSessions().get("client-1").getAbove().isEmpty());
    }

    @Test
    void closedSessionIsForgottenOnEveryNode() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        assertTrue(send(leader, "client-1", 1, "x").get(5, TimeUnit.SECONDS));
        assertTrue(send(leader, "client-2", 1, "y").get(5, TimeUnit.SECONDS));

        assertTrue(leader.closeClientSession("client-1").get(5, TimeUnit.SECONDS));
        write(leader, "end");
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("x", "y", "end"));
            await("session table of " + id, () -> sessions(nodes.get(id)).equals(Map.of("client-2", 1L)));
        }
    }

    @Test
    void leaderRejectsCommandsWhenTooManyArePending() throws Exception {
        maxPendingCommands = 5;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        // không follower nào trả lời nên không lệnh nào commit được
        for (String id : others(leader.getNodeId())) {
            isolate(id);
        }

        var pending = new ArrayList<CompletableFuture<Boolean>>();
        for (int i = 0; i < 5; i++) {
            pending.add(leader.appendClientCommand(("p" + i).getBytes(StandardCharsets.UTF_8)));
        }
        assertFalse(leader.appendClientCommand("over".getBytes(StandardCharsets.UTF_8)).get(1, TimeUnit.SECONDS));
        assertEquals(1, leader.metrics().commandsRejected());
        assertEquals(5, leader.metrics().pendingCommands());
        assertFalse(pending.get(0).isDone());
    }

    @Test
    void snapshotIsTakenAutomaticallyAndBatchesAreBounded() throws Exception {
        snapshotIntervalEntries = 50;
        maxEntriesPerRequest = 7;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = others(leader.getNodeId()).get(0);
        stopNode(lagging);

        var expected = new ArrayList<String>();
        for (int i = 0; i < 230; i++) {
            expected.add("k" + i);
            write(leader, "k" + i);
        }
        // log được compact mà không ai gọi createSnapshot()
        await("automatic snapshots", () -> leader.metrics().snapshotsCreated() >= 4);
        assertTrue(leader.metrics().firstLogIndex() > 150);

        // node tụt lại bắt kịp qua snapshot rồi các request nhỏ
        startNode(lagging, List.of("A", "B", "C"));
        awaitStore(lagging, expected);
        var metrics = leader.metrics();
        assertEquals(NodeState.LEADER, metrics.state());
        assertEquals(metrics.commitIndex(), metrics.lastApplied());
        assertEquals(230, metrics.commandsAccepted());
    }

    @Test
    void nodeThatNeverCatchesUpIsNotAddedAndDoesNotBlockTheCluster() throws Exception {
        catchUpTimeoutMs = 1000;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1");

        // D chưa hề chạy: nó chỉ là learner, cấu hình và quorum không đổi
        assertTrue(leader.onJoinPeerCluster("D"));
        assertFalse(leader.onJoinPeerCluster("E"));
        write(leader, "2");
        assertFalse(leader.getConf().isJoint());
        assertEquals(List.of("A", "B", "C"), leader.getConf().getOldNodes());

        // hết hạn thì yêu cầu bị huỷ và leader nhận thay đổi khác
        connect("E");
        startNode("E", List.of());
        await("the pending change to be abandoned", () -> leader.onJoinPeerCluster("E"));
        awaitFinalConf(List.of("A", "B", "C", "E"), List.of("A", "B", "C", "E"));
        awaitStore("E", List.of("1", "2"));
    }

    @Test
    void severalNodesCanBeReplacedInOneChange() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1", "2");
        var kept = leader.getNodeId();
        var removed = others(kept);
        for (String id : List.of("D", "E")) {
            connect(id);
            startNode(id, List.of());
        }

        var target = List.of(kept, "D", "E");
        assertTrue(leader.changePeers(target));
        awaitFinalConf(target, target);
        for (String id : removed) {
            await("removed node " + id + " to shut itself down", () -> nodes.get(id).isStopped());
        }

        write(awaitLeader(), "3");
        for (String id : target) {
            awaitStore(id, List.of("1", "2", "3"));
        }
        assertFalse(leader.changePeers(target));
        assertFalse(leader.changePeers(List.of()));
    }

    @Test
    void snapshotIsTransferredInSmallChunks() throws Exception {
        snapshotChunkBytes = 7;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = others(leader.getNodeId()).get(0);
        stopNode(lagging);

        var expected = new ArrayList<String>();
        for (int i = 0; i < 40; i++) {
            expected.add("value-" + i);
            assertTrue(send(leader, "client-1", i + 1, "value-" + i).get(5, TimeUnit.SECONDS));
        }
        snapshot(leader);
        var snapshotFile = dataDir.resolve(leader.getNodeId())
                .resolve("snapshot_" + leader.getPersistent().getLastSnapshotIndex()).resolve("snapshot.data");
        assertTrue(Files.size(snapshotFile) > 20L * snapshotChunkBytes, "snapshot must not fit in a few chunks");

        // node tụt lại nhận file qua hàng chục mẩu 7 byte và ghép lại đúng từng byte
        startNode(lagging, List.of("A", "B", "C"));
        awaitStore(lagging, expected);
        var received = dataDir.resolve(lagging)
                .resolve("snapshot_" + nodes.get(lagging).getPersistent().getLastSnapshotIndex()).resolve("snapshot.data");
        assertEquals(Files.readString(snapshotFile), Files.readString(received));
        assertEquals(Map.of("client-1", 40L), sessions(nodes.get(lagging)));
    }

    @Test
    void interruptedSnapshotTransferRestartsCleanly() throws Exception {
        snapshotChunkBytes = 5;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = others(leader.getNodeId()).get(0);
        isolate(lagging);
        var expected = new ArrayList<String>();
        for (int i = 0; i < 60; i++) {
            expected.add("value-" + i);
            write(leader, "value-" + i);
        }
        snapshot(leader);

        // cắt mạng nhiều lần giữa lúc đang truyền: mẩu bị mất, lần truyền phải làm lại mà không ghép sai
        for (int round = 0; round < 6; round++) {
            connect(lagging);
            TimeUnit.MILLISECONDS.sleep(15);
            isolate(lagging);
            TimeUnit.MILLISECONDS.sleep(15);
        }
        connect(lagging);
        awaitStore(lagging, expected);
    }

    @Test
    void outOfOrderSequencesAreDeduplicated() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();

        // lệnh 3 và 2 vào log trước lệnh 1, mỗi lệnh hai lần: bảng phải nhớ cả sequence nằm trên mốc liền mạch
        var pending = new ArrayList<CompletableFuture<Boolean>>();
        for (long sequence : List.of(3L, 2L, 3L, 2L, 1L, 1L)) {
            pending.add(send(leader, "client-1", sequence, "cmd" + sequence));
        }
        for (var future : pending) {
            assertTrue(future.get(5, TimeUnit.SECONDS));
        }
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("cmd3", "cmd2", "cmd1"));
        }
        assertEquals(Map.of("client-1", 3L), sessions(leader));
    }

    @Test
    void resultCallbacksRunOutsideTheNodeLock() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var follower = nodes.get(others(leader.getNodeId()).get(0));

        // callback gắn trực tiếp vào future chạy trên thread hoàn tất nó: thread đó không được còn giữ lock của node
        var written = leader.appendClientCommand("1".getBytes(StandardCharsets.UTF_8))
                .thenApply(ok -> ok && !leader.getLock().isHeldByCurrentThread());
        assertTrue(written.get(5, TimeUnit.SECONDS));
        assertFalse(leader.read(() -> 1).thenApply(v -> leader.getLock().isHeldByCurrentThread()).get(5, TimeUnit.SECONDS));
        assertFalse(follower.read(() -> 1).thenApply(v -> follower.getLock().isHeldByCurrentThread()).get(5, TimeUnit.SECONDS));

        // và nó gọi lại node được mà không bị kẹt
        var chained = leader.appendClientCommand("2".getBytes(StandardCharsets.UTF_8))
                .thenCompose(ok -> leader.appendClientCommand("3".getBytes(StandardCharsets.UTF_8)));
        assertTrue(chained.get(5, TimeUnit.SECONDS));
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("1", "2", "3"));
        }
    }

    @Test
    void concurrentFollowerReadsShareOneReadIndexRequest() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1", "2");
        var id = others(leader.getNodeId()).get(0);
        var follower = nodes.get(id);

        var gate = new CompletableFuture<Void>();
        rpc.heldReadIndex = gate;
        var before = rpc.readIndexCalls.get();
        var reads = new ArrayList<CompletableFuture<List<String>>>();
        for (int i = 0; i < 50; i++) {
            reads.add(follower.read(() -> List.copyOf(machines.get(id).getStore())));
        }
        // lần đọc đầu đã gửi một request; 49 lần còn lại chờ, không gửi thêm request nào
        assertEquals(before + 1, rpc.readIndexCalls.get());
        assertTrue(reads.stream().noneMatch(CompletableFuture::isDone));

        rpc.heldReadIndex = null;
        gate.complete(null);
        for (var read : reads) {
            assertEquals(List.of("1", "2"), read.get(5, TimeUnit.SECONDS));
        }
        // các lần đọc đến sau khi request đầu được gửi đi chung đúng một request nữa
        assertEquals(before + 2, rpc.readIndexCalls.get());
    }

    @Test
    void followerBehindTheLogCacheCatchesUpFromDisk() throws Exception {
        logCacheEntries = 8;
        maxEntriesPerRequest = 5;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = others(leader.getNodeId()).get(0);

        isolate(lagging);
        var expected = new ArrayList<String>();
        for (int i = 0; i < 60; i++) {
            expected.add("w" + i);
        }
        write(leader, expected.toArray(String[]::new));
        // phần follower cần đã rời khỏi bộ nhớ của leader và không có snapshot nào thay cho nó
        var logStore = leader.getPersistent().getLogStore();
        assertEquals(0, logStore.getBaseIndex());
        assertFalse(logStore.isCached(1, maxEntriesPerRequest));

        connect(lagging);
        awaitStore(lagging, expected);
        write(awaitLeader(), "last");
        expected.add("last");
        for (String node : List.of("A", "B", "C")) {
            awaitStore(node, expected);
        }
    }

    @Test
    void logReplicatesRestartsAndCatchesUpAcrossSegments() throws Exception {
        logCacheEntries = 8;
        maxEntriesPerRequest = 5;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = others(leader.getNodeId()).get(0);

        // follower tụt xa hơn bộ nhớ của leader: phần nó cần được đọc lại từ các file segment
        isolate(lagging);
        var expected = new ArrayList<String>();
        for (int i = 0; i < 120; i++) {
            expected.add("w" + i);
        }
        write(leader, expected.toArray(String[]::new));
        connect(lagging);
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, expected);
        }
        try (var files = Files.list(dataDir.resolve(leader.getNodeId()))) {
            var names = files.map(p -> p.getFileName().toString()).filter(name -> name.startsWith("log_")).toList();
            assertTrue(names.size() > 1 && names.stream().allMatch(name -> name.endsWith(".rec")), "segments: " + names);
        }

        // khởi động lại từng node: log đọc lại từ file, rồi snapshot compact nó
        for (String id : List.of("A", "B", "C")) {
            stopNode(id);
            startNode(id, List.of("A", "B", "C"));
            awaitStore(id, expected);
        }
        snapshot(awaitLeader());
        write(awaitLeader(), "after");
        expected.add("after");
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, expected);
        }

        // thư mục còn log dạng JSON của phiên bản cũ phải bị từ chối thay vì được coi là log rỗng
        stopNode("A");
        Files.writeString(dataDir.resolve("A").resolve("log_1.jsonl"), "");
        assertThrows(IOException.class, () -> startNode("A", List.of("A", "B", "C")));
        nodes.remove("A");
    }

    @Test
    void commitIndexIsKeptInItsOwnFileAndADamagedOneIsIgnored() throws Exception {
        startCluster("A");
        write(awaitLeader(), "1", "2", "3");
        var commitIndex = nodes.get("A").getVolatileState().getCommitIndex();
        stopNode("A");

        // file riêng, không nằm trong raft_meta.json: ghi nó không cần fsync
        var file = dataDir.resolve("A").resolve("commit_index");
        var saved = new String(Checksum.unwrap(Files.readAllBytes(file), file.toString()), StandardCharsets.US_ASCII);
        assertEquals(commitIndex, Long.parseLong(saved));
        startNode("A", List.of("A"));
        // apply lại ngay lúc khởi động, trước khi có leader nào
        assertEquals(List.of("1", "2", "3"), machines.get("A").getStore());
        stopNode("A");

        // mất điện giữa lúc ghi đè: node vẫn khởi động, chỉ phải chờ leader commit lại
        Files.writeString(file, "0000");
        startNode("A", List.of("A"));
        awaitStore("A", List.of("1", "2", "3"));
    }
}

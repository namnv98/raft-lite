package com.namnv.raft;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.namnv.entity.LogEntry;
import com.namnv.raft.config.NodeOptions;
import com.namnv.raft.config.RaftConfig;
import com.namnv.raft.example.ListStateMachine;
import com.namnv.raft.runtime.ThreadedRuntime;
import com.namnv.rpc.RaftServerService;
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
import com.namnv.storage.Checksum;
import com.namnv.storage.DiskFaultInjector;
import com.namnv.storage.snapshot.SnapshotReader;
import com.namnv.transport.InMemoryRpcClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
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

/**
 * Hạ tầng chung cho các test cluster: RPC in-memory có thể cắt mạng, runtime tạm dừng được, state machine lỗi theo ý
 * muốn, và các helper dựng cluster, chờ leader, ghi lệnh.
 */
abstract class ClusterTestSupport {

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

        // số AppendEntries tới dropAppendsTarget sẽ bị mất (không tới nơi) trước khi mạng lại bình thường
        volatile String dropAppendsTarget;
        final AtomicInteger appendsToDrop = new AtomicInteger();
        final AtomicInteger appendsDropped = new AtomicInteger();
        // số AppendEntries tới dropAppendsTarget (sau các request bị làm mất ở trên) biến mất không dấu vết: không tới
        // nơi và cũng không bao giờ báo lỗi, như khi máy của peer mất điện và transport không có timeout
        final AtomicInteger appendsToSwallow = new AtomicInteger();

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(String target, AppendEntriesRequest req) {
            if (target.equals(dropAppendsTarget) && appendsToDrop.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                appendsDropped.incrementAndGet();
                return CompletableFuture.failedFuture(new java.io.IOException("dropped"));
            }
            if (target.equals(dropAppendsTarget) && appendsToSwallow.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                return new CompletableFuture<>();
            }
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

    TestRpc rpc;

    final Map<String, PausableRuntime> runtimes = new HashMap<>();

    DiskFaultInjector diskFaults = DiskFaultInjector.NONE;

    int maxPendingCommands = 100_000;

    int maxEntriesPerRequest = 1024;

    int maxInflightAppends = 8;

    List<String> learners = List.of();

    int logCacheEntries = 16_384;

    long snapshotIntervalEntries = 0;

    int catchUpTimeoutMs = 30_000;

    int snapshotChunkBytes = 1 << 20;

    Supplier<ListStateMachine> machineFactory = ListStateMachine::new;

    boolean shutdownOnRemoved = true;

    int departingTimeoutMs = 10_000;

    final Map<String, RaftNode> nodes = new HashMap<>();

    final Map<String, ListStateMachine> machines = new HashMap<>();

    @BeforeEach
    void setUp() {
        rpc = new TestRpc();
        rpc.setReachable(new ConcurrentHashMap<>());
    }

    @AfterEach
    void tearDown() {
        nodes.values().forEach(RaftNode::shutdown);
    }

    RaftNode startNode(String id, List<String> peers) {
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
                .maxInflightAppends(maxInflightAppends)
                .logCacheEntries(logCacheEntries)
                .logSegmentBytes(4096)
                .snapshotIntervalEntries(snapshotIntervalEntries)
                .catchUpTimeoutMs(catchUpTimeoutMs)
                .snapshotChunkBytes(snapshotChunkBytes)
                .raftConfig(RaftConfig.builder().self(id).peers(peers).learners(learners).build())
                .build();
        var node = new RaftNode(options, rpc);
        nodes.put(id, node);
        machines.put(id, machine);
        rpc.register(id, node);
        node.start();
        return node;
    }

    void startCluster(String... ids) {
        for (String id : ids) {
            connect(id);
        }
        for (String id : ids) {
            startNode(id, List.of(ids));
        }
    }

    void stopNode(String id) {
        rpc.unregister(id);
        nodes.remove(id).shutdown();
    }

    // nối hai chiều giữa id và mọi node đang có trong mạng
    void connect(String id) {
        var network = rpc.getReachable();
        network.computeIfAbsent(id, k -> ConcurrentHashMap.newKeySet()).add(id);
        for (var other : network.keySet()) {
            network.get(other).add(id);
            network.get(id).add(other);
        }
    }

    void isolate(String id) {
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

    static void await(String what, BooleanSupplier condition) {
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

    RaftNode awaitLeader(String... excluding) {
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
    static boolean isEstablishedLeader(RaftNode node) {
        if (node.getState() != NodeState.LEADER) {
            return false;
        }
        var log = node.getPersistent().getLogStore();
        var commitIndex = node.getCommitIndex();
        var committed = log.get(commitIndex);
        var commitTerm = committed != null ? committed.getTerm()
                : (commitIndex == log.getBaseIndex() ? log.getBaseTerm() : -1);
        return commitTerm == node.getPersistent().getCurrentTerm();
    }

    void write(RaftNode leader, String... commands) throws Exception {
        for (String command : commands) {
            assertTrue(leader.appendClientCommand(command.getBytes(StandardCharsets.UTF_8)).get(5, TimeUnit.SECONDS),
                    "command " + command + " was not committed");
        }
    }

    // createSnapshot() hoàn tất ở thread IO, chờ tới khi log thực sự được compact
    void snapshot(RaftNode node) {
        var appliedIndex = node.getLastApplied();
        node.createSnapshot();
        await("snapshot of " + node.getNodeId(),
                () -> node.getPersistent().getLogStore().getBaseIndex() >= appliedIndex);
    }

    void awaitFinalConf(List<String> members, List<String> onNodes) {
        for (String id : onNodes) {
            await("final configuration " + members + " on " + id, () -> {
                var conf = nodes.get(id).getConf();
                return !conf.isJoint() && conf.getOldNodes().equals(members);
            });
        }
    }

    List<String> others(String... excluded) {
        var skip = Set.of(excluded);
        return nodes.keySet().stream().filter(id -> !skip.contains(id)).sorted().toList();
    }

    Set<String> departing(RaftNode leader) {
        leader.getLock().lock();
        try {
            var leaderState = leader.getLeaderState();
            return leaderState == null ? Set.of() : Set.copyOf(leaderState.getDeparting().keySet());
        } finally {
            leader.getLock().unlock();
        }
    }

    void awaitStore(String id, List<String> expected) {
        await("store of " + id + " == " + expected + " but was " + machines.get(id).getStore(),
                () -> machines.get(id).getStore().equals(expected));
    }

    static LogEntry entry(long index, long term) {
        return new LogEntry(index, term, ("cmd" + index).getBytes(StandardCharsets.UTF_8));
    }

    // onSnapshotLoad trả về false hay ném exception thì node đều dừng hẳn
    void assertFailedLoadStopsNode(Consumer<FlakyMachine> failure) throws Exception {
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

    JsonNode metaOnDisk(String id) {
        try {
            var file = dataDir.resolve(id).resolve("raft_meta.json");
            return new ObjectMapper().readTree(Checksum.unwrap(Files.readAllBytes(file), file.toString()));
        } catch (IOException e) {
            return fail(e);
        }
    }

    long matchIndex(RaftNode leader, String peer) {
        leader.getLock().lock();
        try {
            var leaderState = leader.getLeaderState();
            return leaderState == null ? 0 : leaderState.getMatchIndex().getOrDefault(peer, 0L);
        } finally {
            leader.getLock().unlock();
        }
    }

    void setMatchIndex(RaftNode leader, String peer, long index) {
        leader.getLock().lock();
        try {
            leader.getLeaderState().getMatchIndex().put(peer, index);
        } finally {
            leader.getLock().unlock();
        }
    }

    CompletableFuture<Boolean> send(RaftNode node, String clientId, long sequence, String command) {
        return node.appendClientCommand(clientId, sequence, command.getBytes(StandardCharsets.UTF_8));
    }

    // clientId -> mốc "mọi sequence tới đây đã apply"
    Map<String, Long> sessions(RaftNode node) {
        node.getLock().lock();
        try {
            var watermarks = new HashMap<String, Long>();
            node.getSessions().forEach((client, session) -> watermarks.put(client, session.getWatermark()));
            return watermarks;
        } finally {
            node.getLock().unlock();
        }
    }

    static List<byte[]> commands(String... values) {
        var result = new ArrayList<byte[]>();
        for (String value : values) {
            result.add(value.getBytes(StandardCharsets.UTF_8));
        }
        return result;
    }

    // giữ response của leader lại rồi ghi từng lệnh một, mỗi lệnh trong một lượt xử lý riêng của leader;
    // trả về số AppendEntries đã được gửi đi mà chưa có câu trả lời. Giữ ngắn hơn hẳn ngưỡng check-quorum (300 ms),
    // nếu không leader tự step-down vì không nghe follower nào trả lời
    int appendsInFlightWhileResponsesAreHeld(RaftNode leader, int commands) throws Exception {
        write(leader, "0"); // log của follower đã khớp
        rpc.holdResponsesTo = leader.getNodeId();
        rpc.heldResponses = new CompletableFuture<>();
        rpc.held.set(0);
        var pending = new ArrayList<CompletableFuture<Boolean>>();
        for (int i = 1; i <= commands; i++) {
            pending.add(leader.appendClientCommand(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
            TimeUnit.MILLISECONDS.sleep(3);
        }
        TimeUnit.MILLISECONDS.sleep(30);
        int inFlight = rpc.held.get();
        rpc.holdResponsesTo = null;
        rpc.heldResponses.complete(null);
        for (var write : pending) {
            assertTrue(write.get(10, TimeUnit.SECONDS));
        }
        return inFlight;
    }
}

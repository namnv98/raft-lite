package com.namnv.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.namnv.ListStateMachine;
import com.namnv.config.NodeOptions;
import com.namnv.config.RaftConfig;
import com.namnv.core.RaftInvariants.Op;
import com.namnv.core.RaftInvariants.Read;
import com.namnv.entity.LogEntry;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Chạy cluster 5 node dưới lỗi ngẫu nhiên (mất gói, trễ, đảo thứ tự, gửi trùng, partition, tắt node, mất điện,
 * snapshot, thêm/gỡ thành viên) trong khi client ghi liên tục, rồi kiểm tra các bất biến của Raft.
 * <p>
 * Tuỳ chỉnh: -Dchaos.seed=123 (chạy lại đúng một seed), -Dchaos.runs=3, -Dchaos.seconds=5.
 * Seed cố định chuỗi lỗi được chọn chứ không cố định lịch chạy của các thread, nên chạy lại chưa chắc tái hiện y hệt.
 */
class RaftChaosTest {

    private static final List<String> IDS = List.of("A", "B", "C", "D", "E");
    private static final int CLIENTS = 3;

    /**
     * RPC in-memory có lỗi: request hoặc response có thể bị mất, bị trễ (nên đến sai thứ tự), request có thể đến hai lần.
     */
    static class ChaosRpc extends InMemoryRpcClient {
        private final Random random;
        private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        volatile boolean chaos = true;
        double dropRate = 0.05;
        double duplicateRate = 0.03;
        int maxDelayMs = 40;
        int maxDuplicateDelayMs = 300;

        ChaosRpc(long seed) {
            this.random = new Random(seed);
        }

        private <T> CompletableFuture<T> faulty(Supplier<CompletableFuture<T>> send) {
            if (!chaos) {
                return send.get();
            }
            if (random.nextDouble() < dropRate) {
                return CompletableFuture.failedFuture(new IOException("request dropped"));
            }
            var result = new CompletableFuture<T>();
            scheduler.schedule(() -> {
                if (random.nextDouble() < duplicateRate) {
                    // bản sao của request đến muộn hơn hẳn, sau cả những request gửi sau nó; response bị bỏ
                    scheduler.schedule(() -> send.get(), random.nextInt(maxDuplicateDelayMs), TimeUnit.MILLISECONDS);
                }
                send.get().whenComplete((response, error) -> {
                    if (error != null) {
                        result.completeExceptionally(error);
                    } else if (random.nextDouble() < dropRate) {
                        // handler đã chạy xong nhưng người gửi không bao giờ biết
                        result.completeExceptionally(new IOException("response dropped"));
                    } else {
                        scheduler.schedule(() -> result.complete(response), random.nextInt(maxDelayMs), TimeUnit.MILLISECONDS);
                    }
                });
            }, random.nextInt(maxDelayMs), TimeUnit.MILLISECONDS);
            return result;
        }

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(String target, RequestVoteRequest req) {
            return faulty(() -> super.requestVote(target, req));
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(String target, AppendEntriesRequest req) {
            return faulty(() -> super.appendEntries(target, req));
        }

        @Override
        public CompletableFuture<PreVoteResponse> preVote(String target, PreVoteRequest req) {
            return faulty(() -> super.preVote(target, req));
        }

        @Override
        public CompletableFuture<InstallSnapshotResponse> installSnapshot(String target, InstallSnapshotRequest req) {
            return faulty(() -> super.installSnapshot(target, req));
        }

        @Override
        public CompletableFuture<TimeoutNowResponse> timeoutNow(String target, TimeoutNowRequest req) {
            return faulty(() -> super.timeoutNow(target, req));
        }

        @Override
        public CompletableFuture<ReadIndexResponse> readIndex(String target, ReadIndexRequest req) {
            return faulty(() -> super.readIndex(target, req));
        }

        void close() {
            scheduler.shutdownNow();
        }
    }

    @TempDir
    Path dataDir;

    private ChaosRpc rpc;
    private Random random;
    private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();
    private final Map<String, ListStateMachine> machines = new ConcurrentHashMap<>();
    private final Set<String> down = ConcurrentHashMap.newKeySet();
    private final Queue<Op> ops = new ConcurrentLinkedQueue<>();
    private final Queue<Read> reads = new ConcurrentLinkedQueue<>();
    private final Set<String> invoked = ConcurrentHashMap.newKeySet();
    private final Map<Long, String> leaderByTerm = new ConcurrentHashMap<>();
    private final Queue<String> violations = new ConcurrentLinkedQueue<>();
    private volatile boolean running;
    private final ObjectMapper objectMapper = new ObjectMapper();

    static LongStream seeds() {
        var fixed = Long.getLong("chaos.seed");
        if (fixed != null) {
            return LongStream.of(fixed);
        }
        var base = System.nanoTime();
        return LongStream.range(0, Integer.getInteger("chaos.runs", 3)).map(i -> base + i);
    }

    @AfterEach
    void tearDown() {
        running = false;
        nodes.values().forEach(RaftNode::shutdown);
        if (rpc != null) {
            rpc.close();
        }
    }

    @ParameterizedTest(name = "seed={0}")
    @MethodSource("seeds")
    void keepsRaftInvariantsUnderRandomFaults(long seed) throws Exception {
        rpc = new ChaosRpc(seed);
        rpc.setReachable(new ConcurrentHashMap<>());
        random = new Random(seed ^ 0x5DEECE66DL);
        healNetwork();
        IDS.forEach(this::startNode);

        running = true;
        var threads = new ArrayList<Thread>();
        threads.add(Thread.ofPlatform().name("nemesis").start(this::nemesis));
        threads.add(Thread.ofPlatform().name("monitor").start(this::monitor));
        threads.add(Thread.ofPlatform().name("reader").start(this::reader));
        for (int c = 0; c < CLIENTS; c++) {
            var client = c;
            threads.add(Thread.ofPlatform().name("client-" + c).start(() -> client(client)));
        }

        TimeUnit.SECONDS.sleep(Integer.getInteger("chaos.seconds", 5));
        running = false;
        for (var thread : threads) {
            thread.join(10_000);
        }

        // hết lỗi: mạng lành lại, mọi node bật lại, cluster phải hoạt động được và hội tụ
        rpc.chaos = false;
        healNetwork();
        new ArrayList<>(down).forEach(this::restart);
        restoreFullMembership();
        var marker = writeUntilCommitted();
        var converged = waitFor(30_000, () -> {
            var reference = machines.get(IDS.get(0)).getStore();
            return reference.contains(marker)
                    && IDS.stream().allMatch(id -> machines.get(id).getStore().equals(reference));
        });
        checkElectionSafety();
        checkAppliedPrefixes();
        checkLogMatching();
        if (!converged) {
            fail("nodes did not converge within 30s after all faults were healed; violations so far: " + distinctViolations());
        }

        var committed = machines.get(IDS.get(0)).getStore();
        var acked = ops.stream().filter(Op::acked).count();
        System.out.printf("chaos seed=%d: %d ops invoked, %d acked, %d committed, %d terms%n",
                seed, ops.size(), acked, committed.size(), leaderByTerm.size());

        assertEquals(List.of(), distinctViolations(), "invariant violations during the run");
        RaftInvariants.assertNoDuplicates(committed);
        RaftInvariants.assertOnlyInvokedCommands(committed, invoked);
        RaftInvariants.assertAckedCommandsSurvive(committed, ops);
        RaftInvariants.assertRealTimeOrder(committed, ops);
        RaftInvariants.assertReadsLinearizable(committed, ops, List.copyOf(reads));
        assertTrue(acked > 0, "no write was ever acknowledged, the run did not exercise anything");
    }

    // ---------- cluster ----------

    private void startNode(String id) {
        var machine = new ListStateMachine();
        var folder = dataDir.resolve(id).toString();
        var options = NodeOptions.builder()
                .raftMetaUri(folder)
                .logUri(folder)
                // segment nhỏ: log đi qua nhiều file, và mỗi lần mất điện giả lập không phải chép cả file 64 MB
                .logSegmentBytes(16 << 10)
                .snapshotUri(folder)
                .electionTimeoutMinMs(150)
                .electionTimeoutMaxMs(300)
                .heartbeatIntervalMs(50)
                // node bị gỡ vẫn chạy để nemesis có thể thêm nó lại
                .shutdownOnRemoved(false)
                .stateMachine(machine)
                .raftConfig(RaftConfig.builder().self(id).peers(IDS).build())
                .build();
        var node = new RaftNode(options, rpc);
        machines.put(id, machine);
        nodes.put(id, node);
        rpc.register(id, node);
        node.start();
    }

    private void crash(String id) {
        down.add(id);
        rpc.unregister(id);
        nodes.get(id).shutdown();
    }

    /**
     * Mất điện: mọi thứ chưa fsync biến mất. Giữ lock của node để log không đổi trong lúc chụp lại trạng thái đĩa;
     * mọi lời ack node đã gửi ra trước thời điểm này đều dựa trên dữ liệu đã bền vững nên vẫn nằm trong bản chụp.
     */
    private void powerLoss(String id) throws IOException {
        down.add(id);
        rpc.unregister(id);
        RaftInvariants.powerLoss(nodes.get(id), dataDir.resolve(id));
    }

    private void restart(String id) {
        startNode(id);
        down.remove(id);
    }

    private List<String> liveNodes() {
        return IDS.stream().filter(id -> !down.contains(id)).toList();
    }

    private String pick(List<String> ids) {
        return ids.get(random.nextInt(ids.size()));
    }

    private void healNetwork() {
        partition(Set.of());
    }

    // các node trong minority chỉ thấy nhau, phần còn lại chỉ thấy nhau
    private void partition(Set<String> minority) {
        var network = rpc.getReachable();
        for (String id : IDS) {
            Set<String> side = ConcurrentHashMap.newKeySet();
            for (String other : IDS) {
                if (minority.contains(id) == minority.contains(other)) {
                    side.add(other);
                }
            }
            network.put(id, side);
        }
    }

    // ---------- actors ----------

    private void nemesis() {
        while (running) {
            sleep(200 + random.nextInt(500));
            try {
                switch (random.nextInt(10)) {
                    case 8 -> removeRandomMember();
                    case 9 -> addMissingMember();
                    case 0, 1 -> healNetwork();
                    // cô lập đúng leader: nó còn nhận lệnh của client một lúc, tạo ra đuôi log chưa commit bị ghi đè sau này
                    case 7 -> partition(Set.of(leaderOrAny().getNodeId()));
                    case 2 -> partition(Set.of(pick(IDS)));
                    case 3 -> {
                        // hai lần chọn có thể trùng nhau, khi đó minority chỉ có một node
                        partition(new HashSet<>(List.of(pick(IDS), pick(IDS))));
                    }
                    case 4 -> {
                        // giữ đa số còn sống để cluster vẫn có thể tiến triển
                        if (down.size() < 2) {
                            var victim = pick(liveNodes());
                            if (random.nextBoolean()) {
                                crash(victim);
                            } else {
                                powerLoss(victim);
                            }
                        }
                    }
                    case 5 -> {
                        if (!down.isEmpty()) {
                            restart(pick(List.copyOf(down)));
                        }
                    }
                    default -> nodes.get(pick(liveNodes())).createSnapshot();
                }
            } catch (Throwable t) {
                violations.add("nemesis failed: " + t);
            }
        }
    }

    // gỡ một thành viên bất kỳ (kể cả leader), nhưng luôn giữ ít nhất 3 thành viên
    private void removeRandomMember() {
        var leader = leaderOrAny();
        var members = leader.getConf().getOldNodes();
        if (members.size() > 3) {
            leader.onLeavePeerCluster(pick(members));
        }
    }

    private void addMissingMember() {
        var leader = leaderOrAny();
        var members = leader.getConf().getOldNodes();
        var missing = IDS.stream().filter(id -> !members.contains(id)).toList();
        if (!missing.isEmpty()) {
            leader.onJoinPeerCluster(pick(missing));
        }
    }

    // sau khi hết lỗi: đưa cluster về đủ 5 thành viên
    private void restoreFullMembership() {
        var restored = waitFor(30_000, () -> {
            var leader = leaderOrAny();
            var conf = leader.getConf();
            if (leader.getState() == NodeState.LEADER && !conf.isJoint() && conf.getOldNodes().containsAll(IDS)) {
                return true;
            }
            addMissingMember();
            sleep(100);
            return false;
        });
        if (!restored) {
            fail("cluster could not get back to full membership within 30s; violations so far: " + distinctViolations());
        }
    }

    // gửi lần lượt từng lệnh; không biết kết quả thì gửi lại đúng (clientId, sequence) đó tới khi được xác nhận
    private void client(int clientId) {
        for (int n = 0; running; n++) {
            var command = "c" + clientId + "-" + n;
            invoked.add(command);
            var invokedAt = System.nanoTime();
            var ok = false;
            while (!ok && running) {
                try {
                    ok = leaderOrAny().appendClientCommand("client-" + clientId, n + 1, command.getBytes(StandardCharsets.UTF_8))
                            .get(2, TimeUnit.SECONDS);
                } catch (Exception e) {
                    // timeout: không biết lệnh có được commit hay không
                }
                if (!ok) {
                    sleep(20);
                }
            }
            ops.add(new Op(command, invokedAt, ok ? System.nanoTime() : Long.MAX_VALUE));
        }
    }

    // đọc nhất quán liên tục từ một node bất kỳ, leader hay follower
    private void reader() {
        while (running) {
            var target = nodes.get(pick(IDS));
            var machine = machines.get(target.getNodeId());
            var invokedAt = System.nanoTime();
            try {
                var seen = target.read(machine::getStore).get(2, TimeUnit.SECONDS);
                reads.add(new Read(invokedAt, System.nanoTime(), seen.size(), seen.isEmpty() ? null : seen.get(seen.size() - 1)));
            } catch (Exception e) {
                // không phải leader hoặc không xác nhận được với đa số
            }
            sleep(10);
        }
    }

    // client không biết leader nào là thật: chọn ngẫu nhiên trong các node tự nhận là leader
    private RaftNode leaderOrAny() {
        var leaders = IDS.stream().filter(id -> nodes.get(id).getState() == NodeState.LEADER).toList();
        return nodes.get(pick(leaders.isEmpty() ? IDS : leaders));
    }

    private void monitor() {
        for (int tick = 0; running; tick++) {
            sleep(2);
            try {
                // hai leader cùng term có thể chỉ tồn tại trong chốc lát nên kiểm tra thật dày
                checkElectionSafety();
                if (tick % 50 == 0) {
                    checkAppliedPrefixes();
                    checkLogMatching();
                }
            } catch (Throwable t) {
                violations.add("monitor failed: " + t);
            }
        }
    }

    // ---------- invariants ----------

    // Election Safety: mỗi term có nhiều nhất một leader
    private void checkElectionSafety() {
        for (String id : IDS) {
            var node = nodes.get(id);
            long term;
            node.getLock().lock();
            try {
                if (node.isStopped() || node.getState() != NodeState.LEADER) {
                    continue;
                }
                term = node.getPersistent().getCurrentTerm();
            } finally {
                node.getLock().unlock();
            }
            var previous = leaderByTerm.putIfAbsent(term, id);
            if (previous != null && !previous.equals(id)) {
                violations.add("two leaders in term " + term + ": " + previous + " and " + id);
            }
        }
    }

    // State Machine Safety: hai node bất kỳ không bao giờ apply hai lệnh khác nhau ở cùng một vị trí
    private void checkAppliedPrefixes() {
        var stores = new HashMap<String, List<String>>();
        for (String id : IDS) {
            stores.put(id, machines.get(id).getStore());
        }
        RaftInvariants.checkAppliedPrefixes(stores, violations);
    }

    // Log Matching: hai log có entry cùng index và cùng term thì giống hệt nhau từ đó trở về trước.
    // Mỗi cặp (index, term) xác định duy nhất phần log đứng trước nó, nên so sánh hai log đọc ở hai thời điểm khác nhau vẫn đúng.
    private void checkLogMatching() {
        var logs = new HashMap<String, List<LogEntry>>();
        for (String id : IDS) {
            logs.put(id, nodes.get(id).getPersistent().getLogStore().readFrom(1));
        }
        RaftInvariants.checkLogMatching(logs, violations);
    }

    private List<String> distinctViolations() {
        return violations.stream().distinct().limit(5).toList();
    }

    // ---------- helpers ----------

    // ghi một lệnh đánh dấu cho tới khi được xác nhận; trả về lệnh đó
    private String writeUntilCommitted() throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        for (int attempt = 0; System.currentTimeMillis() < deadline; attempt++) {
            var command = "final-" + attempt;
            invoked.add(command);
            try {
                if (leaderOrAny().appendClientCommand(command.getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS)) {
                    return command;
                }
            } catch (Exception e) {
                // thử lại
            }
            sleep(50);
        }
        return fail("cluster did not accept a write within 30s after all faults were healed");
    }

    private static boolean waitFor(long timeoutMs, BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            sleep(20);
        }
        return false;
    }

    private static void sleep(long millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

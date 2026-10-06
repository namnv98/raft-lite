package com.namnv.core;

import com.namnv.ListStateMachine;
import com.namnv.config.NodeOptions;
import com.namnv.config.RaftConfig;
import com.namnv.core.RaftInvariants.Op;
import com.namnv.core.RaftInvariants.Read;
import com.namnv.entity.LogEntry;
import com.namnv.rpc.RaftServerService;
import com.namnv.rpc.RpcProcessor;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Mô phỏng tất định: cả cluster 5 node, mạng, đĩa lỗi, đồng hồ lệch, client và nemesis chạy trên một thread với thời gian ảo.
 * Mọi quyết định ngẫu nhiên lấy từ một seed, nên một seed luôn cho đúng một lịch sử và lỗi tìm ra thì chạy lại được y hệt.
 * Vì thời gian là ảo, 30 giây của cluster chỉ mất khoảng một giây thật.
 * <p>
 * Tuỳ chỉnh: -Dsim.seed=123 (chạy lại đúng một seed), -Dsim.runs=8, -Dsim.seconds=30.
 */
class RaftSimulationTest {

    private static final List<String> FIVE = List.of("A", "B", "C", "D", "E");
    private static final List<String> SEVEN = List.of("A", "B", "C", "D", "E", "F", "G");
    private static final int CLIENTS = 3;

    record Result(List<String> committed, int invoked, long acked, int reads, int terms, int confChanges,
                  long snapshotsInstalled, long events) {
    }

    @TempDir
    Path tempDir;

    static LongStream seeds() {
        var fixed = Long.getLong("sim.seed");
        if (fixed != null) {
            return LongStream.of(fixed);
        }
        var base = System.nanoTime();
        return LongStream.range(0, Integer.getInteger("sim.runs", 8)).map(i -> base + i);
    }

    @ParameterizedTest(name = "seed={0}")
    @MethodSource("seeds")
    void keepsRaftInvariantsUnderRandomFaults(long seed) {
        report(seed, new Run(seed, tempDir, FIVE, Integer.getInteger("sim.seconds", 30)).execute());
    }

    static LongStream fewSeeds() {
        return seeds().limit(3);
    }

    @ParameterizedTest(name = "seed={0}")
    @MethodSource("fewSeeds")
    void sevenNodeClusterKeepsInvariants(long seed) {
        report(seed, new Run(seed, tempDir, SEVEN, Integer.getInteger("sim.seconds", 30)).execute());
    }

    // nửa giờ ảo liên tục: log qua nhiều vòng snapshot, hàng chục nghìn lệnh và hàng trăm nhiệm kỳ
    @Test
    void longRunKeepsInvariants() {
        long seed = Long.getLong("sim.seed", System.nanoTime());
        report(seed, new Run(seed, tempDir, FIVE, Integer.getInteger("sim.longSeconds", 1800)).execute());
    }

    private static void report(long seed, Result result) {
        System.out.printf("sim seed=%d: %d ops invoked, %d acked, %d committed, %d reads, %d terms, %d conf changes, "
                        + "%d snapshots installed, %d events%n",
                seed, result.invoked(), result.acked(), result.committed().size(), result.reads(), result.terms(),
                result.confChanges(), result.snapshotsInstalled(), result.events());
    }

    @Test
    void sameSeedReplaysTheSameHistory() {
        long seed = Long.getLong("sim.seed", 20240601L);
        var first = new Run(seed, tempDir.resolve("first"), FIVE, 30).execute();
        var second = new Run(seed, tempDir.resolve("second"), FIVE, 30).execute();
        assertEquals(first, second, "the same seed produced two different histories");
    }

    // ---------- lõi mô phỏng ----------

    /**
     * Hàng đợi sự kiện theo thời gian ảo. Mỗi lần chỉ chạy một sự kiện, đồng hồ nhảy thẳng tới thời điểm của nó.
     */
    static final class Simulation {
        private record Event(long at, long seq, Runnable task) {
        }

        private final PriorityQueue<Event> queue =
                new PriorityQueue<>(Comparator.comparingLong(Event::at).thenComparingLong(Event::seq));
        final Random random;
        long nowNanos;
        long executed;
        private long seq;

        Simulation(long seed) {
            this.random = new Random(seed);
        }

        void schedule(long delayMs, Runnable task) {
            queue.add(new Event(nowNanos + delayMs * 1_000_000L, seq++, task));
        }

        long nowMs() {
            return nowNanos / 1_000_000L;
        }

        // thời điểm của sự kiện kế tiếp, Long.MAX_VALUE nếu không còn gì
        long nextAtMs() {
            var next = queue.peek();
            return next == null ? Long.MAX_VALUE : next.at() / 1_000_000L;
        }

        void runNext() {
            var event = queue.poll();
            nowNanos = event.at();
            executed++;
            event.task().run();
        }
    }

    // runtime của một node: timer và thao tác ghi đĩa trở thành sự kiện trong hàng đợi chung
    static final class SimRuntime implements RaftRuntime {
        private static final int IO_DELAY_MS = 15;
        private final Simulation sim;
        // đồng hồ của node chạy nhanh hay chậm hơn thời gian chung bao nhiêu lần
        private final double clockRate;
        private boolean dead;

        SimRuntime(Simulation sim, double clockRate) {
            this.sim = sim;
            this.clockRate = clockRate;
        }

        @Override
        public long nanoTime() {
            return (long) (sim.nowNanos * clockRate);
        }

        @Override
        public ScheduledTask schedule(Runnable task, long delayMs) {
            var cancelled = new boolean[1];
            // node có đồng hồ nhanh thì timer của nó hết hạn sớm hơn theo thời gian chung
            sim.schedule(Math.round(delayMs / clockRate), () -> {
                if (!dead && !cancelled[0]) {
                    task.run();
                }
            });
            return () -> cancelled[0] = true;
        }

        @Override
        public void executeIo(Runnable task) {
            // độ trễ cố định giữ đúng thứ tự gửi vào, như một thread IO thật; đủ dài để ack của follower
            // có thể về trước khi leader fsync xong log của chính nó
            sim.schedule(IO_DELAY_MS, () -> {
                if (!dead) {
                    task.run();
                }
            });
        }

        @Override
        public int nextInt(int bound) {
            return sim.random.nextInt(bound);
        }

        @Override
        public void shutdown() {
            dead = true;
        }
    }

    // ghi snapshot ngay tại chỗ thay vì ở thread riêng
    static final class SimMachine extends ListStateMachine {
        @Override
        protected void runSnapshotWrite(Runnable write) {
            write.run();
        }
    }

    /**
     * Một lần chạy: dựng cluster, chạy có lỗi trong sim.seconds giây ảo, chữa lành rồi kiểm tra bất biến.
     */
    static final class Run implements RpcProcessor {
        private final long seed;
        private final Path dataDir;
        private final List<String> ids;
        private final int seconds;
        private final Simulation sim;
        private final Random random;

        private final Map<String, RaftNode> nodes = new HashMap<>();
        private final Map<String, ListStateMachine> machines = new HashMap<>();
        private final Map<String, RaftServerService> registry = new HashMap<>();
        private final Map<String, Set<String>> reachable = new HashMap<>();
        private final Set<String> down = new TreeSet<>();

        private final List<Op> ops = new ArrayList<>();
        private final List<Read> reads = new ArrayList<>();
        private final Set<String> invoked = new HashSet<>();
        private final Map<Long, String> leaderByTerm = new HashMap<>();
        private final List<String> violations = new ArrayList<>();

        private boolean faults = true;
        private boolean actorsRunning = true;
        private long stamp;
        private int confChanges;
        private long snapshotsInstalled;
        private String marker;

        private final double dropRate = 0.05;
        private final double duplicateRate = 0.03;
        private final double diskFaultRate = 0.02;
        private final double stragglerRate = 0.01;
        // một nửa số lượt dùng khoảng election timeout rộng: leader bị cô lập còn tự coi là leader lâu sau khi
        // phần còn lại đã bầu leader mới, là lúc đọc nhất quán dễ sai nhất
        private final int electionTimeoutMaxMs;
        private final int maxDelayMs = 40;
        private final int maxDuplicateDelayMs = 300;

        Run(long seed, Path dataDir, List<String> ids, int seconds) {
            this.seed = seed;
            this.dataDir = dataDir;
            this.ids = ids;
            this.seconds = seconds;
            this.sim = new Simulation(seed);
            this.random = sim.random;
            this.electionTimeoutMaxMs = random.nextBoolean() ? 300 : 900;
        }

        Result execute() {
            try {
                return simulate();
            } finally {
                nodes.values().forEach(RaftNode::shutdown);
            }
        }

        private Result simulate() {
            healNetwork();
            ids.forEach(this::startNode);
            sim.schedule(200, this::nemesis);
            sim.schedule(100, this::heavyChecks);
            for (int c = 0; c < CLIENTS; c++) {
                var client = c;
                sim.schedule(300 + c, () -> clientStep(client, 0));
            }
            sim.schedule(400, this::readerStep);
            runFor(seconds * 1000L, () -> false);

            // hết lỗi: mạng lành lại, mọi node bật lại, cluster phải về đủ 5 thành viên, nhận lệnh ghi và hội tụ
            actorsRunning = false;
            faults = false;
            healNetwork();
            noticeStoppedNodes();
            new ArrayList<>(down).forEach(this::restart);
            sim.schedule(100, this::restoreMembership);
            require(runFor(60_000, this::hasFullMembership), "cluster could not get back to full membership");
            sim.schedule(1, () -> writeMarker(0));
            require(runFor(60_000, () -> marker != null), "cluster did not accept a write");
            require(runFor(60_000, this::converged), "nodes did not converge");
            checkAppliedPrefixes();
            checkLogMatching();

            ids.forEach(this::retire);
            var committed = machines.get(ids.get(0)).getStore();
            var acked = ops.stream().filter(Op::acked).count();
            assertEquals(List.of(), distinctViolations(), "seed " + seed + ": invariant violations during the run");
            RaftInvariants.assertNoDuplicates(committed);
            RaftInvariants.assertOnlyInvokedCommands(committed, invoked);
            RaftInvariants.assertAckedCommandsSurvive(committed, ops);
            RaftInvariants.assertRealTimeOrder(committed, ops);
            RaftInvariants.assertReadsLinearizable(committed, ops, reads);
            assertTrue(acked > 0, "seed " + seed + ": no write was ever acknowledged");
            assertTrue(!reads.isEmpty(), "seed " + seed + ": no read ever succeeded");
            return new Result(committed, ops.size(), acked, reads.size(), leaderByTerm.size(), confChanges,
                    snapshotsInstalled, sim.executed);
        }

        // chạy sự kiện cho tới khi done hoặc hết durationMs giây ảo; Election Safety được kiểm tra sau mỗi sự kiện
        private boolean runFor(long durationMs, BooleanSupplier done) {
            long deadline = sim.nowMs() + durationMs;
            while (!done.getAsBoolean()) {
                if (sim.nextAtMs() > deadline) {
                    return false;
                }
                try {
                    sim.runNext();
                } catch (Throwable t) {
                    violations.add("event failed at " + sim.nowMs() + "ms: " + t);
                }
                // đã có vi phạm thì dừng sớm, không cần mô phỏng tiếp
                if (violations.size() > 20) {
                    fail("seed " + seed + ": invariant violations: " + distinctViolations());
                }
                checkElectionSafety();
            }
            return true;
        }

        private void require(boolean reached, String what) {
            if (!reached) {
                fail("seed " + seed + ": " + what + " within 60s after all faults were healed; violations so far: "
                        + distinctViolations());
            }
        }

        // ---------- cluster ----------

        private void startNode(String id) {
            var machine = new SimMachine();
            var folder = dataDir.resolve(id).toString();
            var options = NodeOptions.builder()
                    .raftMetaUri(folder)
                    .logUri(folder)
                    // segment nhỏ: log đi qua nhiều file, và mỗi lần mất điện giả lập không phải chép cả file 64 MB
                    .logSegmentBytes(16 << 10)
                    .snapshotUri(folder)
                    .electionTimeoutMinMs(150)
                    .electionTimeoutMaxMs(electionTimeoutMaxMs)
                    .heartbeatIntervalMs(50)
                    // node bị gỡ vẫn chạy để nemesis có thể thêm nó lại
                    .shutdownOnRemoved(false)
                    // snapshot tự động và request nhỏ, để compact log và gửi nhiều đợt xảy ra thường xuyên
                    // node đang tắt được thêm vào sẽ không bao giờ bắt kịp: huỷ sớm để nemesis thử thay đổi khác
                    .catchUpTimeoutMs(3000)
                    // snapshot đi qua nhiều mẩu, đủ để mất mẩu và đổi leader giữa chừng xảy ra
                    .snapshotChunkBytes(256)
                    // bộ nhớ đệm log nhỏ, để follower tụt lại và node khởi động lại phải đọc entry cũ từ đĩa
                    .logCacheEntries(32)
                    .snapshotIntervalEntries(150)
                    .maxEntriesPerRequest(16)
                    .stateMachine(machine)
                    // mỗi lần khởi động node có một đồng hồ lệch khác nhau, từ chậm 25% tới nhanh 30%
                    .runtime(new SimRuntime(sim, 0.75 + random.nextDouble() * 0.55))
                    .diskFaults(operation -> {
                        if (faults && random.nextDouble() < diskFaultRate) {
                            throw new IOException("injected failure of " + operation);
                        }
                    })
                    .raftConfig(RaftConfig.builder().self(id).peers(ids).build())
                    .build();
            var node = new RaftNode(options, this);
            machines.put(id, machine);
            nodes.put(id, node);
            registry.put(id, node);
            node.start();
        }

        // bộ đếm của node mất khi nó tắt, nên cộng dồn lại trước
        private void retire(String id) {
            snapshotsInstalled += nodes.get(id).metrics().snapshotsInstalled();
        }

        private void crash(String id) {
            retire(id);
            down.add(id);
            registry.remove(id);
            nodes.get(id).shutdown();
        }

        private void powerLoss(String id) {
            retire(id);
            down.add(id);
            registry.remove(id);
            try {
                RaftInvariants.powerLoss(nodes.get(id), dataDir.resolve(id));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        // node tự dừng (ví dụ không lưu được snapshot đã load) được coi như node đã tắt, nemesis sẽ bật lại sau
        private void noticeStoppedNodes() {
            for (String id : ids) {
                if (nodes.get(id).isStopped() && down.add(id)) {
                    registry.remove(id);
                }
            }
        }

        private void restart(String id) {
            startNode(id);
            down.remove(id);
        }

        private String pick(List<String> ids) {
            return ids.get(random.nextInt(ids.size()));
        }

        private List<String> liveNodes() {
            return ids.stream().filter(id -> !down.contains(id)).toList();
        }

        private void healNetwork() {
            partition(Set.of());
        }

        // các node trong minority chỉ thấy nhau, phần còn lại chỉ thấy nhau
        private void partition(Set<String> minority) {
            for (String id : ids) {
                Set<String> side = new HashSet<>();
                for (String other : ids) {
                    if (minority.contains(id) == minority.contains(other)) {
                        side.add(other);
                    }
                }
                reachable.put(id, side);
            }
        }

        // client không biết leader nào là thật: chọn ngẫu nhiên trong các node tự nhận là leader
        private RaftNode leaderOrAny() {
            var leaders = ids.stream().filter(id -> nodes.get(id).getState() == NodeState.LEADER).toList();
            return nodes.get(pick(leaders.isEmpty() ? ids : leaders));
        }

        // ---------- mạng ----------

        private int delay() {
            if (!faults) {
                return 1;
            }
            // thỉnh thoảng một gói tin kẹt rất lâu rồi mới tới, sau cả khi cluster đã đổi leader
            if (random.nextDouble() < stragglerRate) {
                return 500 + random.nextInt(1500);
            }
            return 1 + random.nextInt(maxDelayMs);
        }

        private <T> CompletableFuture<T> call(String from, String to, Function<RaftServerService, T> invoke) {
            return callAsync(from, to, handler -> CompletableFuture.completedFuture(invoke.apply(handler)));
        }

        // request và response đều là sự kiện: có thể mất, trễ (nên đến sai thứ tự), request có thể đến thêm lần nữa
        private <T> CompletableFuture<T> callAsync(String from, String to,
                                                   Function<RaftServerService, CompletableFuture<T>> invoke) {
            var result = new CompletableFuture<T>();
            var dropRequest = faults && random.nextDouble() < dropRate;
            sim.schedule(delay(), () -> {
                var handler = registry.get(to);
                if (dropRequest || handler == null || !reachable.get(from).contains(to)) {
                    result.completeExceptionally(new IOException(from + " cannot reach " + to));
                    return;
                }
                if (faults && random.nextDouble() < duplicateRate) {
                    // bản sao đến muộn hơn hẳn, sau cả những request gửi sau nó
                    sim.schedule(random.nextInt(maxDuplicateDelayMs), () -> deliverDuplicate(to, invoke));
                }
                CompletableFuture<T> answer;
                try {
                    answer = invoke.apply(handler);
                } catch (RuntimeException e) {
                    result.completeExceptionally(e);
                    return;
                }
                answer.whenComplete((response, error) -> {
                    if (error != null || (faults && random.nextDouble() < dropRate)) {
                        // handler đã chạy xong nhưng người gửi không bao giờ biết
                        sim.schedule(delay(), () -> result.completeExceptionally(new IOException("response dropped")));
                    } else {
                        sim.schedule(delay(), () -> result.complete(response));
                    }
                });
            });
            return result;
        }

        private <T> void deliverDuplicate(String to, Function<RaftServerService, T> invoke) {
            var handler = registry.get(to);
            if (handler != null) {
                try {
                    invoke.apply(handler);
                } catch (RuntimeException e) {
                    // node đã tắt
                }
            }
        }

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(String target, RequestVoteRequest req) {
            return call(req.candidateId, target, h -> h.handleRequestVoteRequest(req));
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(String target, AppendEntriesRequest req) {
            // đi qua mã hoá nhị phân như trên mạng thật: follower nhận các entry giải mã từ khung của leader và ghi thẳng
            // các khung đó vào log, nên mô phỏng kiểm tra cả đường đó
            byte[] wire = onTheWire(req);
            return callAsync(req.leaderId, target, h -> {
                try {
                    var received = (AppendEntriesRequest) com.namnv.rpc.RpcCodec.decode(java.nio.ByteBuffer.wrap(wire), 0).message();
                    return h.handleAppendEntriesAsync(received);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
        }

        private static byte[] onTheWire(Object message) {
            try {
                var bytes = new java.io.ByteArrayOutputStream();
                com.namnv.rpc.RpcCodec.write(new java.io.DataOutputStream(bytes), 0, message);
                return bytes.toByteArray();
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }

        @Override
        public CompletableFuture<PreVoteResponse> preVote(String target, PreVoteRequest req) {
            return call(req.candidateId, target, h -> h.handlePreVoteRequest(req));
        }

        @Override
        public CompletableFuture<InstallSnapshotResponse> installSnapshot(String target, InstallSnapshotRequest req) {
            return call(req.getLeaderId(), target, h -> h.handleInstallSnapshotRequest(req));
        }

        @Override
        public CompletableFuture<TimeoutNowResponse> timeoutNow(String target, TimeoutNowRequest req) {
            return call(req.leaderId, target, h -> h.handleTimeoutNowRequest(req));
        }

        @Override
        public CompletableFuture<ReadIndexResponse> readIndex(String target, ReadIndexRequest req) {
            return callAsync(req.requesterId, target, h -> h.handleReadIndexRequest(req));
        }

        // ---------- actors ----------

        private void nemesis() {
            if (!actorsRunning) {
                return;
            }
            noticeStoppedNodes();
            switch (random.nextInt(14)) {
                case 0, 1 -> healNetwork();
                // mất điện đúng leader, lúc nó có nhiều thứ chưa kịp fsync nhất
                case 12, 13 -> {
                    var leader = leaderOrAny().getNodeId();
                    if (down.size() < 2 && !down.contains(leader)) {
                        powerLoss(leader);
                    }
                }
                case 2 -> partition(Set.of(pick(ids)));
                // hai lần chọn có thể trùng nhau, khi đó minority chỉ có một node
                case 3 -> partition(new HashSet<>(List.of(pick(ids), pick(ids))));
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
                case 6 -> nodes.get(pick(liveNodes())).createSnapshot();
                // cô lập đúng leader: nó còn nhận lệnh của client một lúc, tạo ra đuôi log chưa commit bị ghi đè sau này
                case 7 -> partition(Set.of(leaderOrAny().getNodeId()));
                case 8, 9 -> removeRandomMember();
                default -> addMissingMember();
            }
            sim.schedule(200 + random.nextInt(500), this::nemesis);
        }

        // gỡ một thành viên bất kỳ (kể cả leader), nhưng luôn giữ ít nhất 3 thành viên
        private void removeRandomMember() {
            var leader = leaderOrAny();
            var members = leader.getConf().getOldNodes();
            if (members.size() > 3 && leader.onLeavePeerCluster(pick(members))) {
                confChanges++;
            }
        }

        private void addMissingMember() {
            var leader = leaderOrAny();
            var members = leader.getConf().getOldNodes();
            var missing = ids.stream().filter(id -> !members.contains(id)).toList();
            if (!missing.isEmpty() && leader.onJoinPeerCluster(pick(missing))) {
                confChanges++;
            }
        }

        private boolean hasFullMembership() {
            for (String id : ids) {
                var node = nodes.get(id);
                var conf = node.getConf();
                if (node.getState() == NodeState.LEADER && !conf.isJoint() && conf.getOldNodes().containsAll(ids)) {
                    return true;
                }
            }
            return false;
        }

        private void restoreMembership() {
            if (!hasFullMembership()) {
                addMissingMember();
                sim.schedule(100, this::restoreMembership);
            }
        }

        // mỗi client gửi lần lượt từng lệnh; không biết kết quả thì gửi lại đúng (clientId, sequence) đó tới khi được xác nhận
        private void clientStep(int clientId, int n) {
            if (!actorsRunning) {
                return;
            }
            var command = "c" + clientId + "-" + n;
            invoked.add(command);
            attempt(clientId, n, command, ++stamp);
        }

        private void attempt(int clientId, int n, String command, long invokedAt) {
            if (!actorsRunning) {
                ops.add(new Op(command, invokedAt, Long.MAX_VALUE));
                return;
            }
            var finished = new boolean[1];
            // kết thúc lần gửi này đúng một lần: khi có kết quả, hoặc khi client hết kiên nhẫn sau 2 giây
            Function<Boolean, Void> finish = ok -> {
                if (!finished[0]) {
                    finished[0] = true;
                    if (ok) {
                        ops.add(new Op(command, invokedAt, ++stamp));
                        sim.schedule(1 + random.nextInt(10), () -> clientStep(clientId, n + 1));
                    } else {
                        sim.schedule(20, () -> attempt(clientId, n, command, invokedAt));
                    }
                }
                return null;
            };
            leaderOrAny().appendClientCommand("client-" + clientId, n + 1, command.getBytes(StandardCharsets.UTF_8))
                    .whenComplete((ok, error) -> finish.apply(Boolean.TRUE.equals(ok)));
            sim.schedule(2000, () -> finish.apply(false));
        }

        // đọc nhất quán liên tục từ một node bất kỳ, leader hay follower
        private void readerStep() {
            if (!actorsRunning) {
                return;
            }
            var target = nodes.get(pick(ids));
            var machine = machines.get(target.getNodeId());
            var invokedAt = ++stamp;
            target.read(machine::getStore).whenComplete((seen, error) -> {
                if (seen != null) {
                    reads.add(new Read(invokedAt, ++stamp, seen.size(), seen.isEmpty() ? null : seen.get(seen.size() - 1)));
                }
            });
            sim.schedule(5 + random.nextInt(40), this::readerStep);
        }

        // ghi một lệnh đánh dấu cho tới khi được xác nhận
        private void writeMarker(int attempt) {
            if (marker != null) {
                return;
            }
            var command = "final-" + attempt;
            invoked.add(command);
            leaderOrAny().appendClientCommand(command.getBytes(StandardCharsets.UTF_8)).whenComplete((ok, error) -> {
                if (Boolean.TRUE.equals(ok) && marker == null) {
                    marker = command;
                }
            });
            sim.schedule(200, () -> writeMarker(attempt + 1));
        }

        private boolean converged() {
            var reference = machines.get(ids.get(0)).getStore();
            return reference.contains(marker)
                    && ids.stream().allMatch(id -> machines.get(id).getStore().equals(reference));
        }

        // ---------- bất biến ----------

        // Election Safety: mỗi term có nhiều nhất một leader
        private void checkElectionSafety() {
            for (String id : ids) {
                var node = nodes.get(id);
                if (node.isStopped() || node.getState() != NodeState.LEADER) {
                    continue;
                }
                var term = node.getPersistent().getCurrentTerm();
                var previous = leaderByTerm.putIfAbsent(term, id);
                if (previous != null && !previous.equals(id)) {
                    violations.add("two leaders in term " + term + ": " + previous + " and " + id);
                }
            }
        }

        private void heavyChecks() {
            checkAppliedPrefixes();
            checkLogMatching();
            if (actorsRunning) {
                // so sánh cả log và state machine tốn thời gian theo độ dài, nên chạy dài thì kiểm tra thưa hơn
                sim.schedule(Math.max(100, seconds * 1000L / 300), this::heavyChecks);
            }
        }

        private void checkAppliedPrefixes() {
            var stores = new HashMap<String, List<String>>();
            for (String id : ids) {
                stores.put(id, machines.get(id).getStore());
            }
            RaftInvariants.checkAppliedPrefixes(stores, violations);
        }

        private void checkLogMatching() {
            var logs = new HashMap<String, List<LogEntry>>();
            for (String id : ids) {
                logs.put(id, nodes.get(id).getPersistent().getLogStore().readFrom(1));
            }
            RaftInvariants.checkLogMatching(logs, violations);
        }

        private List<String> distinctViolations() {
            return violations.stream().distinct().limit(5).toList();
        }
    }
}

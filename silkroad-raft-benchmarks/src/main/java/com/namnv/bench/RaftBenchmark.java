package com.namnv.bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.namnv.config.NodeOptions;
import com.namnv.config.RaftConfig;
import com.namnv.core.Closure;
import com.namnv.core.NodeState;
import com.namnv.core.RaftNode;
import com.namnv.core.Status;
import com.namnv.entity.LogEntry;
import com.namnv.rpc.RpcCodec;
import com.namnv.transport.InMemoryRpcClient;
import com.namnv.rpc.RpcProcessor;
import com.namnv.transport.SocketRpcClient;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.transport.SocketRpcServer;
import com.namnv.statemachine.StateMachine;
import com.namnv.statemachine.snapshot.SnapshotReader;
import com.namnv.statemachine.snapshot.SnapshotWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Đo thông lượng và độ trễ của một cluster 3 node trên máy này. Không phải test, chạy bằng tay:
 * <pre>
 * mvn -q test-compile
 * java -cp "target/test-classes:target/classes:$(mvn -q dependency:build-classpath -Dmdep.includeScope=test -Dmdep.outputFile=/dev/stdout)" \
 *      com.namnv.bench.RaftBenchmark [thư mục trên đĩa thật] [thư mục trên tmpfs]
 * </pre>
 * Mỗi cấu hình chạy 1 giây làm nóng rồi đo trong BENCH_SECONDS giây (mặc định 3).
 */
public class RaftBenchmark {

    private static final int MEASURE_SECONDS = Integer.getInteger("bench.seconds", 3);
    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(2);

    // state machine rẻ nhất có thể, để con số phản ánh Raft chứ không phải ứng dụng
    static class CountingMachine implements StateMachine {
        final AtomicLong applied = new AtomicLong();

        @Override
        public void onApply(String node, LogEntry entry) {
            applied.incrementAndGet();
        }

        @Override
        public void onSnapshotSave(SnapshotWriter writer, Closure done) {
            try {
                Files.writeString(Path.of(writer.getPath(), "count"), Long.toString(applied.get()));
                writer.addFile("count");
                done.run(Status.OK());
            } catch (IOException e) {
                done.run(Status.ERROR(e.getMessage()));
            }
        }

        @Override
        public boolean onSnapshotLoad(SnapshotReader reader) {
            try {
                applied.set(Long.parseLong(Files.readString(Path.of(reader.getPath(), "count"))));
                return true;
            } catch (IOException e) {
                return false;
            }
        }
    }

    record Result(long operations, long failures, double seconds, long p50Micros, long p99Micros, long maxMicros) {
        long perSecond() {
            return Math.round(operations / seconds);
        }
    }

    // ---------- cluster ----------

    static class Cluster implements AutoCloseable {
        final List<RaftNode> nodes = new ArrayList<>();
        final List<CountingMachine> machines = new ArrayList<>();
        private final List<AutoCloseable> resources = new ArrayList<>();

        Cluster(boolean tcp, Path dataDir) throws Exception {
            deleteRecursively(dataDir);
            var ids = new ArrayList<String>();
            for (int i = 0; i < 3; i++) {
                ids.add(tcp ? "localhost:" + freePort() : "node" + i);
            }
            var inMemory = new InMemoryRpcClient();
            var reachable = new ConcurrentHashMap<String, Set<String>>();
            ids.forEach(id -> reachable.put(id, Set.copyOf(ids)));
            inMemory.setReachable(reachable);
            for (int i = 0; i < ids.size(); i++) {
                var id = ids.get(i);
                var machine = new CountingMachine();
                var folder = dataDir.resolve("n" + i).toString();
                // mỗi node một client riêng, như khi chạy ở các tiến trình khác nhau
                RpcProcessor rpc = inMemory;
                if (tcp) {
                    var client = new SocketRpcClient(2000);
                    resources.add(client);
                    rpc = client;
                }
                var node = new RaftNode(NodeOptions.builder()
                        .raftMetaUri(folder).logUri(folder).snapshotUri(folder)
                        .electionTimeoutMinMs(1000).electionTimeoutMaxMs(2000).heartbeatIntervalMs(100)
                        .clientTimeoutMs(10_000)
                        .snapshotIntervalEntries(Long.getLong("bench.snapshotInterval", 200_000))
                        .commitIndexFlushIntervalMs(Integer.getInteger("bench.commitFlushMs", 1000))
                        .stateMachine(machine)
                        .raftConfig(RaftConfig.builder().self(id).peers(ids).build())
                        .build(), rpc);
                nodes.add(node);
                machines.add(machine);
                if (tcp) {
                    var server = new SocketRpcServer(Integer.parseInt(id.split(":")[1]), node);
                    server.start();
                    resources.add(server::stop);
                } else {
                    inMemory.register(id, node);
                }
            }
            nodes.forEach(RaftNode::start);
        }

        // leader đã commit được entry của chính nó
        RaftNode awaitLeader() throws Exception {
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                for (RaftNode node : nodes) {
                    if (node.getState() == NodeState.LEADER
                            && Boolean.TRUE.equals(node.appendClientCommand(new byte[1]).get(5, TimeUnit.SECONDS))) {
                        return node;
                    }
                }
                Thread.sleep(50);
            }
            throw new IllegalStateException("no leader");
        }

        @Override
        public void close() throws Exception {
            nodes.forEach(RaftNode::shutdown);
            for (AutoCloseable resource : resources) {
                resource.close();
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (Files.exists(path)) {
            try (var walk = Files.walk(path)) {
                for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(p);
                }
            }
        }
    }

    // ---------- tải ----------

    /**
     * Vòng kín: mỗi client gửi một thao tác, chờ xong rồi gửi thao tác kế tiếp.
     */
    static Result run(int clients, Supplier<java.util.concurrent.CompletableFuture<?>> operation) throws Exception {
        return runLoops(clients, () -> operation);
    }

    // như run(), nhưng mỗi vòng kín được cấp một thao tác riêng (ví dụ gắn với một client riêng)
    static Result runLoops(int clients, Supplier<Supplier<java.util.concurrent.CompletableFuture<?>>> operationPerLoop)
            throws Exception {
        long warmupEnd = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        long end = warmupEnd + TimeUnit.SECONDS.toNanos(MEASURE_SECONDS);
        long[] samples = new long[8_000_000];
        long[] finishedAt = new long[samples.length];
        var count = new AtomicInteger();
        var failures = new AtomicLong();
        var finished = new CountDownLatch(clients);
        for (int c = 0; c < clients; c++) {
            fire(operationPerLoop.get(), warmupEnd, end, samples, finishedAt, count, failures, finished);
        }
        if (!finished.await(MEASURE_SECONDS + 60, TimeUnit.SECONDS)) {
            throw new IllegalStateException("benchmark clients did not finish");
        }
        int n = Math.min(count.get(), samples.length);
        if (Boolean.getBoolean("bench.timeline")) {
            printTimeline(samples, finishedAt, n, warmupEnd);
        }
        long[] sorted = Arrays.copyOf(samples, n);
        Arrays.sort(sorted);
        // count đếm mọi thao tác xong trong thời gian đo; mẫu độ trễ chỉ giữ tối đa samples.length cái đầu
        return new Result(count.get(), failures.get(), MEASURE_SECONDS,
                n == 0 ? 0 : sorted[n / 2] / 1000, n == 0 ? 0 : sorted[(int) (n * 0.99)] / 1000,
                n == 0 ? 0 : sorted[n - 1] / 1000);
    }

    // -Dbench.timeline=true: số thao tác và độ trễ lớn nhất trong từng khoảng 100 ms
    private static void printTimeline(long[] samples, long[] finishedAt, int n, long warmupEnd) {
        int buckets = MEASURE_SECONDS * 10;
        long[] worst = new long[buckets];
        int[] done = new int[buckets];
        for (int i = 0; i < n; i++) {
            int bucket = (int) Math.min(buckets - 1, Math.max(0, (finishedAt[i] - warmupEnd) / 100_000_000L));
            worst[bucket] = Math.max(worst[bucket], samples[i]);
            done[bucket]++;
        }
        var line = new StringBuilder("  timeline (max ms / nghìn ops mỗi 100 ms):");
        for (int b = 0; b < buckets; b++) {
            line.append(String.format(" %d/%d", worst[b] / 1_000_000, done[b] / 1000));
        }
        System.out.println(line);
    }

    private static void fire(Supplier<java.util.concurrent.CompletableFuture<?>> operation, long warmupEnd, long end,
                             long[] samples, long[] finishedAt, AtomicInteger count, AtomicLong failures, CountDownLatch finished) {
        long start = System.nanoTime();
        if (start >= end) {
            finished.countDown();
            return;
        }
        operation.get().whenComplete((value, error) -> {
            long now = System.nanoTime();
            boolean ok = error == null && !Boolean.FALSE.equals(value);
            if (!ok) {
                failures.incrementAndGet();
                // không gửi lại ngay để không quay vòng tại chỗ khi cluster đang lỗi
                SCHEDULER.schedule(() -> fire(operation, warmupEnd, end, samples, finishedAt, count, failures, finished),
                        10, TimeUnit.MILLISECONDS);
                return;
            }
            if (start >= warmupEnd && now <= end) {
                int slot = count.getAndIncrement();
                if (slot < samples.length) {
                    samples[slot] = now - start;
                    finishedAt[slot] = now;
                }
            }
            // sang thread khác để lời gọi kế tiếp không lồng trong callback của lời gọi này
            SCHEDULER.execute(() -> fire(operation, warmupEnd, end, samples, finishedAt, count, failures, finished));
        });
    }

    // ---------- các phép đo ----------

    private static void header(String title) {
        System.out.println();
        System.out.println("## " + title);
    }

    private static void row(String label, Result r) {
        System.out.printf("| %-34s | %9d | %8d | %8d | %8d | %5d |%n",
                label, r.perSecond(), r.p50Micros(), r.p99Micros(), r.maxMicros(), r.failures());
    }

    private static void tableHeader(String first) {
        System.out.printf("| %-34s | %9s | %8s | %8s | %8s | %5s |%n", first, "ops/giây", "p50 µs", "p99 µs", "max µs", "lỗi");
        System.out.println("|" + "-".repeat(36) + "|" + "-".repeat(11) + "|" + "-".repeat(10) + "|" + "-".repeat(10) + "|"
                + "-".repeat(10) + "|" + "-".repeat(7) + "|");
    }

    static void fsyncCost(String name, Path dir) throws IOException {
        Files.createDirectories(dir);
        Path file = dir.resolve("fsync-probe");
        int rounds = 300;
        byte[] block = new byte[256];
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            long[] nanos = new long[rounds];
            for (int i = 0; i < rounds; i++) {
                channel.write(ByteBuffer.wrap(block));
                long start = System.nanoTime();
                channel.force(false);
                nanos[i] = System.nanoTime() - start;
            }
            Arrays.sort(nanos);
            System.out.printf("| %-14s | %8d | %8d |%n", name, nanos[rounds / 2] / 1000, nanos[(int) (rounds * 0.99)] / 1000);
        }
        Files.delete(file);
    }

    static void codec() throws IOException {
        header("Mã hoá một AppendEntries (64 entry)");
        System.out.printf("| %-12s | %-18s | %10s | %14s | %14s |%n", "payload", "định dạng", "kích thước", "mã hoá/giây", "giải mã/giây");
        System.out.println("|--------------|--------------------|------------|----------------|----------------|");
        var json = new ObjectMapper();
        for (int payload : new int[]{128, 4096}) {
            var entries = new ArrayList<LogEntry>();
            byte[] command = new byte[payload];
            new java.util.Random(1).nextBytes(command);
            for (int i = 0; i < 64; i++) {
                entries.add(new LogEntry(i + 1, 3, command, "client-1", i + 1));
            }
            var request = new AppendEntriesRequest(3, "localhost:8080", 0, 0, entries, 0);

            var frame = new ByteArrayOutputStream();
            RpcCodec.write(new DataOutputStream(frame), 1, request);
            byte[] binary = frame.toByteArray();
            long encode = perSecond(() -> {
                var out = new ByteArrayOutputStream(binary.length);
                RpcCodec.write(new DataOutputStream(out), 1, request);
            });
            long decode = perSecond(() -> RpcCodec.read(new DataInputStream(new ByteArrayInputStream(binary))));
            System.out.printf("| %-12s | %-18s | %10d | %14d | %14d |%n", payload + " byte", "nhị phân (hiện tại)", binary.length, encode, decode);

            // JSON của cùng danh sách entry, như transport và log của các phiên bản trước, để so sánh
            byte[] text = json.writeValueAsBytes(entries);
            long jsonEncode = perSecond(() -> json.writeValueAsBytes(entries));
            long jsonDecode = perSecond(() -> json.readValue(text, LogEntry[].class));
            System.out.printf("| %-12s | %-18s | %10d | %14d | %14d |%n", payload + " byte", "JSON (Jackson)", text.length, jsonEncode, jsonDecode);
        }
    }

    interface IoTask {
        void run() throws IOException;
    }

    private static long perSecond(IoTask task) throws IOException {
        long warmupEnd = System.nanoTime() + 300_000_000L;
        while (System.nanoTime() < warmupEnd) {
            task.run();
        }
        long start = System.nanoTime();
        long end = start + 700_000_000L;
        long count = 0;
        while (System.nanoTime() < end) {
            task.run();
            count++;
        }
        return Math.round(count / ((System.nanoTime() - start) / 1e9));
    }

    public static void main(String[] args) throws Exception {
        Path disk = Path.of(args.length > 0 ? args[0] : "target/bench-data");
        Path tmpfs = Path.of(args.length > 1 ? args[1] : "/tmp/silkroad-raft-bench");
        System.out.println("# Silk Road Raft benchmark: 3 node trên một máy, đo " + MEASURE_SECONDS + " giây mỗi cấu hình");

        header("Chi phí một lần fsync (µs)");
        System.out.printf("| %-14s | %8s | %8s |%n", "nơi lưu", "p50", "p99");
        System.out.println("|----------------|----------|----------|");
        fsyncCost("đĩa thật", disk);
        fsyncCost("tmpfs", tmpfs);

        // -Dbench.quick=true: chỉ đo đường ghi lệnh nhỏ trên đĩa thật, để so sánh nhanh trước và sau một thay đổi
        boolean quick = Boolean.getBoolean("bench.quick");
        if (!quick) {
            codec();
        }

        header("Ghi: transport × nơi lưu × kích thước lệnh × số client đồng thời");
        tableHeader("cấu hình");
        for (boolean tcp : new boolean[]{true, false}) {
            for (Path dir : quick ? List.of(disk) : List.of(disk, tmpfs)) {
                for (int payload : quick ? new int[]{128} : new int[]{128, 4096}) {
                    try (var cluster = new Cluster(tcp, dir.resolve("write-" + (tcp ? "tcp" : "mem") + "-" + payload))) {
                        var leader = cluster.awaitLeader();
                        byte[] command = new byte[payload];
                        for (int clients : quick && Boolean.getBoolean("bench.timeline") ? new int[]{512} : new int[]{1, 32, 512}) {
                            var result = run(clients, () -> leader.appendClientCommand(command));
                            row((tcp ? "TCP" : "in-memory") + ", " + (dir == disk ? "đĩa" : "tmpfs") + ", " + payload + "B, "
                                    + clients + " client", result);
                        }
                    }
                }
            }
        }

        if (quick) {
            finish(disk, tmpfs);
        }
        header("Đọc nhất quán (TCP, đĩa thật, không có lệnh ghi xen kẽ)");
        tableHeader("cấu hình");
        try (var cluster = new Cluster(true, disk.resolve("read"))) {
            var leader = cluster.awaitLeader();
            var follower = cluster.nodes.stream().filter(n -> n != leader).findFirst().orElseThrow();
            var machine = cluster.machines.get(cluster.nodes.indexOf(follower));
            for (int clients : new int[]{1, 32, 512}) {
                row("đọc trên leader, " + clients + " client", run(clients, () -> leader.read(machine.applied::get)));
            }
            for (int clients : new int[]{1, 32, 512}) {
                row("đọc trên follower, " + clients + " client", run(clients, () -> follower.read(machine.applied::get)));
            }
        }

        finish(disk, tmpfs);
    }

    private static void finish(Path disk, Path tmpfs) throws IOException {
        // node vừa tắt có thể còn việc ghi đĩa đang xếp hàng
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        deleteRecursively(disk);
        deleteRecursively(tmpfs);
        SCHEDULER.shutdownNow();
        System.exit(0);
    }
}

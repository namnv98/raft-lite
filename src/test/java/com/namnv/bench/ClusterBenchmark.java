package com.namnv.bench;

import com.namnv.rpc.client.RaftClient;
import com.namnv.agent.AgentLoop;
import com.namnv.kv.KvCommands;
import com.namnv.rpc.client.MessageTransport;
import com.namnv.rpc.client.SocketRpcClient;
import com.namnv.rpc.nio.NioRpcClient;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;

/**
 * Đo một cluster 3 node chạy thành ba tiến trình riêng trên máy này, với tải đi từ tiến trình thứ tư (chính nó) qua TCP.
 * Không có lời gọi hàm nội bộ nào giữa client và node hay giữa các node. Chạy bằng tay:
 * <pre>
 * mvn -q test-compile
 * java -cp "target/test-classes:target/classes:$(mvn -q dependency:build-classpath -Dmdep.includeScope=test -Dmdep.outputFile=/dev/stdout)" \
 *      com.namnv.bench.ClusterBenchmark [thư mục dữ liệu]
 * </pre>
 * -Dbench.seconds=3: thời gian đo mỗi cấu hình. -Dbench.connections=16: số kết nối TCP mà các client dùng chung tới mỗi node.
 * -Dbench.logSync=false: các node không fsync log (NodeOptions.logSync).
 * -Dbench.logPreallocate=false: không cấp phát sẵn file segment (NodeOptions.logPreallocate).
 * -Dbench.transport=socket: transport cũ thay cho NIO trên vòng của node. -Dbench.clientLoops=2: số vòng NIO phía client.
 * -Draft.agent.idle=busy (truyền cho cả node qua -Dbench.nodeArgs): vòng không bao giờ ngủ, như BusySpinIdleStrategy.
 * -Dbench.payloads=128,4096, -Dbench.clients=1,32,512 và -Dbench.phases=write,read: chỉ đo một phần của bảng.
 */
public class ClusterBenchmark {

    private static final int CONNECTIONS = Integer.getInteger("bench.connections", 16);
    private static final String TRANSPORT = System.getProperty("bench.transport", "nio");
    private static final int CLIENT_LOOPS = Integer.getInteger("bench.clientLoops", 2);
    // -Dbench.kv=lmdb|rocksdb: các node chạy kho KV; ghi là put với key 8 byte tăng dần trong 1 triệu key và value
    // dài payload byte, đọc là get một key, như công cụ benchmark của etcd (put --sequential-keys, range foo)
    private static final String STORE = System.getProperty("bench.kv", "");
    private static final boolean KV = !STORE.isEmpty() && !"false".equals(STORE);
    private static final java.util.concurrent.atomic.AtomicLong NEXT_KEY = new java.util.concurrent.atomic.AtomicLong();
    private static final byte[] READ_QUERY = KvCommands.get("foo".getBytes(java.nio.charset.StandardCharsets.UTF_8));

    private static byte[] writeCommand(byte[] payload) {
        if (!KV) {
            return payload;
        }
        var key = java.nio.ByteBuffer.allocate(Long.BYTES).putLong(NEXT_KEY.getAndIncrement() % 1_000_000).array();
        return KvCommands.put(key, payload);
    }

    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of(args.length > 0 ? args[0] : "target/bench-cluster").toAbsolutePath();
        deleteRecursively(dataDir);
        Files.createDirectories(dataDir);

        var servers = new ArrayList<String>();
        for (int i = 0; i < 3; i++) {
            servers.add("localhost:" + freePort());
        }
        var processes = new ArrayList<Process>();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> processes.forEach(Process::destroyForcibly)));
        var java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        for (int i = 0; i < 3; i++) {
            var log = dataDir.resolve("node" + i + ".log").toFile();
            // -Dbench.nodeArgs="...": tham số JVM thêm cho các tiến trình node; log GC của từng node luôn được ghi lại
            var command = new ArrayList<String>(List.of(java, "-Xlog:gc:file=" + dataDir.resolve("gc" + i + ".log")));
            command.add("-Dbench.logSync=" + System.getProperty("bench.logSync", "true"));
            command.add("-Dbench.transport=" + TRANSPORT);
            command.add("-Dbench.kv=" + STORE);
            if (System.getProperty("bench.kvSync") != null) {
                command.add("-Dbench.kvSync=" + System.getProperty("bench.kvSync"));
            }
            command.add("-Dbench.logPreallocate=" + System.getProperty("bench.logPreallocate", "true"));
            // snapshot của kho KV chép toàn bộ kho ra file, nên thưa hơn
            command.add("-Dbench.snapshotInterval=" + System.getProperty("bench.snapshotInterval", KV ? "1000000" : "200000"));
            command.add("-Dbench.commitFlushMs=" + System.getProperty("bench.commitFlushMs", "1000"));
            var extra = System.getProperty("bench.nodeArgs", "").trim();
            if (!extra.isEmpty()) {
                command.addAll(List.of(extra.split("\\s+")));
            }
            command.addAll(List.of("-cp", System.getProperty("java.class.path"), BenchNode.class.getName(),
                    servers.get(i), String.join(",", servers), dataDir.resolve("n" + i).toString()));
            processes.add(new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log).start());
        }

        // mỗi transport là một bộ kết nối TCP riêng tới các node; các client chia nhau dùng.
        // Với nio, các transport chia nhau CLIENT_LOOPS vòng (mỗi vòng một thread, như agent phía client của Aeron)
        var loops = new ArrayList<AgentLoop>();
        for (int i = 0; "nio".equals(TRANSPORT) && i < CLIENT_LOOPS; i++) {
            loops.add(new AgentLoop("bench-client-" + i));
        }
        var transports = new ArrayList<MessageTransport>();
        for (int i = 0; i < CONNECTIONS; i++) {
            transports.add(loops.isEmpty() ? new SocketRpcClient(2000) : new NioRpcClient(loops.get(i % loops.size()), 2000));
        }
        var clientCounter = new AtomicInteger();
        Supplier<RaftClient> newClient = () -> {
            int n = clientCounter.getAndIncrement();
            return new RaftClient(transports.get(n % transports.size()), servers, "bench-client-" + n, 30_000);
        };

        try {
            var probe = newClient.get();
            probe.write(new byte[1]).get(60, TimeUnit.SECONDS);
            var pids = processes.stream().map(p -> String.valueOf(p.pid())).toList();
            System.out.println("# Raft Lite: 3 node là 3 tiến trình (pid " + String.join(", ", pids)
                    + "), client ở tiến trình thứ tư, tất cả qua TCP trên loopback, transport " + TRANSPORT
                    + ", idle " + System.getProperty("raft.agent.idle", "backoff") + (KV ? ", kho KV " + STORE + ", kvSync " + System.getProperty("bench.kvSync", "theo log") : ", state machine chỉ đếm"));
            System.out.println("# đo " + Integer.getInteger("bench.seconds", 3) + " giây mỗi cấu hình, "
                    + CONNECTIONS + " kết nối TCP dùng chung tới mỗi node, dữ liệu ở " + dataDir);

            System.out.println();
            System.out.println("## Ghi qua mạng (mỗi client có clientId riêng, có chống ghi trùng)");
            printHeader();
            boolean quick = Boolean.getBoolean("bench.quick");
            int[] payloads = ints("bench.payloads", quick ? "128" : "128,4096");
            int[] clientCounts = ints("bench.clients", quick ? "512" : "1,32,512");
            var phases = List.of(System.getProperty("bench.phases", quick ? "write" : "write,read").split(","));
            for (int payload : phases.contains("write") ? payloads : new int[0]) {
                byte[] command = new byte[payload];
                for (int clients : clientCounts) {
                    var pool = new ArrayList<RaftClient>();
                    for (int c = 0; c < clients; c++) {
                        pool.add(newClient.get());
                    }
                    // mỗi lời gọi của vòng đo thuộc về một client cố định, để sequence của nó tăng liền nhau
                    var result = runPerClient(pool, client -> client.write(writeCommand(command)));
                    printRow("ghi " + payload + "B, " + clients + " client", result);
                }
            }

            if (!phases.contains("read")) {
                return;
            }
            System.out.println();
            System.out.println("## Đọc nhất quán qua mạng");
            printHeader();
            var leader = findLeader(probe, servers);
            var follower = servers.stream().filter(s -> !s.equals(leader)).findFirst().orElseThrow();
            for (int clients : clientCounts) {
                var pool = new ArrayList<RaftClient>();
                for (int c = 0; c < clients; c++) {
                    pool.add(newClient.get());
                }
                byte[] query = KV ? READ_QUERY : new byte[0];
                printRow("đọc qua leader, " + clients + " client", runPerClient(pool, client -> client.readFrom(leader, query)));
                printRow("đọc qua follower, " + clients + " client", runPerClient(pool, client -> client.readFrom(follower, query)));
            }
        } finally {
            transports.forEach(MessageTransport::close);
            processes.forEach(Process::destroyForcibly);
            for (Process process : processes) {
                process.waitFor(10, TimeUnit.SECONDS);
            }
            int problems = 0;
            for (int i = 0; i < 3; i++) {
                File log = dataDir.resolve("node" + i + ".log").toFile();
                problems += (int) Files.readAllLines(log.toPath()).stream()
                        .filter(line -> line.contains(" ERROR ") || line.contains("Exception")).count();
            }
            System.out.println();
            for (int i = 0; i < 3; i++) {
                var pauses = Files.readAllLines(dataDir.resolve("gc" + i + ".log")).stream()
                        .filter(line -> line.contains("Pause"))
                        .map(line -> line.substring(line.lastIndexOf(' ') + 1).replace("ms", "").replace(',', '.'))
                        .mapToDouble(Double::parseDouble).toArray();
                System.out.printf("GC của node %d: %d lần dừng, lâu nhất %.1f ms, tổng %.0f ms%n", i, pauses.length,
                        DoubleStream.of(pauses).max().orElse(0), DoubleStream.of(pauses).sum());
            }
            System.out.println("dòng ERROR/Exception trong log của ba node: " + problems);
            // -Dbench.keep=true: giữ lại log của các node để xem sau
            if (!Boolean.getBoolean("bench.keep")) {
                deleteRecursively(dataDir);
            }
            System.out.flush();
            // các thread nền của benchmark không tự dừng
            Runtime.getRuntime().halt(0);
        }
    }

    // node nào trả lời lần đọc nhanh nhất mà không cần hỏi ai chính là leader; ở đây chỉ cần biết một node không phải leader,
    // nên dùng cách đơn giản: ghi một lệnh rồi xem client đang nhắm tới node nào
    private static String findLeader(RaftClient probe, List<String> servers) throws Exception {
        for (String server : servers) {
            var writer = new RaftClient(List.of(server), "leader-probe-" + server, 500, 700);
            try {
                if (writer.write(new byte[1]).get(2, TimeUnit.SECONDS)) {
                    return server;
                }
            } catch (Exception notLeader) {
                // node này không phải leader
            } finally {
                writer.close();
            }
        }
        throw new IllegalStateException("no leader found");
    }

    private static RaftBenchmark.Result runPerClient(List<RaftClient> pool,
                                                     java.util.function.Function<RaftClient, CompletableFuture<?>> operation)
            throws Exception {
        var index = new AtomicInteger();
        // RaftBenchmark.run khởi động đúng pool.size() vòng kín; mỗi vòng giữ một client riêng cho mọi lời gọi của nó
        return RaftBenchmark.runLoops(pool.size(), () -> {
            var client = pool.get(index.getAndIncrement());
            return () -> operation.apply(client);
        });
    }

    private static int[] ints(String property, String fallback) {
        return java.util.Arrays.stream(System.getProperty(property, fallback).split(",")).mapToInt(Integer::parseInt).toArray();
    }

    private static void printHeader() {
        System.out.printf("| %-30s | %9s | %8s | %8s | %8s | %5s |%n", "cấu hình", "TPS", "p50 µs", "p99 µs", "max µs", "lỗi");
        System.out.println("|" + "-".repeat(32) + "|" + "-".repeat(11) + "|" + "-".repeat(10) + "|" + "-".repeat(10) + "|"
                + "-".repeat(10) + "|" + "-".repeat(7) + "|");
    }

    private static void printRow(String label, RaftBenchmark.Result r) {
        System.out.printf("| %-30s | %9d | %8d | %8d | %8d | %5d |%n",
                label, r.perSecond(), r.p50Micros(), r.p99Micros(), r.maxMicros(), r.failures());
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
}

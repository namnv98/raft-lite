package com.namnv.ledger.bench;

import com.namnv.ledger.client.LedgerClient;
import com.namnv.ledger.gateway.LedgerGateway;
import com.namnv.ledger.gateway.TransferBatcher;
import com.namnv.ledger.model.LedgerAccount;
import com.namnv.ledger.model.LedgerResult;
import com.namnv.ledger.model.LedgerTransfer;
import com.namnv.agent.AgentLoop;
import com.namnv.rpc.MessageTransport;
import com.namnv.transport.nio.NioRpcClient;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * Cùng một cụm sổ cái ba node (ba tiến trình), mỗi request là một giao dịch lẻ, đo ba cách gửi:
 * <ol>
 * <li>nhị phân: {@link LedgerClient#transfer} trực tiếp tới cụm, mỗi giao dịch một lệnh Raft;</li>
 * <li>HTTP, không gom: qua {@link LedgerGateway} (tiến trình riêng), mỗi request một lệnh Raft;</li>
 * <li>HTTP, gom lô: như trên nhưng cổng gom các request đồng thời bằng {@link TransferBatcher}.</li>
 * </ol>
 * Client chạy vòng kín (gửi, chờ trả lời, gửi tiếp), mỗi client một virtual thread và (với HTTP) một kết nối keep-alive,
 * Idempotency-Key mới cho mỗi giao dịch.
 * Tham số: thư mục dữ liệu. -Dgateway.clients=1,64,512 -Dgateway.seconds=3 -Dgateway.modes=binary,http,http-batch
 * -Dgateway.maxBatch=1000 -Dgateway.maxInflight=4, và các -Dledger.* của {@link LocalCluster}.
 * -Dgateway.modes=serve -Dgateway.serveSeconds=120: chỉ dựng cụm, tài khoản và hai cổng (in "READY http=... http-batch=...")
 * rồi chờ, để đo bằng wrk (bench/wrk/run.sh).
 */
public final class GatewayBenchmark {
    private static final int USD = 840;
    private static final long BANK = 1;
    private static final long FIRST_CUSTOMER = 2;
    private static final int CUSTOMERS = 10_000;

    private final LocalCluster cluster;
    private final List<MessageTransport> transports = new ArrayList<>();
    private final AtomicLong nextId = new AtomicLong(1L << 40);

    private GatewayBenchmark(LocalCluster cluster) {
        this.cluster = cluster;
        var loops = List.of(new AgentLoop("gateway-bench-0"), new AgentLoop("gateway-bench-1"));
        for (int i = 0; i < 8; i++) {
            transports.add(new NioRpcClient(loops.get(i % loops.size()), 2000));
        }
    }

    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of(args.length > 0 ? args[0] : "target/gateway-bench").toAbsolutePath();
        int exit = 0;
        var cluster = new LocalCluster(dataDir);
        try {
            new GatewayBenchmark(cluster).run();
        } catch (Throwable t) {
            t.printStackTrace();
            exit = 1;
        } finally {
            cluster.close();
            System.out.flush();
            Runtime.getRuntime().halt(exit);
        }
    }

    private void run() throws Exception {
        var admin = new LedgerClient(transports.getFirst(), cluster.servers, 30_000);
        var accounts = new ArrayList<LedgerAccount>();
        accounts.add(new LedgerAccount(BANK, USD, 0));
        for (long id = FIRST_CUSTOMER; id < FIRST_CUSTOMER + CUSTOMERS; id++) {
            accounts.add(new LedgerAccount(id, USD, 0));
        }
        for (int from = 0; from < accounts.size(); from += 1000) {
            var results = admin.createAccounts(accounts.subList(from, Math.min(accounts.size(), from + 1000)))
                    .get(60, TimeUnit.SECONDS);
            require(results.stream().allMatch(LedgerResult::succeeded), "account creation failed: " + results);
        }
        System.out.printf("# Sổ cái: 3 node (3 tiến trình), cổng HTTP là tiến trình thứ tư, client ở tiến trình thứ năm; "
                + "mỗi request một giao dịch; fsync %s%n", System.getProperty("ledger.logSync", "true"));
        System.out.println();
        System.out.printf("| %-22s | %8s | %11s | %8s | %8s | %8s | %8s | %-18s |%n",
                "cách gửi", "client", "giao dịch/s", "p50 ms", "p99 ms", "max ms", "lô TB", "CPU client/cổng/node");
        System.out.println("|" + "-".repeat(24) + "|" + "-".repeat(10) + "|" + "-".repeat(13) + "|"
                + ("-".repeat(10) + "|").repeat(4) + "-".repeat(20) + "|");
        int seconds = Integer.getInteger("gateway.seconds", 3);
        var clientCounts = Arrays.stream(System.getProperty("gateway.clients", "1,64,512").split(","))
                .mapToInt(Integer::parseInt).toArray();
        for (String mode : System.getProperty("gateway.modes", "binary,http,http-batch").split(",")) {
            switch (mode) {
                case "binary" -> {
                    for (int clients : clientCounts) {
                        var client = new LedgerClient(transports.get(clients % transports.size()), cluster.servers, 30_000);
                        measure("nhị phân, từng lệnh", clients, seconds, () -> {
                            var result = client.transfer(transfer()).get(30, TimeUnit.SECONDS);
                            return result == LedgerResult.OK;
                        }, null);
                    }
                }
                case "http", "http-batch" -> {
                    boolean batching = mode.equals("http-batch");
                    try (var gateway = new GatewayProcess(batching)) {
                        // mỗi client giữ một kết nối keep-alive riêng (như connection pool của một dịch vụ gọi tới)
                        var connections = new java.util.concurrent.ConcurrentLinkedQueue<RawHttp>();
                        var connection = ThreadLocal.withInitial(() -> {
                            var c = new RawHttp(gateway.port);
                            connections.add(c);
                            return c;
                        });
                        // vòng đầu (64 client, 2 giây, không in) để JIT của tiến trình cổng nóng lên
                        for (int clients : IntStream.concat(IntStream.of(-64), Arrays.stream(clientCounts)).toArray()) {
                            measure(batching ? "HTTP, gom lô" : "HTTP, từng lệnh", clients, clients < 0 ? 2 : seconds, () -> {
                                var t = transfer();
                                String body = "{\"debitAccountId\":" + t.debitAccountId() + ",\"creditAccountId\":"
                                        + t.creditAccountId() + ",\"amount\":" + t.amount() + ",\"ledger\":" + t.ledger() + "}";
                                int status = connection.get().post("/transfers", "bench-" + t.id(), body);
                                if (status != 201) {
                                    throw new IllegalStateException("HTTP " + status);
                                }
                                return true;
                            }, batching ? gateway : null);
                        }
                        connections.forEach(RawHttp::close);
                    }
                }
                case "serve" -> {
                    // chỉ dựng cụm và hai cổng rồi chờ, để đo bằng công cụ ngoài (wrk, xem bench/wrk)
                    try (var single = new GatewayProcess(false); var batched = new GatewayProcess(true)) {
                        System.out.printf("READY http=%d http-batch=%d%n", single.port, batched.port);
                        System.out.flush();
                        Thread.sleep(TimeUnit.SECONDS.toMillis(Integer.getInteger("gateway.serveSeconds", 120)));
                        long[] counters = batched.counters();
                        System.out.printf("cổng gom lô: %,d giao dịch trong %,d lô (TB %.1f)%n", counters[1], counters[0],
                                (double) counters[1] / Math.max(1, counters[0]));
                    }
                }
                default -> throw new IllegalArgumentException("unknown mode " + mode);
            }
        }
        var totals = admin.totals().get(30, TimeUnit.SECONDS);
        require(totals.balanced(), "books are not balanced: " + totals);
        for (String server : cluster.servers) {
            require(admin.totalsFrom(server).get(30, TimeUnit.SECONDS).equals(totals), "replica " + server + " differs");
        }
        System.out.println();
        System.out.printf("kiểm tra: %,d giao dịch đã ghi; sổ cân; ba bản sao giống nhau%n", totals.transfers());
    }

    private LedgerTransfer transfer() {
        long id = nextId.getAndIncrement();
        long from = FIRST_CUSTOMER + Math.floorMod(id * 7919, CUSTOMERS);
        long to = FIRST_CUSTOMER + Math.floorMod(id * 104_729 + 1, CUSTOMERS);
        if (from == to) {
            to = from == FIRST_CUSTOMER ? FIRST_CUSTOMER + 1 : FIRST_CUSTOMER;
        }
        return new LedgerTransfer(id, from, to, 1, USD);
    }

    private static long ownCpuNanos() {
        return ((com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean())
                .getProcessCpuTime();
    }

    private void measure(String label, int clients, int seconds, Callable<Boolean> request, GatewayProcess gateway)
            throws Exception {
        boolean print = clients > 0;
        clients = Math.abs(clients);
        long warmupEnd = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        long end = warmupEnd + TimeUnit.SECONDS.toNanos(seconds);
        long[] latencies = new long[8_000_000];
        long[] before = gateway != null ? gateway.counters() : null;
        long wallStart = System.nanoTime();
        long[] cpuBefore = {ownCpuNanos(), gateway != null ? gateway.cpuNanos() : 0, cluster.cpuNanos()};
        var count = new AtomicInteger();
        var failed = new AtomicLong();
        var firstError = new java.util.concurrent.atomic.AtomicReference<String>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int c = 0; c < clients; c++) {
                executor.submit(() -> {
                    while (true) {
                        long start = System.nanoTime();
                        if (start >= end) {
                            return null;
                        }
                        boolean ok;
                        try {
                            ok = request.call();
                        } catch (Exception e) {
                            firstError.compareAndSet(null, e.toString());
                            ok = false;
                        }
                        long now = System.nanoTime();
                        if (start >= warmupEnd && now <= end) {
                            if (!ok) {
                                failed.incrementAndGet();
                            } else {
                                int slot = count.getAndIncrement();
                                if (slot < latencies.length) {
                                    latencies[slot] = now - start;
                                }
                            }
                        }
                    }
                });
            }
        }
        if (!print) {
            return;
        }
        double wall = System.nanoTime() - wallStart;
        String cpu = String.format("%.1f / %s / %.1f", (ownCpuNanos() - cpuBefore[0]) / wall,
                gateway != null ? String.format("%.1f", (gateway.cpuNanos() - cpuBefore[1]) / wall) : "-",
                (cluster.cpuNanos() - cpuBefore[2]) / wall);
        int n = count.get();
        String batch = "1";
        if (gateway != null) {
            long[] after = gateway.counters();
            batch = String.format("%.1f", (double) (after[1] - before[1]) / Math.max(1, after[0] - before[0]));
        }
        long[] sorted = Arrays.copyOf(latencies, Math.min(n, latencies.length));
        Arrays.sort(sorted);
        int m = sorted.length;
        System.out.printf("| %-22s | %8d | %,11d | %8.2f | %8.2f | %8.2f | %8s | %-18s |%n", label, clients, n / seconds,
                m == 0 ? 0 : sorted[m / 2] / 1e6, m == 0 ? 0 : sorted[(int) (m * 0.99)] / 1e6, m == 0 ? 0 : sorted[m - 1] / 1e6,
                batch, cpu);
        if (failed.get() > 0) {
            System.out.printf("  (%,d request lỗi, ví dụ: %s)%n", failed.get(), firstError.get());
        }
    }

    /**
     * Client HTTP/1.1 tối giản trên một socket keep-alive: ghi request, đọc dòng trạng thái, header và body theo
     * Content-Length. Client HTTP của JDK tốn nhiều CPU tới mức chính nó giới hạn số request/s của phép đo.
     */
    private static final class RawHttp {
        private final java.net.Socket socket;
        private final java.io.OutputStream out;
        private final java.io.InputStream in;
        private final byte[] line = new byte[8192];

        RawHttp(int port) {
            try {
                socket = new java.net.Socket("localhost", port);
                socket.setTcpNoDelay(true);
                out = new java.io.BufferedOutputStream(socket.getOutputStream(), 4096);
                in = new java.io.BufferedInputStream(socket.getInputStream(), 8192);
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }

        int post(String path, String idempotencyKey, String body) throws java.io.IOException {
            byte[] payload = body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.write(("POST " + path + " HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nIdempotency-Key: "
                    + idempotencyKey + "\r\nContent-Length: " + payload.length + "\r\n\r\n")
                    .getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            out.write(payload);
            out.flush();
            String statusLine = readLine();
            int status = Integer.parseInt(statusLine.substring(9, 12));
            int length = 0;
            for (String header = readLine(); !header.isEmpty(); header = readLine()) {
                if (header.regionMatches(true, 0, "Content-Length:", 0, 15)) {
                    length = Integer.parseInt(header.substring(15).trim());
                }
            }
            in.skipNBytes(length);
            return status;
        }

        private String readLine() throws java.io.IOException {
            int n = 0;
            for (int b = in.read(); b != '\n'; b = in.read()) {
                if (b < 0) {
                    throw new java.io.EOFException("connection closed");
                }
                if (b != '\r') {
                    line[n++] = (byte) b;
                }
            }
            return new String(line, 0, n, java.nio.charset.StandardCharsets.US_ASCII);
        }

        void close() {
            try {
                socket.close();
            } catch (java.io.IOException ignored) {
                // đã xong
            }
        }
    }

    /** {@link LedgerGateway} chạy thành tiến trình riêng, như khi triển khai thật */
    private final class GatewayProcess implements AutoCloseable {
        final int port;
        private final Process process;

        GatewayProcess(boolean batching) throws Exception {
            port = LocalCluster.freePort();
            var java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
            process = new ProcessBuilder(java, "-Xms1g", "-Xmx1g",
                    "-Dgateway.batch=" + batching,
                    "-Dgateway.maxBatch=" + Integer.getInteger("gateway.maxBatch", 1000),
                    "-Dgateway.maxInflight=" + Integer.getInteger("gateway.maxInflight", 4),
                    "-Dgateway.eventLoops=" + Integer.getInteger("gateway.eventLoops",
                            Math.max(1, Runtime.getRuntime().availableProcessors() / 4)),
                    "-cp", System.getProperty("java.class.path"),
                    LedgerGateway.class.getName(), String.valueOf(port), String.join(",", cluster.servers))
                    .redirectErrorStream(true)
                    .redirectOutput(cluster.dataDir.resolve("gateway-" + (batching ? "batch" : "single") + ".log").toFile())
                    .start();
            Runtime.getRuntime().addShutdownHook(new Thread(process::destroyForcibly));
            try (var http = HttpClient.newHttpClient()) {
                var totals = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/totals"))
                        .timeout(Duration.ofSeconds(5)).build();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (true) {
                    try {
                        if (http.send(totals, HttpResponse.BodyHandlers.discarding()).statusCode() == 200) {
                            break;
                        }
                    } catch (Exception e) {
                        // chưa lắng nghe
                    }
                    require(System.nanoTime() < deadline && process.isAlive(), "gateway did not start");
                    Thread.sleep(100);
                }
            }
        }

        /** {số lô, số giao dịch} mà cổng đã gom */
        long[] counters() throws Exception {
            try (var http = HttpClient.newHttpClient()) {
                var body = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/stats")).build(),
                        HttpResponse.BodyHandlers.ofString()).body();
                var json = new com.fasterxml.jackson.databind.ObjectMapper().readTree(body);
                return new long[]{json.get("batches").asLong(), json.get("transfers").asLong()};
            }
        }

        long cpuNanos() {
            return process.info().totalCpuDuration().map(Duration::toNanos).orElse(0L);
        }

        @Override
        public void close() throws Exception {
            process.destroy();
            process.waitFor(10, TimeUnit.SECONDS);
            process.destroyForcibly();
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}

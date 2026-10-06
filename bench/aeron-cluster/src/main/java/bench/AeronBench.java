package bench;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.Header;
import io.aeron.samples.cluster.ClusterConfig;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Cùng kịch bản với ClusterBenchmark của Raft Lite, nhưng trên Aeron Cluster: 3 node là 3 tiến trình trên máy này,
 * tiến trình này là client. "N client đồng thời" ở đây là N lệnh đang chờ xác nhận cùng lúc trên một phiên client,
 * mỗi lệnh được gửi tiếp ngay khi lệnh trước của nó được xác nhận (vòng kín), như bên Raft Lite.
 * Tham số: thư mục dữ liệu | mức fsync. -Dbench.seconds=5, -Dbench.payloads=128,4096, -Dbench.clients=1,32,512.
 */
public class AeronBench implements EgressListener {
    private long[] samples = new long[16_000_000];
    private int count;
    private int inflight;
    private long measureFrom;
    private long measureTo;

    @Override
    public void onMessage(long clusterSessionId, long timestamp, DirectBuffer buffer, int offset, int length, Header header) {
        long now = System.nanoTime();
        long sentAt = buffer.getLong(offset);
        inflight--;
        if (sentAt >= measureFrom && now <= measureTo && count < samples.length) {
            samples[count++] = now - sentAt;
        }
    }

    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of(args[0]).toAbsolutePath();
        String syncLevel = args.length > 1 ? args[1] : "0";
        int seconds = Integer.getInteger("bench.seconds", 5);
        deleteRecursively(dataDir);
        Files.createDirectories(dataDir);

        var java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        var processes = new ArrayList<Process>();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> processes.forEach(Process::destroyForcibly)));
        for (int i = 0; i < 3; i++) {
            var command = new ArrayList<>(List.of(java,
                    "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
                    "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
                    "--add-opens", "java.base/java.util.zip=ALL-UNNAMED",
                    "-Xlog:gc:file=" + dataDir.resolve("gc" + i + ".log")));
            command.add("-Dbench.tuned=" + Boolean.getBoolean("bench.tuned"));
            var extra = System.getProperty("bench.nodeArgs", "").trim();
            if (!extra.isEmpty()) {
                command.addAll(List.of(extra.split("\\s+")));
            }
            command.addAll(List.of("-cp", System.getProperty("java.class.path"), AeronNode.class.getName(),
                    String.valueOf(i), dataDir.toString(), syncLevel));
            processes.add(new ProcessBuilder(command).redirectErrorStream(true)
                    .redirectOutput(dataDir.resolve("node" + i + ".log").toFile()).start());
        }

        var bench = new AeronBench();
        int exit = 0;
        boolean tuned = Boolean.getBoolean("bench.tuned");
        var driverContext = new MediaDriver.Context().dirDeleteOnStart(true).dirDeleteOnShutdown(true);
        if (tuned) {
            driverContext.threadingMode(io.aeron.driver.ThreadingMode.SHARED)
                    .sharedIdleStrategy(new org.agrona.concurrent.BusySpinIdleStrategy());
        }
        try (MediaDriver driver = MediaDriver.launchEmbedded(driverContext);
             AeronCluster cluster = connect(bench, driver)) {
            System.out.println("# Aeron Cluster " + System.getProperty("bench.aeronVersion", "") + ": 3 node là 3 tiến trình, "
                    + "client ở tiến trình thứ tư, UDP trên loopback, " + (tuned ? "cấu hình độ trễ thấp (busy-spin)" : "cấu hình mặc định") + ", mức fsync " + syncLevel + ", đo " + seconds + " giây mỗi cấu hình");
            System.out.printf("| %-30s | %9s | %8s | %8s | %8s |%n", "cấu hình", "TPS", "p50 µs", "p99 µs", "max µs");
            System.out.println("|" + "-".repeat(32) + "|" + "-".repeat(11) + "|" + "-".repeat(10) + "|" + "-".repeat(10) + "|"
                    + "-".repeat(10) + "|");
            for (int payload : ints("bench.payloads", "128,4096")) {
                for (int clients : ints("bench.clients", "1,32,512")) {
                    bench.run(cluster, payload, clients, seconds);
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
            exit = 1;
        } finally {
            processes.forEach(Process::destroyForcibly);
            for (Process process : processes) {
                process.waitFor(10, TimeUnit.SECONDS);
            }
            int problems = 0;
            for (int i = 0; i < 3; i++) {
                problems += (int) Files.readAllLines(dataDir.resolve("node" + i + ".log")).stream()
                        .filter(line -> line.contains("Exception") || line.contains("ERROR")).count();
            }
            System.out.println("dòng ERROR/Exception trong log của ba node: " + problems);
            if (!Boolean.getBoolean("bench.keep")) {
                deleteRecursively(dataDir);
            }
            System.out.flush();
            Runtime.getRuntime().halt(exit);
        }
    }

    // cluster cần vài giây để bầu leader; thử kết nối lại tới khi được
    private static AeronCluster connect(AeronBench listener, MediaDriver driver) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (true) {
            try {
                return AeronCluster.connect(new AeronCluster.Context()
                        .egressListener(listener)
                        .egressChannel("aeron:udp?endpoint=localhost:0")
                        .aeronDirectoryName(driver.aeronDirectoryName())
                        .ingressChannel("aeron:udp")
                        .ingressEndpoints(ClusterConfig.ingressEndpoints(
                                AeronNode.HOSTS, AeronNode.PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET)));
            } catch (RuntimeException e) {
                if (System.nanoTime() > deadline) {
                    throw e;
                }
                Thread.sleep(500);
            }
        }
    }

    private void run(AeronCluster cluster, int payload, int window, int seconds) {
        var buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(payload));
        // để mọi lệnh của cấu hình trước được xác nhận xong
        long drainUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (inflight > 0 && System.nanoTime() < drainUntil) {
            cluster.pollEgress();
        }
        inflight = 0;
        count = 0;
        measureFrom = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        measureTo = measureFrom + TimeUnit.SECONDS.toNanos(seconds);
        long nextKeepAlive = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        long now;
        while ((now = System.nanoTime()) < measureTo) {
            while (inflight < window) {
                buffer.putLong(0, System.nanoTime());
                if (cluster.offer(buffer, 0, payload) < 0) {
                    break; // bị đẩy lùi: xử lý các xác nhận đang chờ rồi thử lại
                }
                inflight++;
            }
            if (cluster.pollEgress() == 0) {
                Thread.onSpinWait();
            }
            if (now > nextKeepAlive) {
                cluster.sendKeepAlive();
                nextKeepAlive = now + TimeUnit.SECONDS.toNanos(1);
            }
        }
        long[] sorted = Arrays.copyOf(samples, count);
        Arrays.sort(sorted);
        int n = sorted.length;
        System.out.printf("| %-30s | %9d | %8d | %8d | %8d |%n", "ghi " + payload + "B, " + window + " client",
                Math.round(n / (double) seconds), n == 0 ? 0 : sorted[n / 2] / 1000, n == 0 ? 0 : sorted[(int) (n * 0.99)] / 1000,
                n == 0 ? 0 : sorted[n - 1] / 1000);
    }

    private static int[] ints(String property, String fallback) {
        return Arrays.stream(System.getProperty(property, fallback).split(",")).mapToInt(Integer::parseInt).toArray();
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

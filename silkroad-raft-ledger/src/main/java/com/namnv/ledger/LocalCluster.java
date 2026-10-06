package com.namnv.ledger;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Ba {@link LedgerNode} chạy thành ba tiến trình trên máy này, cho các benchmark. Đọc các tham số -Dledger.* của tiến trình
 * hiện tại (xem {@link LedgerBenchmark}) và truyền xuống các node.
 */
final class LocalCluster implements AutoCloseable {
    final Path dataDir;
    final List<String> servers = new ArrayList<>();
    private final List<Process> processes = new ArrayList<>();

    LocalCluster(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        deleteRecursively(dataDir);
        Files.createDirectories(dataDir);
        boolean nodeIps = Boolean.getBoolean("ledger.nodeIps");
        for (int i = 0; i < 3; i++) {
            servers.add((nodeIps ? "127.0.1." + (i + 1) : "localhost") + ":" + freePort());
        }
        var java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> processes.forEach(Process::destroyForcibly)));
        for (int i = 0; i < 3; i++) {
            String heap = System.getProperty("ledger.nodeHeap", "2g");
            var command = new ArrayList<>(List.of(java, "-Xms" + heap, "-Xmx" + heap, "-XX:+AlwaysPreTouch",
                    "-Dledger.expectedAccounts=" + (Integer.getInteger("ledger.accounts", 100_000) + 16),
                    "-Dledger.expectedTransfers=" + System.getProperty("ledger.expectedTransfers", "32000000"),
                    "-Dledger.logSync=" + System.getProperty("ledger.logSync", "true"),
                    "-Dledger.bindLocal=" + nodeIps,
                    "-Dledger.snapshotInterval=" + System.getProperty("ledger.snapshotInterval", "100000"),
                    "-cp", System.getProperty("java.class.path")));
            // -Dledger.nodeArgs="...": tham số JVM thêm cho các node (ví dụ để ghi JFR)
            var extra = System.getProperty("ledger.nodeArgs", "").trim();
            if (!extra.isEmpty()) {
                command.addAll(List.of(extra.split("\\s+")));
            }
            command.addAll(List.of(LedgerNode.class.getName(),
                    servers.get(i), String.join(",", servers), dataDir.resolve("n" + i).toString()));
            processes.add(new ProcessBuilder(command).redirectErrorStream(true)
                    .redirectOutput(dataDir.resolve("node" + i + ".log").toFile()).start());
        }
    }

    /** thời gian CPU (ns) mà các node đã dùng */
    long cpuNanos() {
        return processes.stream().mapToLong(p -> p.info().totalCpuDuration().map(java.time.Duration::toNanos).orElse(0L)).sum();
    }

    @Override
    public void close() throws IOException {
        processes.forEach(Process::destroyForcibly);
        if (!Boolean.getBoolean("ledger.keep")) {
            deleteRecursively(dataDir);
        }
    }

    static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    static void deleteRecursively(Path path) throws IOException {
        if (Files.exists(path)) {
            try (var walk = Files.walk(path)) {
                for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(p);
                }
            }
        }
    }
}

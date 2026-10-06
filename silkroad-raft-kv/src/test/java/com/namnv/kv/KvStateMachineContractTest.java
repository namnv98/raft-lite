package com.namnv.kv;

import com.namnv.config.NodeOptions;
import com.namnv.config.RaftConfig;
import com.namnv.core.NodeState;
import com.namnv.core.RaftClientService;
import com.namnv.core.RaftNode;
import com.namnv.core.Status;
import com.namnv.core.ThreadedRuntime;
import com.namnv.entity.LogEntry;
import com.namnv.client.RaftClient;
import com.namnv.transport.nio.NioRpcClient;
import com.namnv.transport.nio.NioRpcServer;
import com.namnv.statemachine.snapshot.SnapshotReader;
import com.namnv.statemachine.snapshot.SnapshotWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Hợp đồng chung của các kho KV: mỗi kho (LMDB, RocksDB) chạy cùng một bộ test qua một lớp con.
 */
abstract class KvStateMachineContractTest {

    /** kho mới, rỗng, trong {@code dir} */
    protected abstract BufferedKvStateMachine newMachine(Path dir, boolean sync, long batchIntervalMs, int batchLimit);

    @TempDir
    Path dir;

    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            closeables.get(i).close();
        }
    }

    private BufferedKvStateMachine machine(String name) {
        // lô dài để test chủ động quyết định lúc nào dữ liệu vào kho
        var machine = newMachine(dir.resolve(name), false, 60_000, 1_000_000);
        closeables.add(machine);
        return machine;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private long index;

    private void apply(BufferedKvStateMachine machine, byte[] command) {
        machine.onApply("n", new LogEntry(++index, 1, command));
    }

    private static String get(BufferedKvStateMachine machine, String key) {
        var value = machine.get(bytes(key));
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    @Test
    void readsSeeEveryAppliedCommandWhetherOrNotItReachedLmdb() {
        var kv = machine("a");
        apply(kv, KvCommands.put(bytes("k1"), bytes("v1")));
        apply(kv, KvCommands.put(bytes("k2"), bytes("v2")));
        assertEquals("v1", get(kv, "k1")); // chỉ mới trong bộ đệm
        kv.flushNow();
        assertEquals("v1", get(kv, "k1")); // đã vào LMDB

        apply(kv, KvCommands.put(bytes("k1"), bytes("v1b")));
        apply(kv, KvCommands.delete(bytes("k2")));
        assertEquals("v1b", get(kv, "k1"));
        assertNull(get(kv, "k2")); // xoá trong bộ đệm che value cũ trong LMDB
        kv.flushNow();
        assertEquals("v1b", get(kv, "k1"));
        assertNull(get(kv, "k2"));
        assertNull(get(kv, "missing"));

        // value rỗng thật khác với key bị xoá
        apply(kv, KvCommands.put(bytes("empty"), new byte[0]));
        assertArrayEquals(new byte[0], kv.get(bytes("empty")));
        kv.flushNow();
        assertArrayEquals(new byte[0], kv.get(bytes("empty")));
    }

    @Test
    void queryAnswersGetCommands() {
        var kv = machine("q");
        apply(kv, KvCommands.put(bytes("k"), bytes("v")));
        assertArrayEquals(bytes("v"), KvCommands.value(kv.query(KvCommands.get(bytes("k")))));
        assertNull(KvCommands.value(kv.query(KvCommands.get(bytes("other")))));
    }

    @Test
    void malformedAndForeignCommandsAreIgnored() {
        var kv = machine("m");
        apply(kv, bytes("not a kv command"));
        apply(kv, new byte[]{KvCommands.PUT, 0, 50, 1}); // độ dài key vượt quá lệnh
        apply(kv, KvCommands.put(bytes("k"), bytes("v")));
        assertEquals("v", get(kv, "k"));
    }

    @Test
    void snapshotCapturesTheStateAtTheMomentItIsTaken() throws Exception {
        var source = machine("src");
        for (int i = 0; i < 100; i++) {
            apply(source, KvCommands.put(bytes("key" + i), bytes("old" + i)));
        }
        source.flushNow();
        // phần còn trong bộ đệm lúc chụp: ghi đè, xoá và key mới
        apply(source, KvCommands.put(bytes("key1"), bytes("new1")));
        apply(source, KvCommands.delete(bytes("key2")));
        apply(source, KvCommands.put(bytes("fresh"), bytes("f")));

        Path snapshotDir = Files.createDirectories(dir.resolve("snapshot"));
        var writer = new SnapshotWriter(snapshotDir.toString(), new ArrayList<>());
        var saved = new CompletableFuture<Status>();
        source.onSnapshotSave(writer, saved::complete);
        // lệnh apply sau lúc chụp không được lọt vào snapshot, dù đã vào LMDB trước khi file được ghi xong
        apply(source, KvCommands.put(bytes("key3"), bytes("after")));
        apply(source, KvCommands.put(bytes("late"), bytes("after")));
        source.flushNow();
        assertTrue(saved.get(10, TimeUnit.SECONDS).isOk());
        assertTrue(!writer.getFile().isEmpty(), "snapshot must register its files");

        var target = machine("dst");
        apply(target, KvCommands.put(bytes("stale"), bytes("x"))); // state cũ phải bị thay hoàn toàn
        target.flushNow();
        apply(target, KvCommands.put(bytes("stale2"), bytes("y")));
        assertTrue(target.onSnapshotLoad(new SnapshotReader(snapshotDir.toString())));

        assertEquals("new1", get(target, "key1"));
        assertNull(get(target, "key2"));
        assertEquals("old3", get(target, "key3"));
        assertEquals("old99", get(target, "key99"));
        assertEquals("f", get(target, "fresh"));
        assertNull(get(target, "late"));
        assertNull(get(target, "stale"));
        assertNull(get(target, "stale2"));
    }

    // ---------- cluster ----------

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private RaftNode startNode(List<String> servers, int i, List<BufferedKvStateMachine> machines) {
        var folder = dir.resolve("node" + i).toString();
        var runtime = new ThreadedRuntime();
        var kv = newMachine(dir.resolve("kv" + i), true, 20, 500);
        machines.set(i, kv);
        var node = new RaftNode(NodeOptions.builder()
                .raftMetaUri(folder).logUri(folder).snapshotUri(folder)
                .electionTimeoutMinMs(150).electionTimeoutMaxMs(300).heartbeatIntervalMs(50)
                .snapshotIntervalEntries(300)
                .runtime(runtime)
                .stateMachine(kv)
                .raftConfig(RaftConfig.builder().self(servers.get(i)).peers(servers).build())
                .build(), new NioRpcClient(runtime.loop(), 500));
        var server = new NioRpcServer(Integer.parseInt(servers.get(i).split(":")[1]), node,
                new RaftClientService(node, kv::query), runtime.loop());
        server.start();
        node.start();
        closeables.add(kv);
        closeables.add(node::shutdown);
        return node;
    }

    @Test
    void clusterServesKvOverTheNetworkAndRebuildsAfterRestart() throws Exception {
        var servers = new ArrayList<String>();
        for (int i = 0; i < 3; i++) {
            servers.add("localhost:" + freePort());
        }
        var machines = new ArrayList<BufferedKvStateMachine>(java.util.Collections.nCopies(3, null)); // chỗ cho từng node
        var nodes = new ArrayList<RaftNode>();
        for (int i = 0; i < 3; i++) {
            nodes.add(startNode(servers, i, machines));
        }
        var transport = new NioRpcClient(1000);
        closeables.add(transport);
        var client = new RaftClient(transport, servers, "kv-client", 15_000);

        // đủ nhiều để mỗi node snapshot vài lần và kho nhận nhiều lô. Các lệnh gửi đồng thời không có thứ tự định trước,
        // nên mỗi đợt chỉ ghi mỗi key một lần, và đợt sau chỉ bắt đầu khi đợt trước xong
        writeAll(client, 0, 700, i -> KvCommands.put(bytes("k" + i), bytes("v" + i)));
        writeAll(client, 0, 700, i -> KvCommands.put(bytes("k" + i), bytes("v" + (i + 700))));

        // một follower tắt trong khi cluster ghi tiếp đủ nhiều để leader compact mất phần log nó cần
        int follower = 0;
        while (nodes.get(follower).getState() == NodeState.LEADER) {
            follower++;
        }
        nodes.get(follower).shutdown();
        machines.get(follower).close();
        closeables.remove(machines.get(follower));
        writeAll(client, 0, 700, i -> i % 7 == 0 ? KvCommands.delete(bytes("k" + i)) : KvCommands.put(bytes("k" + i), bytes("v" + (i + 1400))));
        writeAll(client, 0, 700, i -> KvCommands.put(bytes("x" + i), bytes("x" + i)));
        // hai node còn lại vẫn là đa số và trả lời đọc nhất quán giống nhau
        for (int i = 0; i < 3; i++) {
            if (i != follower) {
                assertEquals("v1999", string(client.readFrom(servers.get(i), KvCommands.get(bytes("k599"))).get(10, TimeUnit.SECONDS)));
                assertNull(KvCommands.value(client.readFrom(servers.get(i), KvCommands.get(bytes("k7"))).get(10, TimeUnit.SECONDS)));
            }
        }

        // bật lại follower: kho của nó bị xoá lúc khởi động; phần còn thiếu chỉ có thể đến bằng snapshot gửi qua mạng
        int port = Integer.parseInt(servers.get(follower).split(":")[1]);
        // vòng của node cũ đóng cổng của nó một cách bất đồng bộ
        await("old node to release its port", () -> {
            try (var socket = new ServerSocket(port)) {
                return true;
            } catch (IOException e) {
                return false;
            }
        });
        int restarted = follower;
        var node = startNode(servers, restarted, machines);
        var kv = machines.get(restarted);
        await("restarted node to catch up", () -> node.metrics().lastApplied() >= 2800
                && "x699".equals(stringOrNull(kv.get(bytes("x699")))));
        assertTrue(node.metrics().snapshotsInstalled() > 0, "restarted node should have received a snapshot from the leader");
        assertEquals("v1999", stringOrNull(kv.get(bytes("k599"))));
        assertEquals("v1998", stringOrNull(kv.get(bytes("k598"))));
        assertNull(kv.get(bytes("k7")));
        for (String key : List.of("k123", "k0", "x0", "x350")) {
            assertEquals(string(client.readFrom(servers.get(restarted), KvCommands.get(bytes(key))).get(10, TimeUnit.SECONDS)),
                    stringOrNull(kv.get(bytes(key))));
        }
    }

    private static void writeAll(RaftClient client, int from, int to, java.util.function.IntFunction<byte[]> command)
            throws Exception {
        var pending = new ArrayList<CompletableFuture<Boolean>>();
        for (int i = from; i < to; i++) {
            pending.add(client.write(command.apply(i)));
        }
        for (var write : pending) {
            assertTrue(write.get(30, TimeUnit.SECONDS));
        }
    }

    private static String string(byte[] getResult) {
        return stringOrNull(KvCommands.value(getResult));
    }

    private static String stringOrNull(byte[] value) {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(20);
        }
        fail("Timed out waiting for: " + what);
    }
}

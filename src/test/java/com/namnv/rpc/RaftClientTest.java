package com.namnv.rpc;

import com.namnv.ListStateMachine;
import com.namnv.config.NodeOptions;
import com.namnv.config.RaftConfig;
import com.namnv.core.NodeState;
import com.namnv.core.RaftClientService;
import com.namnv.core.RaftNode;
import com.namnv.rpc.client.RaftClient;
import com.namnv.rpc.client.SocketRpcClient;
import com.namnv.rpc.server.SocketRpcServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Client đi qua TCP tới một cluster 3 node, không gọi hàm nào của node trực tiếp.
 */
class RaftClientTest {

    @TempDir
    Path dataDir;

    private final List<String> servers = new ArrayList<>();
    private final List<RaftNode> nodes = new ArrayList<>();
    private final List<ListStateMachine> machines = new ArrayList<>();
    private final List<SocketRpcServer> rpcServers = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
        nodes.forEach(RaftNode::shutdown);
        rpcServers.forEach(SocketRpcServer::stop);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void startCluster() throws IOException {
        for (int i = 0; i < 3; i++) {
            servers.add("localhost:" + freePort());
        }
        for (int i = 0; i < 3; i++) {
            var machine = new ListStateMachine();
            var folder = dataDir.resolve("node" + i).toString();
            var transport = new SocketRpcClient(500);
            closeables.add(transport);
            var node = new RaftNode(NodeOptions.builder()
                    .raftMetaUri(folder).logUri(folder).snapshotUri(folder)
                    .electionTimeoutMinMs(150).electionTimeoutMaxMs(300).heartbeatIntervalMs(50)
                    .stateMachine(machine)
                    .raftConfig(RaftConfig.builder().self(servers.get(i)).peers(servers).build())
                    .build(), transport);
            // câu hỏi duy nhất của ứng dụng mẫu: toàn bộ danh sách, mỗi lệnh một dòng
            var service = new RaftClientService(node,
                    query -> String.join("\n", machine.getStore()).getBytes(StandardCharsets.UTF_8));
            var server = new SocketRpcServer(Integer.parseInt(servers.get(i).split(":")[1]), node, null, service);
            server.start();
            nodes.add(node);
            machines.add(machine);
            rpcServers.add(server);
            node.start();
        }
    }

    private RaftClient client(String clientId) {
        var client = new RaftClient(servers, clientId, 500, 15_000);
        closeables.add(client);
        return client;
    }

    private static List<String> lines(byte[] result) {
        var text = new String(result, StandardCharsets.UTF_8);
        return text.isEmpty() ? List.of() : List.of(text.split("\n"));
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(20);
        }
        fail("Timed out waiting for: " + what);
    }

    @Test
    void writesAndReadsGoThroughTheNetwork() throws Exception {
        startCluster();
        var client = client("client-1");

        var expected = new ArrayList<String>();
        for (int i = 0; i < 30; i++) {
            expected.add("cmd" + i);
            assertTrue(client.write(("cmd" + i).getBytes(StandardCharsets.UTF_8)).get(20, TimeUnit.SECONDS));
            // lần đọc ngay sau một lệnh ghi đã được xác nhận phải thấy lệnh đó
            assertEquals(expected, lines(client.read(new byte[0]).get(20, TimeUnit.SECONDS)));
        }
        // đọc nhất quán từ từng node, kể cả follower
        for (String server : servers) {
            assertEquals(expected, lines(client.readFrom(server, new byte[0]).get(20, TimeUnit.SECONDS)));
        }
    }

    @Test
    void concurrentClientsAllGetTheirWritesApplied() throws Exception {
        startCluster();
        var pending = new ArrayList<CompletableFuture<Boolean>>();
        for (int c = 0; c < 8; c++) {
            var client = client("client-" + c);
            for (int i = 0; i < 25; i++) {
                pending.add(client.write(("c" + c + "-" + i).getBytes(StandardCharsets.UTF_8)));
            }
        }
        for (var write : pending) {
            assertTrue(write.get(30, TimeUnit.SECONDS));
        }
        var seen = lines(client("reader").read(new byte[0]).get(20, TimeUnit.SECONDS));
        assertEquals(200, seen.size());
        assertEquals(200, seen.stream().distinct().count());
    }

    @Test
    void clientFailsOverWhenTheLeaderDiesAndAppliesEachCommandOnce() throws Exception {
        startCluster();
        var client = client("client-1");
        var expected = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            expected.add("before" + i);
            assertTrue(client.write(("before" + i).getBytes(StandardCharsets.UTF_8)).get(20, TimeUnit.SECONDS));
        }

        // tắt hẳn leader (cả node lẫn cổng mạng của nó) trong khi client đang gửi tiếp
        int leader = -1;
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).getState() == NodeState.LEADER) {
                leader = i;
            }
        }
        assertTrue(leader >= 0);
        var pending = new ArrayList<CompletableFuture<Boolean>>();
        for (int i = 0; i < 5; i++) {
            expected.add("during" + i);
            pending.add(client.write(("during" + i).getBytes(StandardCharsets.UTF_8)));
        }
        rpcServers.get(leader).stop();
        nodes.get(leader).shutdown();
        for (int i = 0; i < 10; i++) {
            expected.add("after" + i);
            pending.add(client.write(("after" + i).getBytes(StandardCharsets.UTF_8)));
        }
        for (var write : pending) {
            assertTrue(write.get(30, TimeUnit.SECONDS));
        }

        // lệnh nào cũng có mặt đúng một lần, dù client đã gửi lại một số lệnh cho leader mới
        var seen = lines(client.read(new byte[0]).get(20, TimeUnit.SECONDS));
        assertEquals(expected.size(), seen.size());
        assertEquals(List.copyOf(expected).stream().sorted().toList(), seen.stream().sorted().toList());
        for (int i = 0; i < nodes.size(); i++) {
            if (i != leader) {
                var machine = machines.get(i);
                await("replica " + i + " to converge", () -> machine.getStore().equals(seen));
            }
        }
    }
}

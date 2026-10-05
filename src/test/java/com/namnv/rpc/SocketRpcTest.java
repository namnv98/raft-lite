package com.namnv.rpc;

import com.namnv.ListStateMachine;
import com.namnv.config.NodeOptions;
import com.namnv.config.RaftConfig;
import com.namnv.core.NodeState;
import com.namnv.core.RaftNode;
import com.namnv.entity.ConfigurationEntry;
import com.namnv.entity.LogEntry;
import com.namnv.rpc.client.SocketRpcClient;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.InstallSnapshotRequest;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.RequestVoteRequest;
import com.namnv.rpc.model.request.TimeoutNowRequest;
import com.namnv.rpc.model.response.AppendEntriesResponse;
import com.namnv.rpc.model.response.InstallSnapshotResponse;
import com.namnv.rpc.model.response.PreVoteResponse;
import com.namnv.rpc.model.response.RequestVoteResponse;
import com.namnv.rpc.model.response.TimeoutNowResponse;
import com.namnv.rpc.server.SocketRpcServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class SocketRpcTest {

    // trả lại đúng dữ liệu của request trong response, để kiểm tra từng lời gọi nhận đúng câu trả lời của nó
    static class EchoService implements RaftServerService {
        final AtomicInteger calls = new AtomicInteger();
        volatile long delayMs;
        volatile AppendEntriesRequest lastAppend;
        volatile InstallSnapshotRequest lastSnapshot;

        private void handle() {
            calls.incrementAndGet();
            if (delayMs > 0) {
                try {
                    TimeUnit.MILLISECONDS.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public RequestVoteResponse handleRequestVoteRequest(RequestVoteRequest req) {
            handle();
            return new RequestVoteResponse(req.term, true);
        }

        @Override
        public AppendEntriesResponse handleAppendEntriesRequest(AppendEntriesRequest req) {
            handle();
            lastAppend = req;
            return new AppendEntriesResponse(req.term, true, req.prevLogIndex + req.entries.size());
        }

        @Override
        public PreVoteResponse handlePreVoteRequest(PreVoteRequest req) {
            handle();
            return new PreVoteResponse(req.term, false);
        }

        @Override
        public InstallSnapshotResponse handleInstallSnapshotRequest(InstallSnapshotRequest req) {
            handle();
            lastSnapshot = req;
            return new InstallSnapshotResponse(req.getTerm(), true);
        }

        @Override
        public TimeoutNowResponse handleTimeoutNowRequest(TimeoutNowRequest req) {
            handle();
            return new TimeoutNowResponse(req.term, true);
        }
    }

    // class "độc": chỉ cần được deserialize là đã chạy code
    static class Bomb implements java.io.Serializable {
        static final AtomicInteger detonations = new AtomicInteger();

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            detonations.incrementAndGet();
        }
    }

    @TempDir
    Path dataDir;

    private final List<SocketRpcServer> servers = new ArrayList<>();
    private final List<SocketRpcClient> clients = new ArrayList<>();
    private final List<RaftNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(RaftNode::shutdown);
        clients.forEach(SocketRpcClient::close);
        servers.forEach(SocketRpcServer::stop);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private SocketRpcServer startServer(int port, RaftServerService service) {
        var server = new SocketRpcServer(port, service);
        servers.add(server);
        server.start();
        return server;
    }

    private SocketRpcClient client(int timeoutMs) {
        var client = new SocketRpcClient(timeoutMs);
        clients.add(client);
        return client;
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
    void everyRpcTypeRoundTrips() throws Exception {
        var service = new EchoService();
        var port = freePort();
        startServer(port, service);
        var address = "localhost:" + port;
        var rpc = client(1000);

        assertTrue(rpc.requestVote(address, new RequestVoteRequest(3, "A", 7, 2)).get(5, TimeUnit.SECONDS).voteGranted);
        assertFalse(rpc.preVote(address, new PreVoteRequest(4, "A", 7, 2)).get(5, TimeUnit.SECONDS).voteGranted);
        assertTrue(rpc.timeoutNow(address, new TimeoutNowRequest(5, "A")).get(5, TimeUnit.SECONDS).success);

        // entry thường, no-op và config entry đều phải đi qua được bộ lọc deserialization
        var conf = new ConfigurationEntry(List.of("A", "B"), List.of("A", "B", "C"), true);
        var entries = List.of(
                new LogEntry(8, 6, "hello".getBytes(StandardCharsets.UTF_8)),
                new LogEntry(9, 6, null),
                LogEntry.newConfigurationEntry(10, 6, conf));
        var append = rpc.appendEntries(address, new AppendEntriesRequest(6, "A", 7, 2, entries, 5)).get(5, TimeUnit.SECONDS);
        assertEquals(10, append.matchIndex);
        assertEquals("hello", new String(service.lastAppend.entries.get(0).getCommand(), StandardCharsets.UTF_8));
        assertEquals(conf, service.lastAppend.entries.get(2).getConfiguration());
        // heartbeat rỗng
        assertEquals(7, rpc.appendEntries(address, new AppendEntriesRequest(6, "A", 7, 2, List.of(), 5))
                .get(5, TimeUnit.SECONDS).matchIndex);

        var snapshot = new InstallSnapshotRequest(7, "A", 10, 6, conf, Map.of("snapshot.data", new byte[]{1, 2, 3}),
                new HashMap<>(Map.of("client-1", 7L)));
        assertTrue(rpc.installSnapshot(address, snapshot).get(5, TimeUnit.SECONDS).isSuccess());
        assertArrayEquals(new byte[]{1, 2, 3}, service.lastSnapshot.getFiles().get("snapshot.data"));
        assertEquals(conf, service.lastSnapshot.getConf());
        assertEquals(Map.of("client-1", 7L), service.lastSnapshot.getSessions());
    }

    @Test
    void concurrentCallsGetTheirOwnResponses() throws Exception {
        var port = freePort();
        startServer(port, new EchoService());
        var rpc = client(2000);

        // mọi lời gọi dùng chung một kết nối: response không được lẫn sang lời gọi khác
        var calls = new ArrayList<CompletableFuture<RequestVoteResponse>>();
        for (int term = 0; term < 200; term++) {
            calls.add(rpc.requestVote("localhost:" + port, new RequestVoteRequest(term, "A", 0, 0)));
        }
        for (int term = 0; term < calls.size(); term++) {
            assertEquals(term, calls.get(term).get(10, TimeUnit.SECONDS).term);
        }
    }

    @Test
    void callFailsWhenNobodyListensAndRecoversWhenServerComesBack() throws Exception {
        var service = new EchoService();
        var port = freePort();
        var address = "localhost:" + port;
        var rpc = client(500);

        assertThrows(ExecutionException.class,
                () -> rpc.preVote(address, new PreVoteRequest(1, "A", 0, 0)).get(5, TimeUnit.SECONDS));

        var server = startServer(port, service);
        assertEquals(1, rpc.preVote(address, new PreVoteRequest(1, "A", 0, 0)).get(5, TimeUnit.SECONDS).term);

        // server khởi động lại: kết nối cũ của client đã chết, lời gọi kế tiếp phải tự nối lại
        server.stop();
        startServer(port, service);
        assertEquals(2, rpc.preVote(address, new PreVoteRequest(2, "A", 0, 0)).get(5, TimeUnit.SECONDS).term);
    }

    @Test
    void slowPeerTimesOutWithoutPoisoningLaterCalls() throws Exception {
        var service = new EchoService();
        var port = freePort();
        startServer(port, service);
        var address = "localhost:" + port;
        var rpc = client(200);

        service.delayMs = 1500;
        assertThrows(ExecutionException.class,
                () -> rpc.requestVote(address, new RequestVoteRequest(1, "A", 0, 0)).get(5, TimeUnit.SECONDS));

        // response muộn của lời gọi trước không được trả về cho lời gọi sau
        service.delayMs = 0;
        assertEquals(42, rpc.requestVote(address, new RequestVoteRequest(42, "A", 0, 0)).get(5, TimeUnit.SECONDS).term);
    }

    @Test
    void serverRejectsForeignClassesAndUnknownRequests() throws Exception {
        var service = new EchoService();
        var port = freePort();
        startServer(port, service);

        // class ngoài danh sách cho phép bị bộ lọc chặn, object lạ trong danh sách thì không phải request
        for (Object payload : List.of(new File("/etc/passwd"), new ArrayList<>(List.of("not a request")))) {
            try (Socket socket = new Socket("localhost", port)) {
                socket.setSoTimeout(2000);
                var out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(payload);
                out.flush();
                var in = new ObjectInputStream(socket.getInputStream());
                assertThrows(IOException.class, in::readObject, "server answered a " + payload.getClass().getSimpleName());
            }
        }
        assertEquals(0, service.calls.get());

        // server vẫn phục vụ bình thường sau đó
        assertEquals(9, client(1000).requestVote("localhost:" + port, new RequestVoteRequest(9, "A", 0, 0))
                .get(5, TimeUnit.SECONDS).term);
    }

    @Test
    void foreignClassIsRejectedBeforeItsCodeRuns() throws Exception {
        var port = freePort();
        startServer(port, new EchoService());

        try (Socket socket = new Socket("localhost", port)) {
            socket.setSoTimeout(2000);
            var out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(new Bomb());
            out.flush();
            assertThrows(IOException.class, () -> new ObjectInputStream(socket.getInputStream()).readObject());
        }
        // bộ lọc phải chặn ngay từ tên class, trước khi readObject của nó được gọi
        assertEquals(0, Bomb.detonations.get());
    }

    @Test
    void clusterReplicatesOverSockets() throws Exception {
        var ids = new ArrayList<String>();
        for (int i = 0; i < 3; i++) {
            ids.add("localhost:" + freePort());
        }
        var rpc = client(500);
        var machines = new ArrayList<ListStateMachine>();
        for (int i = 0; i < ids.size(); i++) {
            var machine = new ListStateMachine();
            var folder = dataDir.resolve("node" + i).toString();
            var node = new RaftNode(NodeOptions.builder()
                    .raftMetaUri(folder)
                    .logUri(folder)
                    .snapshotUri(folder)
                    .electionTimeoutMinMs(150)
                    .electionTimeoutMaxMs(300)
                    .heartbeatIntervalMs(50)
                    .stateMachine(machine)
                    .raftConfig(RaftConfig.builder().self(ids.get(i)).peers(ids).build())
                    .build(), rpc);
            machines.add(machine);
            nodes.add(node);
            startServer(Integer.parseInt(ids.get(i).split(":")[1]), node);
            node.start();
        }

        var expected = new ArrayList<String>();
        for (int i = 0; i < 20; i++) {
            var command = "cmd" + i;
            // leader vừa đắc cử có thể còn đổi, nên thử lại tới khi lệnh được xác nhận
            await("command " + command + " to commit", () -> {
                var leader = nodes.stream().filter(n -> n.getState() == NodeState.LEADER).findFirst();
                try {
                    // lần thử trước có thể đã commit dù client không nhận được xác nhận
                    return machines.stream().anyMatch(m -> m.getStore().contains(command)) || (leader.isPresent()
                            && leader.get().appendClientCommand(command.getBytes(StandardCharsets.UTF_8)).get(3, TimeUnit.SECONDS));
                } catch (Exception e) {
                    return false;
                }
            });
            expected.add(command);
        }
        for (ListStateMachine machine : machines) {
            await("replica to converge", () -> machine.getStore().equals(expected));
        }
    }
}

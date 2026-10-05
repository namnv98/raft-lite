package com.namnv.rpc;

import com.namnv.ListStateMachine;
import com.namnv.config.NodeOptions;
import com.namnv.config.RaftConfig;
import com.namnv.core.NodeState;
import com.namnv.core.RaftNode;
import com.namnv.entity.ClientSession;
import com.namnv.entity.ConfigurationEntry;
import com.namnv.entity.LogEntry;
import com.namnv.rpc.client.SocketRpcClient;
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
import com.namnv.rpc.server.SocketRpcServer;
import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
            return new InstallSnapshotResponse(req.getTerm(), true, true);
        }

        @Override
        public TimeoutNowResponse handleTimeoutNowRequest(TimeoutNowRequest req) {
            handle();
            return new TimeoutNowResponse(req.term, true);
        }

        @Override
        public CompletableFuture<ReadIndexResponse> handleReadIndexRequest(ReadIndexRequest req) {
            handle();
            // trả lời muộn ở thread khác, như leader thật chờ một vòng heartbeat
            return CompletableFuture.supplyAsync(() -> new ReadIndexResponse(true, 99, req.requesterId),
                    CompletableFuture.delayedExecutor(50, TimeUnit.MILLISECONDS));
        }
    }

    // class "độc": chỉ cần được Java deserialize là đã chạy code
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
        var readIndex = rpc.readIndex(address, new ReadIndexRequest("B")).get(5, TimeUnit.SECONDS);
        assertTrue(readIndex.success);
        assertEquals(99, readIndex.readIndex);
        assertEquals("B", readIndex.leaderId);

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

        var session = new ClientSession();
        session.markApplied(1);
        session.markApplied(3);
        var snapshot = new InstallSnapshotRequest(7, "A", 10, 6, conf, new HashMap<>(Map.of("client-1", session)),
                List.of("snapshot.data"), "snapshot.data", 4096, new byte[]{1, 2, 3}, true);
        assertTrue(rpc.installSnapshot(address, snapshot).get(5, TimeUnit.SECONDS).isSuccess());
        assertArrayEquals(new byte[]{1, 2, 3}, service.lastSnapshot.getData());
        assertEquals(4096, service.lastSnapshot.getOffset());
        assertEquals(List.of("snapshot.data"), service.lastSnapshot.getFiles());
        assertTrue(service.lastSnapshot.isDone());
        assertEquals(conf, service.lastSnapshot.getConf());
        assertEquals(Map.of("client-1", session), service.lastSnapshot.getSessions());
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

    // gửi các byte thô tới server và trả về true nếu server đóng kết nối mà không trả lời gì
    private static boolean closedWithoutAnswer(int port, byte[] bytes) throws IOException {
        try (Socket socket = new Socket("localhost", port)) {
            socket.setSoTimeout(3000);
            socket.getOutputStream().write(bytes);
            socket.getOutputStream().flush();
            return socket.getInputStream().read() == -1;
        }
    }

    private static byte[] frame(int length, int type, String json) throws IOException {
        var bytes = new ByteArrayOutputStream();
        var out = new DataOutputStream(bytes);
        out.writeInt(length);
        out.writeByte(type);
        out.write(json.getBytes(StandardCharsets.UTF_8));
        return bytes.toByteArray();
    }

    @Test
    void serverClosesConnectionOnAnythingThatIsNotAValidRequest() throws Exception {
        var service = new EchoService();
        var port = freePort();
        startServer(port, service);

        var json = "{\"term\":1,\"candidateId\":\"A\",\"lastLogIndex\":0,\"lastLogTerm\":0}";
        // loại message không tồn tại
        assertTrue(closedWithoutAnswer(port, frame(json.length() + 1, 99, json)));
        // độ dài khung vô lý: server không được cấp phát theo nó
        assertTrue(closedWithoutAnswer(port, frame(Integer.MAX_VALUE, 0, json)));
        assertTrue(closedWithoutAnswer(port, frame(-5, 0, json)));
        // JSON hỏng, và JSON hợp lệ nhưng không khớp loại message
        assertTrue(closedWithoutAnswer(port, frame(8, 0, "{\"term\"")));
        assertTrue(closedWithoutAnswer(port, frame(18, 0, "{\"surprise\":true}")));
        // một response gửi nhầm chiều (loại 1 là PreVoteResponse)
        var response = "{\"term\":1,\"voteGranted\":true}";
        assertTrue(closedWithoutAnswer(port, frame(response.length() + 1, 1, response)));
        assertEquals(0, service.calls.get());

        // server vẫn phục vụ bình thường sau đó
        assertEquals(9, client(1000).requestVote("localhost:" + port, new RequestVoteRequest(9, "A", 0, 0))
                .get(5, TimeUnit.SECONDS).term);
    }

    @Test
    void serializedJavaObjectsAreNeverDeserialized() throws Exception {
        var port = freePort();
        startServer(port, new EchoService());

        // định dạng trên dây là JSON có khung: một object Java được serialize chỉ là rác, code của nó không bao giờ chạy
        var bytes = new ByteArrayOutputStream();
        try (var out = new ObjectOutputStream(bytes)) {
            out.writeObject(new Bomb());
        }
        assertTrue(closedWithoutAnswer(port, bytes.toByteArray()));
        assertEquals(0, Bomb.detonations.get());
    }

    // ---------- TLS ----------

    private static final char[] PASSWORD = "changeit".toCharArray();

    private void keytool(String... args) throws Exception {
        var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        command.addAll(List.of(args));
        command.addAll(List.of("-storepass", "changeit", "-storetype", "PKCS12", "-noprompt"));
        var process = new ProcessBuilder(command).redirectErrorStream(true).start();
        var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), "keytool failed: " + output);
    }

    // một cặp khoá tự ký, và truststore chỉ tin đúng chứng chỉ của trusted
    private SSLContext tlsContext(String name, String trusted) throws Exception {
        var keyStore = dataDir.resolve(name + ".p12");
        if (!Files.exists(keyStore)) {
            keytool("-genkeypair", "-alias", "node", "-keyalg", "EC", "-dname", "CN=" + name, "-validity", "2",
                    "-keystore", keyStore.toString());
            keytool("-exportcert", "-alias", "node", "-keystore", keyStore.toString(),
                    "-file", dataDir.resolve(name + ".cer").toString());
        }
        var trustStore = dataDir.resolve(name + "-trusts-" + trusted + ".p12");
        if (!Files.exists(trustStore)) {
            keytool("-importcert", "-alias", "peer", "-file", dataDir.resolve(trusted + ".cer").toString(),
                    "-keystore", trustStore.toString());
        }
        return TlsContexts.fromKeyStores(keyStore, trustStore, PASSWORD);
    }

    @Test
    void tlsServerOnlyTalksToPeersWithATrustedCertificate() throws Exception {
        // cả cluster dùng chung một chứng chỉ; kẻ lạ có chứng chỉ riêng và chỉ tin chính nó (hoặc tin cluster)
        var cluster = tlsContext("cluster", "cluster");
        tlsContext("stranger", "stranger");
        var strangerTrustingCluster = tlsContext("stranger", "cluster");

        var service = new EchoService();
        var port = freePort();
        var address = "localhost:" + port;
        var server = new SocketRpcServer(port, service, cluster);
        servers.add(server);
        server.start();

        // node có chứng chỉ của cluster: mọi thứ chạy như thường, kể cả message lớn
        var member = new SocketRpcClient(2000, cluster);
        clients.add(member);
        assertEquals(7, member.requestVote(address, new RequestVoteRequest(7, "A", 0, 0)).get(5, TimeUnit.SECONDS).term);
        var payload = new byte[200_000];
        var entries = List.of(new LogEntry(1, 1, payload));
        assertEquals(1, member.appendEntries(address, new AppendEntriesRequest(1, "A", 0, 0, entries, 0))
                .get(5, TimeUnit.SECONDS).matchIndex);
        assertEquals(payload.length, service.lastAppend.entries.get(0).getCommand().length);
        var served = service.calls.get();

        // kẻ lạ tin server nhưng server không tin nó; và client không dùng TLS
        var stranger = new SocketRpcClient(2000, strangerTrustingCluster);
        clients.add(stranger);
        assertThrows(ExecutionException.class,
                () -> stranger.requestVote(address, new RequestVoteRequest(8, "X", 0, 0)).get(5, TimeUnit.SECONDS));
        assertThrows(ExecutionException.class,
                () -> client(2000).requestVote(address, new RequestVoteRequest(9, "X", 0, 0)).get(5, TimeUnit.SECONDS));
        assertEquals(served, service.calls.get());
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

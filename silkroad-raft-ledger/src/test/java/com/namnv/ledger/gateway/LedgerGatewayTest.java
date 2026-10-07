package com.namnv.ledger.gateway;

import com.namnv.ledger.client.LedgerClient;
import com.namnv.ledger.node.LedgerNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Cổng HTTP trước một cụm sổ cái ba node thật: mã trả về, chống trùng bằng Idempotency-Key, gom lô. */
class LedgerGatewayTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir
    Path dir;

    private final List<String> servers = new ArrayList<>();
    private final List<LedgerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private String base;

    record Reply(int status, JsonNode body) {
    }

    @BeforeEach
    void startCluster() throws Exception {
        for (int i = 0; i < 3; i++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                servers.add("localhost:" + socket.getLocalPort());
            }
        }
        for (int i = 0; i < 3; i++) {
            nodes.add(LedgerNode.start(servers.get(i), servers, dir.resolve("n" + i).toString(), true, 100_000, false));
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables.reversed()) {
            closeable.close();
        }
        for (LedgerNode node : nodes) {
            node.close();
        }
        http.close();
    }

    private TransferBatcher startGateway(boolean batching) throws Exception {
        var client = new LedgerClient(servers, 20_000);
        var batcher = batching ? new TransferBatcher(client, 100, 2, 10_000) : null;
        var gateway = LedgerGateway.start(0, client, batcher);
        closeables.add(gateway);
        base = "http://localhost:" + gateway.port();
        return batcher;
    }

    private Reply post(String path, String key, String body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(base + path)).POST(HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json");
        if (key != null) {
            request.header("Idempotency-Key", key);
        }
        return reply(http.send(request.build(), HttpResponse.BodyHandlers.ofString()));
    }

    private Reply get(String path) throws Exception {
        return reply(http.send(HttpRequest.newBuilder(URI.create(base + path)).build(), HttpResponse.BodyHandlers.ofString()));
    }

    private static Reply reply(HttpResponse<String> response) throws IOException {
        return new Reply(response.statusCode(), JSON.readTree(response.body()));
    }

    private static String transfer(long debit, long credit, long amount) {
        return "{\"debitAccountId\":" + debit + ",\"creditAccountId\":" + credit + ",\"amount\":" + amount + ",\"ledger\":840}";
    }

    @Test
    void statusCodesAndIdempotencyKeys() throws Exception {
        startGateway(false);
        assertEquals(201, post("/accounts", null, "{\"id\":1,\"ledger\":840}").status());
        assertEquals(201, post("/accounts", null, "{\"id\":2,\"ledger\":840,\"flags\":1}").status());
        assertEquals(201, post("/accounts", null, "{\"id\":3,\"ledger\":840,\"flags\":1}").status());
        var again = post("/accounts", null, "{\"id\":2,\"ledger\":840,\"flags\":1}");
        assertEquals(200, again.status());
        assertEquals("EXISTS", again.body().get("result").asText());

        // ngân hàng nạp cho khách 2; gửi lại cùng key (ví dụ sau khi mất kết nối) không ghi hai lần
        var funded = post("/transfers", "fund-2", transfer(1, 2, 500));
        assertEquals(201, funded.status());
        assertEquals(LedgerGateway.idFromKey("fund-2"), funded.body().get("id").asLong());
        assertEquals(200, post("/transfers", "fund-2", transfer(1, 2, 500)).status());
        // cùng key nhưng nội dung khác: lỗi của bên gọi, không được ghi đè
        assertEquals(409, post("/transfers", "fund-2", transfer(1, 2, 501)).status());

        var overdraft = post("/transfers", "pay-1", transfer(2, 3, 501));
        assertEquals(422, overdraft.status());
        assertEquals("EXCEEDS_CREDITS", overdraft.body().get("result").asText());
        assertEquals(422, post("/transfers", "pay-2", transfer(2, 99, 1)).status());
        assertEquals(201, post("/transfers", "pay-3", transfer(2, 3, 200)).status());
        // id ghi rõ trong body thay cho key
        assertEquals(201, post("/transfers", null, "{\"id\":77,\"debitAccountId\":2,\"creditAccountId\":3,\"amount\":1,\"ledger\":840}").status());

        assertEquals(400, post("/transfers", null, transfer(1, 2, 5)).status());
        assertEquals(400, post("/transfers", "bad", "{not json").status());
        assertEquals(405, get("/transfers").status());
        assertEquals(404, get("/accounts/12345").status());
        assertEquals(404, get("/accounts/abc").status());

        var customer = get("/accounts/2").body();
        assertEquals(500, customer.get("creditsPosted").asLong());
        assertEquals(201, customer.get("debitsPosted").asLong());
        var stored = get("/transfers/" + LedgerGateway.idFromKey("pay-3"));
        assertEquals(200, stored.status());
        assertEquals(200, stored.body().get("amount").asLong());
        var totals = get("/totals").body();
        assertEquals(3, totals.get("transfers").asLong());
        assertEquals(totals.get("debitsPosted").asLong(), totals.get("creditsPosted").asLong());
    }

    @Test
    void pipelinedRequestsOnOneConnectionAreAnsweredInOrder() throws Exception {
        startGateway(true);
        assertEquals(201, post("/accounts", null, "{\"id\":1,\"ledger\":840}").status());
        assertEquals(201, post("/accounts", null, "{\"id\":2,\"ledger\":840,\"flags\":1}").status());
        int port = Integer.parseInt(base.substring(base.lastIndexOf(':') + 1));
        try (var socket = new Socket("localhost", port)) {
            // ba request liền nhau, không chờ trả lời: cái đầu cần cụm Raft (chậm), hai cái sau trả lời ngay trên cổng
            // (404 và 400), nhưng trả lời vẫn phải về đúng thứ tự
            String body = transfer(1, 2, 7);
            String requests = "POST /transfers HTTP/1.1\r\nHost: x\r\nIdempotency-Key: pipelined\r\nContent-Length: "
                    + body.length() + "\r\n\r\n" + body
                    + "GET /nowhere HTTP/1.1\r\nHost: x\r\n\r\n"
                    + "POST /transfers HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
            socket.getOutputStream().write(requests.getBytes(StandardCharsets.US_ASCII));
            String replies = new String(socket.getInputStream().readAllBytes(), StandardCharsets.US_ASCII);
            int first = replies.indexOf("HTTP/1.1 201");
            int second = replies.indexOf("HTTP/1.1 404");
            int third = replies.indexOf("HTTP/1.1 400");
            assertTrue(first >= 0 && first < second && second < third, replies);
        }
        assertEquals(7, get("/accounts/2").body().get("creditsPosted").asLong());
    }

    @Test
    void oversizedBodiesAreRefused() throws Exception {
        startGateway(false);
        var huge = "{\"pad\":\"" + "x".repeat(100_000) + "\"}";
        var reply = http.send(HttpRequest.newBuilder(URI.create(base + "/transfers"))
                .POST(HttpRequest.BodyPublishers.ofString(huge)).header("Idempotency-Key", "big").build(),
                HttpResponse.BodyHandlers.discarding());
        assertEquals(413, reply.statusCode());
        assertEquals(400, post("/transfers", "strings", "{\"debitAccountId\":\"2\",\"creditAccountId\":3,\"amount\":1,\"ledger\":840}").status());
        assertEquals(400, post("/transfers", "array", "[1,2]").status());
        assertEquals(400, post("/transfers", "trailing", transfer(1, 2, 3) + " {}").status());
    }

    @Test
    void concurrentRequestsAreBatchedAndEachGetsItsOwnAnswer() throws Exception {
        var batcher = startGateway(true);
        assertEquals(201, post("/accounts", null, "{\"id\":1,\"ledger\":840}").status());
        assertEquals(201, post("/accounts", null, "{\"id\":2,\"ledger\":840,\"flags\":1}").status());
        assertEquals(201, post("/transfers", "fund", transfer(1, 2, 1000)).status());

        // 300 giao dịch, mỗi cái rút 4 từ tài khoản chỉ có 1000, và mỗi cái được gửi hai lần cùng key (như client gửi lại
        // khi hết thời gian chờ), tất cả đồng thời: đúng 250 giao dịch được ghi, lần gửi thứ hai của chúng nhận 200, 50 giao
        // dịch còn lại bị từ chối cả hai lần, kể cả khi chúng nằm chung lô
        int transfers = 300;
        var replies = new ArrayList<Future<Reply>>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 2 * transfers; i++) {
                String key = "withdraw-" + (i % transfers);
                replies.add(executor.submit(() -> post("/transfers", key, transfer(2, 1, 4))));
            }
        }
        int created = 0;
        int existed = 0;
        int refused = 0;
        for (var reply : replies) {
            switch (reply.get().status()) {
                case 201 -> created++;
                case 200 -> existed++;
                case 422 -> refused++;
                default -> throw new AssertionError("unexpected " + reply.get());
            }
        }
        assertEquals(250, created);
        assertEquals(250, existed);
        assertEquals(100, refused);
        var customer = get("/accounts/2").body();
        assertEquals(1000, customer.get("creditsPosted").asLong());
        assertEquals(1000, customer.get("debitsPosted").asLong());
        assertTrue(batcher.averageBatch() > 1.5, "requests were not batched: " + batcher.averageBatch());
    }
}

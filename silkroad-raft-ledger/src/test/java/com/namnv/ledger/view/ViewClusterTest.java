package com.namnv.ledger.view;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.namnv.ledger.client.LedgerClient;
import com.namnv.ledger.event.EventPublisher;
import com.namnv.ledger.gateway.LedgerGateway;
import com.namnv.ledger.model.LedgerAccount;
import com.namnv.ledger.model.LedgerTotals;
import com.namnv.ledger.model.LedgerTransfer;
import com.namnv.ledger.node.LedgerNode;
import com.namnv.ledger.state.Ledger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Kiến trúc CQRS đầy đủ: ba voter ghi, một learner phát dòng sự kiện vào cơ sở dữ liệu quan hệ (H2, chế độ PostgreSQL), và
 * cổng HTTP trả sao kê từ cơ sở dữ liệu đó. Learner khởi động lại giữa chừng: mỗi sự kiện vẫn được ghi đúng một lần.
 */
class ViewClusterTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int USD = 840;
    private static final long BANK = 1;
    private static final long FIRST = 2;
    private static final int CUSTOMERS = 20;

    @TempDir
    Path dir;

    private final List<LedgerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final HttpClient http = HttpClient.newHttpClient();

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

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("timed out waiting for " + what);
            }
            Thread.sleep(20);
        }
    }

    private String url() {
        return "jdbc:h2:file:" + dir.resolve("view") + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE";
    }

    private long count(String table) {
        try (var connection = DriverManager.getConnection(url(), "sa", "");
             var rows = connection.createStatement().executeQuery("SELECT COUNT(*) FROM " + table)) {
            rows.next();
            return rows.getLong(1);
        } catch (java.sql.SQLException e) {
            return -1; // bảng chưa được tạo
        }
    }

    private JsonNode get(String base, String path) throws Exception {
        var response = http.send(HttpRequest.newBuilder(URI.create(base + path)).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), path + ": " + response.body());
        return JSON.readTree(response.body());
    }

    @Test
    void learnerFeedsARelationalViewThatServesStatementsExactlyOnceAcrossRestarts() throws Exception {
        var servers = new ArrayList<String>();
        for (int i = 0; i < 4; i++) {
            servers.add("localhost:" + freePort());
        }
        var voters = servers.subList(0, 3);
        var learnerId = servers.get(3);
        for (int i = 0; i < 3; i++) {
            nodes.add(LedgerNode.start(servers.get(i), voters, List.of(learnerId), dir.resolve("n" + i).toString(), true, 50,
                    false, new Ledger(dir.resolve("n" + i).resolve("transfers")), null));
        }
        Callable<LedgerNode> startLearner = () -> LedgerNode.start(learnerId, voters, List.of(learnerId),
                dir.resolve("learner").toString(), true, 50, false, new Ledger(dir.resolve("learner").resolve("transfers")),
                new EventPublisher(new JdbcEventSink(url(), "sa", ""), 10_000));
        nodes.add(startLearner.call());

        var client = new LedgerClient(voters, 20_000);
        var view = new LedgerView(url(), "sa", "", 2);
        closeables.add(view);
        var gateway = LedgerGateway.start(0, client, null, view);
        closeables.add(gateway);
        String base = "http://localhost:" + gateway.port();

        var accounts = new ArrayList<LedgerAccount>();
        accounts.add(new LedgerAccount(BANK, USD, 0));
        for (long id = FIRST; id < FIRST + CUSTOMERS; id++) {
            accounts.add(new LedgerAccount(id, USD, LedgerAccount.DEBITS_MUST_NOT_EXCEED_CREDITS));
        }
        client.createAccounts(accounts).get(20, TimeUnit.SECONDS);
        long nextId = 1;
        LedgerTotals totals = null;
        for (int round = 0; round < 2; round++) {
            // ngân hàng nạp cho từng khách, rồi khách 2 trả cho khách khác: đủ entry để có snapshot (mỗi 50 entry)
            for (int b = 0; b < 40; b++) {
                var batch = new ArrayList<LedgerTransfer>();
                for (int t = 0; t < 10; t++) {
                    long customer = FIRST + (t % CUSTOMERS);
                    batch.add(b % 2 == 0 || customer == FIRST
                            ? new LedgerTransfer(nextId++, BANK, customer, 10, USD)
                            : new LedgerTransfer(nextId++, FIRST, customer, 1, USD));
                }
                client.transfers(batch).get(20, TimeUnit.SECONDS);
            }
            totals = client.totals().get(10, TimeUnit.SECONDS);
            long expectedTransfers = totals.transfers();
            await("the view to catch up", () -> count("ledger_transfers") == expectedTransfers);
            if (round == 0) {
                // learner khởi động lại: phát lại phần sau snapshot; phía truy vấn bỏ qua phần đã có
                nodes.get(3).close();
                int port = Integer.parseInt(learnerId.split(":")[1]);
                await("old learner to release its port", () -> {
                    try (var socket = new ServerSocket(port)) {
                        return true;
                    } catch (IOException e) {
                        return false;
                    }
                });
                nodes.set(3, startLearner.call());
            }
        }
        // đúng một lần: mỗi giao dịch đúng một dòng và đúng hai bút toán, mỗi tài khoản đúng một dòng
        assertEquals(totals.transfers(), count("ledger_transfers"));
        assertEquals(2 * totals.transfers(), count("ledger_entries"));
        assertEquals(totals.accounts(), count("ledger_accounts"));

        // số dư ở phía truy vấn khớp với cụm Raft
        var fromCluster = client.lookupAccounts(List.of(FIRST)).get(10, TimeUnit.SECONDS).getFirst();
        var fromView = view.account(FIRST).get(10, TimeUnit.SECONDS);
        assertEquals(fromCluster.debitsPosted(), fromView.debitsPosted());
        assertEquals(fromCluster.creditsPosted(), fromView.creditsPosted());

        // sao kê qua HTTP: mới trước cũ sau, có thời điểm, số dư sau mỗi bút toán nối đúng vào nhau, phân trang đủ hết
        var allEntries = new ArrayList<JsonNode>();
        String next = null;
        do {
            var page = get(base, "/accounts/" + FIRST + "/entries?limit=7" + (next == null ? "" : "&before=" + next));
            page.get("entries").forEach(allEntries::add);
            next = page.get("next").isNull() ? null : page.get("next").asText();
        } while (next != null);
        assertEquals(count("ledger_entries WHERE account_id = " + FIRST), allEntries.size());
        long debits = fromView.debitsPosted();
        long credits = fromView.creditsPosted();
        long previousTime = Long.MAX_VALUE;
        for (JsonNode entry : allEntries) {
            assertEquals(debits, entry.get("debitsPosted").asLong());
            assertEquals(credits, entry.get("creditsPosted").asLong());
            long at = entry.get("postedAtMs").asLong();
            assertTrue(at > 1_600_000_000_000L && at <= previousTime, "timestamps go backwards: " + entry);
            previousTime = at;
            // lùi lại trước bút toán này
            if (entry.get("side").asText().equals("D")) {
                debits -= entry.get("amount").asLong();
            } else {
                credits -= entry.get("amount").asLong();
            }
        }
        assertEquals(0, debits);
        assertEquals(0, credits);

        // giao dịch tra từ cụm cũng mang thời điểm ghi, đúng bằng thời điểm phía truy vấn có
        var posted = client.lookupTransfers(List.of(1L)).get(10, TimeUnit.SECONDS).getFirst();
        assertEquals(view.transfer(1).get(10, TimeUnit.SECONDS).postedAtMs(), posted.timestamp());
        assertEquals(posted.timestamp(), get(base, "/transfers/1").get("timestamp").asLong());
        assertTrue(get(base, "/view/position").get("position").asText().contains("."));
        assertNull(view.account(9_999).get(10, TimeUnit.SECONDS));
    }
}

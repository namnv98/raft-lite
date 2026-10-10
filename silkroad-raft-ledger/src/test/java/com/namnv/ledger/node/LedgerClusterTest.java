package com.namnv.ledger.node;

import com.namnv.ledger.client.LedgerClient;
import com.namnv.ledger.event.EventPublisher;
import com.namnv.ledger.event.JsonLinesEventSink;
import com.namnv.ledger.event.LedgerEvent;
import com.namnv.ledger.model.LedgerAccount;
import com.namnv.ledger.model.LedgerBalance;
import com.namnv.ledger.model.LedgerResult;
import com.namnv.ledger.model.LedgerTotals;
import com.namnv.ledger.model.LedgerTransfer;
import com.namnv.ledger.state.Ledger;
import com.namnv.raft.NodeState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static com.namnv.ledger.model.LedgerAccount.DEBITS_MUST_NOT_EXCEED_CREDITS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Ba node sổ cái thật qua TCP, nhiều client chuyển tiền đồng thời (có một tài khoản nóng và các giao dịch rút quá số dư).
 */
class LedgerClusterTest {
    private static final int USD = 840;
    private static final long BANK = 1;
    private static final long HOT = 2;
    private static final int CUSTOMERS = 200;

    @TempDir
    Path dir;

    private final List<String> servers = new ArrayList<>();
    private final List<LedgerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
        for (LedgerNode node : nodes) {
            if (node != null) {
                node.close();
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private LedgerNode startNode(int i) throws Exception {
        return LedgerNode.start(servers.get(i), servers, dir.resolve("n" + i).toString(), true, 100, false);
    }

    private LedgerClient client() {
        var client = new LedgerClient(servers, 20_000);
        closeables.add(client);
        return client;
    }

    @Test
    void concurrentTransfersKeepEveryReplicaBalancedAndIdentical() throws Exception {
        for (int i = 0; i < 3; i++) {
            servers.add("localhost:" + freePort());
        }
        for (int i = 0; i < 3; i++) {
            nodes.add(startNode(i));
        }
        var admin = client();

        // ngân hàng được nợ tuỳ ý; mọi khách hàng (kể cả tài khoản nóng) không được âm
        var accounts = new ArrayList<LedgerAccount>();
        accounts.add(new LedgerAccount(BANK, USD, 0));
        for (long id = HOT; id < HOT + CUSTOMERS; id++) {
            accounts.add(new LedgerAccount(id, USD, DEBITS_MUST_NOT_EXCEED_CREDITS));
        }
        assertTrue(admin.createAccounts(accounts).get(20, TimeUnit.SECONDS).stream().allMatch(LedgerResult::succeeded));
        var funding = new ArrayList<LedgerTransfer>();
        for (long id = HOT; id < HOT + CUSTOMERS; id++) {
            funding.add(new LedgerTransfer(1_000_000 + id, BANK, id, 1_000, USD));
        }
        assertTrue(admin.transfers(funding).get(20, TimeUnit.SECONDS).stream().allMatch(r -> r == LedgerResult.OK));

        // 8 client, mỗi client 40 lô x 25 giao dịch; nửa số giao dịch đi vào hoặc ra khỏi tài khoản nóng,
        // số tiền ngẫu nhiên nên có những lần bị từ chối vì không đủ tiền
        var nextId = new AtomicLong(10_000_000);
        var tally = new EnumMap<LedgerResult, AtomicLong>(LedgerResult.class);
        for (LedgerResult r : LedgerResult.values()) {
            tally.put(r, new AtomicLong());
        }
        var work = new ArrayList<CompletableFuture<Void>>();
        for (int c = 0; c < 8; c++) {
            var client = client();
            var random = new Random(c);
            work.add(CompletableFuture.runAsync(() -> {
                for (int b = 0; b < 40; b++) {
                    var batch = new ArrayList<LedgerTransfer>();
                    for (int t = 0; t < 25; t++) {
                        long from = random.nextBoolean() ? HOT : HOT + 1 + random.nextInt(CUSTOMERS - 1);
                        long to = HOT + 1 + random.nextInt(CUSTOMERS - 1);
                        if (from == to) {
                            to = HOT;
                        }
                        batch.add(new LedgerTransfer(nextId.getAndIncrement(), from, to, 1 + random.nextInt(300), USD));
                    }
                    try {
                        client.transfers(batch).get(30, TimeUnit.SECONDS).forEach(r -> tally.get(r).incrementAndGet());
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }
            }));
        }
        CompletableFuture.allOf(work.toArray(CompletableFuture[]::new)).get(120, TimeUnit.SECONDS);
        long ok = tally.get(LedgerResult.OK).get();
        long refused = tally.get(LedgerResult.EXCEEDS_CREDITS).get();
        assertEquals(8 * 40 * 25, ok + refused, "only OK and EXCEEDS_CREDITS are expected: " + tally);
        assertTrue(ok > 0 && refused > 0, "the run should include both kinds: " + tally);

        // mọi bản sao: cùng tổng, tổng nợ bằng tổng có, đúng số giao dịch đã được xác nhận
        var expected = admin.totals().get(10, TimeUnit.SECONDS);
        assertTrue(expected.balanced());
        assertEquals(CUSTOMERS + 1, expected.accounts());
        assertEquals(CUSTOMERS + ok, expected.transfers());
        for (String server : servers) {
            assertEquals(expected, admin.totalsFrom(server).get(10, TimeUnit.SECONDS), "replica " + server);
        }
        // không khách hàng nào âm, và tổng số dư của khách hàng đúng bằng số ngân hàng đã nạp (tiền được bảo toàn)
        var ids = new ArrayList<Long>();
        for (long id = HOT; id < HOT + CUSTOMERS; id++) {
            ids.add(id);
        }
        var balances = admin.lookupAccounts(ids).get(10, TimeUnit.SECONDS);
        assertTrue(balances.stream().allMatch(b -> b.creditBalance() >= 0));
        assertEquals(CUSTOMERS * 1_000L, balances.stream().mapToLong(LedgerBalance::creditBalance).sum());
        for (String server : servers) {
            assertEquals(balances, admin.lookupAccountsFrom(server, ids).get(10, TimeUnit.SECONDS));
        }

        // gửi lại một giao dịch đã ghi: không ghi lần hai
        var first = new LedgerTransfer(nextId.getAndIncrement(), BANK, HOT, 5, USD);
        assertEquals(LedgerResult.OK, admin.transfer(first).get(10, TimeUnit.SECONDS));
        assertEquals(LedgerResult.EXISTS, admin.transfer(first).get(10, TimeUnit.SECONDS));

        // khởi động lại một follower: sổ được dựng lại từ snapshot và log trên đĩa, giống hệt các bản khác
        int follower = 0;
        while (nodes.get(follower).node().getState() == NodeState.LEADER) {
            follower++;
        }
        // snapshot chạy ngầm: chờ follower có ít nhất một snapshot để lần khởi động lại thật sự dùng nó
        var snapshotting = nodes.get(follower).node();
        await("the follower to take a snapshot", () -> snapshotting.metrics().snapshotsCreated() > 0);
        nodes.get(follower).close();
        nodes.set(follower, null);
        int port = Integer.parseInt(servers.get(follower).split(":")[1]);
        await("old node to release its port", () -> {
            try (var socket = new ServerSocket(port)) {
                return true;
            } catch (IOException e) {
                return false;
            }
        });
        nodes.set(follower, startNode(follower));
        var after = admin.totals().get(10, TimeUnit.SECONDS);
        var restarted = nodes.get(follower).ledger();
        await("restarted replica to catch up", () -> restarted.totals().equals(after));
        assertTrue(nodes.get(follower).node().metrics().firstLogIndex() > 1, "the replica should have used a snapshot");
        assertEquals(admin.lookupAccounts(ids).get(10, TimeUnit.SECONDS),
                admin.lookupAccountsFrom(servers.get(follower), ids).get(10, TimeUnit.SECONDS));
    }

    // ---------- learner và dòng sự kiện ----------

    private static List<LedgerEvent> readEvents(Path file) throws IOException {
        var json = new com.fasterxml.jackson.databind.ObjectMapper();
        var events = new ArrayList<LedgerEvent>();
        if (java.nio.file.Files.exists(file)) {
            for (String line : java.nio.file.Files.readAllLines(file)) {
                events.add(json.readValue(line, LedgerEvent.class));
            }
        }
        return events;
    }

    // các sự kiện khớp đúng với sổ cái: đủ, không trùng, đúng thứ tự, tổng tiền khớp
    private static void assertFeedMatches(List<LedgerEvent> events, LedgerTotals totals) {
        for (int i = 1; i < events.size(); i++) {
            assertTrue(events.get(i).isAfter(events.get(i - 1).index(), events.get(i - 1).position()),
                    "events out of order at " + i);
        }
        var transfers = events.stream().filter(e -> e.type() == LedgerEvent.Type.TRANSFER_POSTED).toList();
        assertEquals(totals.accounts(), events.stream().filter(e -> e.type() == LedgerEvent.Type.ACCOUNT_CREATED).count());
        assertEquals(totals.transfers(), transfers.size());
        assertEquals(totals.transfers(), transfers.stream().map(LedgerEvent::id).distinct().count());
        assertEquals(totals.debitsPosted(), transfers.stream().mapToLong(LedgerEvent::amount).sum());
    }

    @Test
    void learnerPublishesEveryCommittedChangeExactlyOnceAcrossRestarts() throws Exception {
        for (int i = 0; i < 4; i++) {
            servers.add("localhost:" + freePort());
        }
        var voters = servers.subList(0, 3);
        var learnerId = servers.get(3);
        Path feed = dir.resolve("events.jsonl");
        for (int i = 0; i < 3; i++) {
            nodes.add(LedgerNode.start(servers.get(i), voters, List.of(learnerId), dir.resolve("n" + i).toString(), true, 50,
                    false, new Ledger(dir.resolve("n" + i).resolve("transfers")), null));
        }
        java.util.concurrent.Callable<LedgerNode> startLearner = () -> LedgerNode.start(learnerId, voters, List.of(learnerId),
                dir.resolve("learner").toString(), true, 50, false, new Ledger(dir.resolve("learner").resolve("transfers")),
                new EventPublisher(new JsonLinesEventSink(feed), 10_000));
        nodes.add(startLearner.call());
        // client chỉ nói chuyện với các voter
        var admin = new LedgerClient(voters, 20_000);
        closeables.add(admin);

        var accounts = new ArrayList<LedgerAccount>();
        accounts.add(new LedgerAccount(BANK, USD, 0));
        for (long id = HOT; id < HOT + 50; id++) {
            accounts.add(new LedgerAccount(id, USD, DEBITS_MUST_NOT_EXCEED_CREDITS));
        }
        admin.createAccounts(accounts).get(20, TimeUnit.SECONDS);
        var nextId = new AtomicLong(1);
        for (int round = 0; round < 2; round++) {
            for (int b = 0; b < 30; b++) {
                var batch = new ArrayList<LedgerTransfer>();
                for (int t = 0; t < 20; t++) {
                    batch.add(new LedgerTransfer(nextId.getAndIncrement(), BANK, HOT + (t % 50), 1 + t, USD));
                }
                admin.transfers(batch).get(20, TimeUnit.SECONDS);
            }
            var totals = admin.totals().get(10, TimeUnit.SECONDS);
            var learner = nodes.get(3).ledger();
            await("learner to apply everything", () -> learner.totals().equals(totals));
            await("the feed to catch up", () -> {
                try {
                    return readEvents(feed).size() == totals.accounts() + totals.transfers();
                } catch (IOException e) {
                    return false;
                }
            });
            assertFeedMatches(readEvents(feed), totals);
            if (round == 0) {
                // learner khởi động lại: dựng lại sổ từ snapshot và log, phát lại phần sau snapshot; sink bỏ qua phần đã có
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
        assertTrue(nodes.get(3).node().getState() != NodeState.LEADER);
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(20);
        }
        fail("Timed out waiting for: " + what);
    }
}

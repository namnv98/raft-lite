package com.namnv.ledger;

import com.namnv.agent.AgentLoop;
import com.namnv.rpc.MessageTransport;
import com.namnv.transport.nio.NioRpcClient;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Đo dịch vụ sổ cái như Binance Ledger đo: ba node là ba tiến trình, client ở tiến trình này, mọi thứ qua TCP.
 * <ol>
 * <li>Tạo một tài khoản ngân hàng (được nợ tuỳ ý) và {@code ledger.accounts} tài khoản khách hàng (không được âm),
 * rồi ngân hàng nạp cho mỗi khách hàng cùng một số tiền.</li>
 * <li>Mỗi client gửi liên tục các lô {@code ledger.batch} giao dịch giữa các khách hàng, số tiền ngẫu nhiên (nên có
 * giao dịch bị từ chối vì không đủ tiền). Kịch bản "nóng": một nửa số giao dịch đi vào hoặc ra khỏi cùng một tài
 * khoản.</li>
 * <li>Cuối cùng kiểm tra trên cả ba node: tổng nợ bằng tổng có, các bản sao giống nhau, không khách hàng nào âm và tổng
 * số dư của khách hàng đúng bằng số đã nạp.</li>
 * </ol>
 * Tham số: thư mục dữ liệu. -Dledger.accounts=100000 -Dledger.batch=100 -Dledger.clients=1,32,256 -Dledger.seconds=5
 * -Dledger.scenarios=uniform,hot -Dledger.logSync=false -Dledger.nodeIps=true (cho tc netem, xem bench/netem)
 * -Dledger.nodeHeap=2g: heap của mỗi node. Lịch sử giao dịch nằm trong RocksDB, nên bộ nhớ không tăng theo số giao dịch
 * (trừ bloom filter của id, khoảng 1,5 byte mỗi giao dịch).
 */
public final class LedgerBenchmark {
    private static final int USD = 840;
    private static final long BANK = 1;
    private static final long FIRST_CUSTOMER = 2;
    private static final long FUNDING = 1_000_000;

    private final List<String> servers;
    private final List<MessageTransport> transports = new ArrayList<>();
    private final int customers;
    private final int batch;
    private final AtomicLong nextTransferId = new AtomicLong(1L << 40);

    private LedgerBenchmark(List<String> servers, int customers, int batch) {
        this.servers = servers;
        this.customers = customers;
        this.batch = batch;
        var loops = List.of(new AgentLoop("ledger-bench-0"), new AgentLoop("ledger-bench-1"));
        for (int i = 0; i < 8; i++) {
            transports.add(new NioRpcClient(loops.get(i % loops.size()), 2000));
        }
    }

    private LedgerClient client(int n) {
        return new LedgerClient(transports.get(n % transports.size()), servers, 30_000);
    }

    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of(args.length > 0 ? args[0] : "target/ledger-bench").toAbsolutePath();
        int exit = 0;
        var cluster = new LocalCluster(dataDir);
        try {
            var bench = new LedgerBenchmark(cluster.servers, Integer.getInteger("ledger.accounts", 100_000),
                    Integer.getInteger("ledger.batch", 100));
            bench.run();
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
        var admin = client(0);
        long setupStart = System.nanoTime();
        // tài khoản và tiền nạp, theo lô 1000
        var accounts = new ArrayList<LedgerAccount>();
        accounts.add(new LedgerAccount(BANK, USD, 0));
        for (long id = FIRST_CUSTOMER; id < FIRST_CUSTOMER + customers; id++) {
            accounts.add(new LedgerAccount(id, USD, LedgerAccount.DEBITS_MUST_NOT_EXCEED_CREDITS));
        }
        for (int from = 0; from < accounts.size(); from += 1000) {
            var results = admin.createAccounts(accounts.subList(from, Math.min(accounts.size(), from + 1000)))
                    .get(60, TimeUnit.SECONDS);
            require(results.stream().allMatch(LedgerResult::succeeded), "account creation failed: " + results);
        }
        for (long from = FIRST_CUSTOMER; from < FIRST_CUSTOMER + customers; from += 1000) {
            var funding = new ArrayList<LedgerTransfer>();
            for (long id = from; id < Math.min(FIRST_CUSTOMER + customers, from + 1000); id++) {
                funding.add(new LedgerTransfer(nextTransferId.getAndIncrement(), BANK, id, FUNDING, USD));
            }
            var results = admin.transfers(funding).get(60, TimeUnit.SECONDS);
            require(results.stream().allMatch(LedgerResult::succeeded), "funding failed: " + results);
        }
        System.out.printf("# Sổ cái trên Silk Road Raft: 3 node là 3 tiến trình, client ở tiến trình thứ tư, qua TCP%n");
        System.out.printf("# %d tài khoản khách hàng (không được âm), mỗi tài khoản nạp %d; lô %d giao dịch; fsync %s; "
                        + "chuẩn bị mất %.1f giây%n", customers, FUNDING, batch, System.getProperty("ledger.logSync", "true"),
                (System.nanoTime() - setupStart) / 1e9);
        System.out.println();
        System.out.printf("| %-24s | %12s | %8s | %9s | %9s | %9s |%n",
                "kịch bản", "giao dịch/s", "từ chối", "p50 lô ms", "p99 lô ms", "max lô ms");
        System.out.println("|" + "-".repeat(26) + "|" + "-".repeat(14) + "|" + "-".repeat(10) + "|" + "-".repeat(11)
                + "|" + "-".repeat(11) + "|" + "-".repeat(11) + "|");
        int seconds = Integer.getInteger("ledger.seconds", 3);
        for (String scenario : System.getProperty("ledger.scenarios", "uniform,hot").split(",")) {
            double hot = scenario.equals("hot") ? 0.5 : 0.0;
            for (String c : System.getProperty("ledger.clients", "1,32,256").split(",")) {
                measure(scenario, Integer.parseInt(c), hot, seconds);
            }
        }
        verify(admin);
    }

    // các client chạy vòng kín: gửi một lô, chờ kết quả, gửi lô kế tiếp
    private void measure(String scenario, int clients, double hotShare, int seconds) throws Exception {
        long warmupEnd = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        long end = warmupEnd + TimeUnit.SECONDS.toNanos(seconds);
        long[] latencies = new long[4_000_000];
        var count = new AtomicInteger();
        var transfers = new AtomicLong();
        var refused = new AtomicLong();
        var finished = new CountDownLatch(clients);
        for (int c = 0; c < clients; c++) {
            var client = client(c);
            var random = new SplittableRandom(c * 31L + scenario.hashCode());
            loop(client, random, hotShare, warmupEnd, end, latencies, count, transfers, refused, finished);
        }
        require(finished.await(seconds + 120, TimeUnit.SECONDS), "clients did not finish");
        int n = Math.min(count.get(), latencies.length);
        long[] sorted = Arrays.copyOf(latencies, n);
        Arrays.sort(sorted);
        System.out.printf("| %-24s | %,12d | %7.1f%% | %9.2f | %9.2f | %9.2f |%n",
                scenario + ", " + clients + " client", transfers.get() / seconds,
                transfers.get() == 0 ? 0 : 100.0 * refused.get() / transfers.get(),
                n == 0 ? 0 : sorted[n / 2] / 1e6, n == 0 ? 0 : sorted[(int) (n * 0.99)] / 1e6, n == 0 ? 0 : sorted[n - 1] / 1e6);
    }

    private void loop(LedgerClient client, SplittableRandom random, double hotShare, long warmupEnd, long end,
                      long[] latencies, AtomicInteger count, AtomicLong transfers, AtomicLong refused, CountDownLatch finished) {
        long start = System.nanoTime();
        if (start >= end) {
            finished.countDown();
            return;
        }
        var list = new ArrayList<LedgerTransfer>(batch);
        for (int i = 0; i < batch; i++) {
            long from = FIRST_CUSTOMER + random.nextInt(customers);
            long to = FIRST_CUSTOMER + random.nextInt(customers);
            if (random.nextDouble() < hotShare) {
                if (random.nextBoolean()) {
                    from = FIRST_CUSTOMER; // tài khoản nóng
                } else {
                    to = FIRST_CUSTOMER;
                }
            }
            if (from == to) {
                to = from == FIRST_CUSTOMER ? FIRST_CUSTOMER + 1 : FIRST_CUSTOMER;
            }
            list.add(new LedgerTransfer(nextTransferId.getAndIncrement(), from, to, 1 + random.nextInt(2 * (int) (FUNDING / 100)), USD));
        }
        client.transfers(list).whenComplete((results, error) -> {
            long now = System.nanoTime();
            if (error != null) {
                System.err.println("batch failed: " + error);
            } else if (start >= warmupEnd && now <= end) {
                int slot = count.getAndIncrement();
                if (slot < latencies.length) {
                    latencies[slot] = now - start;
                }
                transfers.addAndGet(results.size());
                refused.addAndGet(results.stream().filter(r -> r != LedgerResult.OK).count());
            }
            loop(client, random, hotShare, warmupEnd, end, latencies, count, transfers, refused, finished);
        });
    }

    // kiểm tra trên cả ba node: các bản sao giống nhau, sổ cân, tiền được bảo toàn, không ai âm
    private void verify(LedgerClient admin) throws Exception {
        var totals = admin.totals().get(30, TimeUnit.SECONDS);
        require(totals.balanced(), "books are not balanced: " + totals);
        for (String server : servers) {
            var replica = admin.totalsFrom(server).get(30, TimeUnit.SECONDS);
            require(replica.equals(totals), "replica " + server + " differs: " + replica + " vs " + totals);
        }
        long sum = 0;
        long negative = 0;
        for (long from = FIRST_CUSTOMER; from < FIRST_CUSTOMER + customers; from += 10_000) {
            var ids = new ArrayList<Long>();
            for (long id = from; id < Math.min(FIRST_CUSTOMER + customers, from + 10_000); id++) {
                ids.add(id);
            }
            var leaderView = admin.lookupAccounts(ids).get(30, TimeUnit.SECONDS);
            for (String server : servers) {
                require(leaderView.equals(admin.lookupAccountsFrom(server, ids).get(30, TimeUnit.SECONDS)),
                        "replica " + server + " has different balances");
            }
            for (var balance : leaderView) {
                sum += balance.creditBalance();
                negative += balance.creditBalance() < 0 ? 1 : 0;
            }
        }
        require(negative == 0, negative + " customer accounts are negative");
        require(sum == customers * FUNDING, "money was created or destroyed: " + sum + " vs " + customers * FUNDING);
        System.out.println();
        System.out.printf("kiểm tra: %,d giao dịch đã ghi; tổng nợ = tổng có = %,d; ba bản sao giống hệt nhau; "
                + "không khách hàng nào âm; tổng số dư khách hàng = số đã nạp%n", totals.transfers(), totals.debitsPosted());
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}

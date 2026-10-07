package com.namnv.ledger.bench;

import com.namnv.ledger.event.LedgerEvent;
import com.namnv.ledger.view.JdbcEventSink;
import com.namnv.ledger.view.LedgerView;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * Đo phía truy vấn trên một cơ sở dữ liệu thật: tốc độ {@link JdbcEventSink} ghi dòng sự kiện (theo lô như
 * {@code EventPublisher} gửi), rồi độ trễ của truy vấn sao kê qua {@link LedgerView}.
 * <pre>
 * java ... com.namnv.ledger.bench.ViewBenchmark jdbc:postgresql://localhost:5432/ledger?reWriteBatchedInserts=true user password
 * </pre>
 * -Dview.accounts=10000 -Dview.transfers=1000000 -Dview.batch=10000 -Dview.queries=20000 -Dview.queryThreads=8.
 * Cơ sở dữ liệu nên trống (các bảng được tạo nếu chưa có).
 */
public final class ViewBenchmark {
    private static final int USD = 840;

    public static void main(String[] args) throws Exception {
        String url = args[0];
        String user = args.length > 1 ? args[1] : null;
        String password = args.length > 2 ? args[2] : null;
        int accounts = Integer.getInteger("view.accounts", 10_000);
        int transfers = Integer.getInteger("view.transfers", 1_000_000);
        int batchSize = Integer.getInteger("view.batch", 10_000);

        long[] debits = new long[accounts + 1];
        long[] credits = new long[accounts + 1];
        try (var sink = new JdbcEventSink(url, user, password)) {
            long index = sink.lastIndex() + 1;
            long transferId = 1L << 40;
            var batch = new ArrayList<LedgerEvent>(batchSize);
            for (int a = 1; a <= accounts; a++) {
                batch.add(LedgerEvent.accountCreated(index, a - 1, System.currentTimeMillis(), a, USD, 0));
            }
            sink.write(batch);
            index++;
            var random = new SplittableRandom(7);
            long start = System.nanoTime();
            long written = 0;
            while (written < transfers) {
                batch.clear();
                // 100 giao dịch mỗi entry, như client gửi theo lô
                for (int i = 0; i < batchSize && written < transfers; i++, written++) {
                    int from = 1 + random.nextInt(accounts);
                    int to = 1 + random.nextInt(accounts);
                    if (to == from) {
                        to = from % accounts + 1;
                    }
                    long amount = 1 + random.nextInt(1000);
                    debits[from] += amount;
                    credits[to] += amount;
                    batch.add(LedgerEvent.transferPosted(index, i % 100, System.currentTimeMillis(), transferId++, from, to,
                            amount, USD, debits[from], credits[from], debits[to], credits[to]));
                    if (i % 100 == 99) {
                        index++;
                    }
                }
                index++;
                sink.write(batch);
            }
            double seconds = (System.nanoTime() - start) / 1e9;
            System.out.printf("ghi: %,d giao dịch (%,d dòng: giao dịch, 2 bút toán, số dư) trong %.1f s = %,.0f giao dịch/s, "
                    + "lô %,d sự kiện%n", transfers, sink.rowsWritten(), seconds, transfers / seconds, batchSize);
        }

        int queries = Integer.getInteger("view.queries", 20_000);
        int threads = Integer.getInteger("view.queryThreads", 8);
        try (var view = new LedgerView(url, user, password, threads)) {
            var random = new SplittableRandom(11);
            for (String kind : List.of("số dư", "sao kê 50 dòng")) {
                long[] latencies = new long[queries];
                int inflight = threads * 4;
                var futures = new ArrayList<java.util.concurrent.CompletableFuture<?>>();
                long start = System.nanoTime();
                for (int q = 0; q < queries; q++) {
                    long account = 1 + random.nextInt(accounts);
                    int slot = q;
                    long sent = System.nanoTime();
                    var future = kind.equals("số dư") ? view.account(account) : view.entries(account, 50, null);
                    futures.add(future.whenComplete((r, e) -> latencies[slot] = System.nanoTime() - sent));
                    if (futures.size() >= inflight) {
                        futures.removeFirst().get(30, TimeUnit.SECONDS);
                    }
                }
                for (var f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }
                double seconds = (System.nanoTime() - start) / 1e9;
                Arrays.sort(latencies);
                System.out.printf("đọc %s: %,.0f truy vấn/s (%d thread), p50 %.2f ms, p99 %.2f ms, max %.2f ms%n", kind,
                        queries / seconds, threads, latencies[queries / 2] / 1e6, latencies[(int) (queries * 0.99)] / 1e6,
                        latencies[queries - 1] / 1e6);
            }
        }
    }
}

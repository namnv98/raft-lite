package com.namnv.ledger.view;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Đọc phía truy vấn của sổ cái (các bảng mà {@link JdbcEventSink} ghi): số dư, giao dịch và sao kê theo tài khoản. Dữ liệu
 * đi sau cụm Raft một chút (độ trễ của learner và của sink): {@link #position()} cho biết đã tới sự kiện nào.
 * <p>
 * Truy vấn chạy trên {@code threads} thread riêng, mỗi thread một kết nối, và trả về {@link CompletableFuture}: event loop của
 * cổng HTTP không bao giờ chờ cơ sở dữ liệu.
 */
public final class LedgerView implements AutoCloseable {
    private final String url;
    private final String user;
    private final String password;
    private final ExecutorService executor;
    private final ThreadLocal<Queries> queries = new ThreadLocal<>();
    private final List<Queries> opened = new ArrayList<>();

    /** một tài khoản như phía truy vấn thấy */
    public record Account(long id, int ledger, int flags, long debitsPosted, long creditsPosted, long createdAtMs,
                          long updatedAtMs) {
        public long creditBalance() {
            return creditsPosted - debitsPosted;
        }
    }

    /** một giao dịch đã ghi */
    public record Transfer(long id, long debitAccountId, long creditAccountId, long amount, int ledger, long postedAtMs,
                           long logIndex, int logPosition) {
    }

    /**
     * Một bút toán trong sao kê của một tài khoản: {@code side} là D (tài khoản bị ghi nợ) hoặc C (được ghi có); hai tổng
     * là của tài khoản ngay sau bút toán.
     */
    public record Entry(long accountId, long logIndex, int logPosition, long transferId, String side, long amount,
                        long debitsPosted, long creditsPosted, long postedAtMs) {
        public long creditBalance() {
            return creditsPosted - debitsPosted;
        }
    }

    /** vị trí trong dòng sự kiện của sổ cái: (index của entry trong log, thứ tự trong lô) */
    public record Position(long logIndex, int logPosition) {
        @Override
        public String toString() {
            return logIndex + "." + logPosition;
        }

        /** đọc dạng "index.position" của {@link #toString()} */
        public static Position parse(String text) {
            int dot = text.indexOf('.');
            if (dot < 0) {
                throw new IllegalArgumentException("expected index.position: " + text);
            }
            return new Position(Long.parseLong(text.substring(0, dot)), Integer.parseInt(text.substring(dot + 1)));
        }
    }

    private final class Queries {
        final Connection connection;
        final PreparedStatement account;
        final PreparedStatement transfer;
        final PreparedStatement entries;
        final PreparedStatement entriesBefore;
        final PreparedStatement position;

        Queries() throws SQLException {
            connection = DriverManager.getConnection(url, user, password);
            ViewSchema.create(connection);
            connection.setAutoCommit(true);
            connection.setReadOnly(true);
            account = connection.prepareStatement("SELECT id, ledger, flags, debits_posted, credits_posted, created_at_ms, "
                    + "updated_at_ms FROM ledger_accounts WHERE id = ?");
            transfer = connection.prepareStatement("SELECT id, debit_account_id, credit_account_id, amount, ledger, "
                    + "posted_at_ms, log_index, log_position FROM ledger_transfers WHERE id = ?");
            String entryColumns = "SELECT account_id, log_index, log_position, transfer_id, side, amount, debits_posted, "
                    + "credits_posted, posted_at_ms FROM ledger_entries WHERE account_id = ?";
            String newestFirst = " ORDER BY log_index DESC, log_position DESC LIMIT ?";
            entries = connection.prepareStatement(entryColumns + newestFirst);
            // trang kế tiếp: bút toán đứng trước (index, position) của dòng cuối trang trước
            entriesBefore = connection.prepareStatement(entryColumns
                    + " AND (log_index < ? OR (log_index = ? AND log_position < ?))" + newestFirst);
            position = connection.prepareStatement("SELECT log_index, log_position FROM ledger_feed WHERE id = 1");
        }
    }

    private interface Query<T> {
        T run(Queries queries) throws SQLException;
    }

    public LedgerView(String url, String user, String password, int threads) {
        this.url = url;
        this.user = user;
        this.password = password;
        var counter = new AtomicInteger();
        this.executor = Executors.newFixedThreadPool(threads,
                r -> Thread.ofPlatform().daemon().name("ledger-view-" + counter.getAndIncrement()).unstarted(r));
    }

    private <T> CompletableFuture<T> submit(Query<T> query) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Queries q = queries.get();
                if (q == null || q.connection.isClosed()) {
                    q = new Queries();
                    queries.set(q);
                    synchronized (opened) {
                        opened.add(q);
                    }
                }
                try {
                    return query.run(q);
                } catch (SQLException e) {
                    // kết nối có thể đã hỏng (cơ sở dữ liệu khởi động lại): lần sau mở kết nối mới
                    queries.remove();
                    q.connection.close();
                    throw e;
                }
            } catch (SQLException e) {
                throw new IllegalStateException("ledger view query failed: " + e.getMessage(), e);
            }
        }, executor);
    }

    /** tài khoản, hoặc null nếu phía truy vấn chưa có nó */
    public CompletableFuture<Account> account(long id) {
        return submit(q -> {
            q.account.setLong(1, id);
            try (ResultSet row = q.account.executeQuery()) {
                return row.next() ? new Account(row.getLong(1), row.getInt(2), row.getInt(3), row.getLong(4), row.getLong(5),
                        row.getLong(6), row.getLong(7)) : null;
            }
        });
    }

    /** giao dịch, hoặc null nếu phía truy vấn chưa có nó */
    public CompletableFuture<Transfer> transfer(long id) {
        return submit(q -> {
            q.transfer.setLong(1, id);
            try (ResultSet row = q.transfer.executeQuery()) {
                return row.next() ? new Transfer(row.getLong(1), row.getLong(2), row.getLong(3), row.getLong(4), row.getInt(5),
                        row.getLong(6), row.getLong(7), row.getInt(8)) : null;
            }
        });
    }

    /**
     * Sao kê: tối đa {@code limit} bút toán mới nhất của tài khoản, mới trước cũ sau; {@code before} (có thể null) là vị
     * trí của bút toán cuối trang trước, để lấy trang kế tiếp.
     */
    public CompletableFuture<List<Entry>> entries(long accountId, int limit, Position before) {
        return submit(q -> {
            PreparedStatement statement;
            if (before == null) {
                statement = q.entries;
                statement.setLong(1, accountId);
                statement.setInt(2, limit);
            } else {
                statement = q.entriesBefore;
                statement.setLong(1, accountId);
                statement.setLong(2, before.logIndex());
                statement.setLong(3, before.logIndex());
                statement.setInt(4, before.logPosition());
                statement.setInt(5, limit);
            }
            var result = new ArrayList<Entry>(limit);
            try (ResultSet row = statement.executeQuery()) {
                while (row.next()) {
                    result.add(new Entry(row.getLong(1), row.getLong(2), row.getInt(3), row.getLong(4), row.getString(5).trim(),
                            row.getLong(6), row.getLong(7), row.getLong(8), row.getLong(9)));
                }
            }
            return result;
        });
    }

    /** vị trí của sự kiện cuối cùng phía truy vấn đã có */
    public CompletableFuture<Position> position() {
        return submit(q -> {
            try (ResultSet row = q.position.executeQuery()) {
                return row.next() ? new Position(row.getLong(1), row.getInt(2)) : new Position(0, -1);
            }
        });
    }

    @Override
    public void close() {
        executor.shutdown();
        synchronized (opened) {
            for (Queries q : opened) {
                try {
                    q.connection.close();
                } catch (SQLException ignored) {
                    // đang đóng
                }
            }
        }
    }
}

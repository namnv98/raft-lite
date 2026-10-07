package com.namnv.ledger.view;

import com.namnv.ledger.event.EventSink;
import com.namnv.ledger.event.LedgerEvent;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Phía truy vấn của sổ cái (CQRS, như "view domain" của Binance Ledger): ghi dòng sự kiện vào một cơ sở dữ liệu quan hệ
 * (xem {@link ViewSchema}) để hệ thống khác truy vấn số dư, giao dịch và sao kê mà không đụng tới cụm Raft.
 * <ul>
 * <li>Mỗi lô sự kiện là một transaction: các dòng mới, số dư mới của các tài khoản liên quan (chỉ giá trị sau cùng trong lô)
 * và vị trí của sự kiện cuối ({@code ledger_feed}) cùng commit hoặc cùng không. Khởi động lại, sink đọc vị trí đó và bỏ qua
 * những sự kiện đã có: mỗi sự kiện được ghi đúng một lần.</li>
 * <li>Cơ sở dữ liệu lỗi hay khởi động lại thì sink rollback, kết nối lại và thử lại lô đó (chờ tăng dần tới 5 giây), không
 * bỏ sự kiện nào. Trong lúc đó hàng đợi của {@code EventPublisher} đầy dần rồi chặn node phát sự kiện, nên sink thường
 * chạy trên một learner: việc ghi của cụm không bị ảnh hưởng.</li>
 * </ul>
 * Với PostgreSQL nên thêm {@code reWriteBatchedInserts=true} vào URL để driver gộp các INSERT của một lô.
 */
@Slf4j
public final class JdbcEventSink implements EventSink {
    private static final long MAX_BACKOFF_MS = 5_000;

    private final String url;
    private final String user;
    private final String password;
    private volatile boolean closing;

    private Connection connection;
    private PreparedStatement insertAccount;
    private PreparedStatement insertTransfer;
    private PreparedStatement insertEntry;
    private PreparedStatement updateAccount;
    private PreparedStatement updateFeed;

    private long lastIndex;
    private int lastPosition = -1;
    private long rowsWritten;

    public JdbcEventSink(String url, String user, String password) throws IOException {
        this.url = url;
        this.user = user;
        this.password = password;
        try {
            connect();
        } catch (SQLException e) {
            throw new IOException("cannot open the ledger view at " + url, e);
        }
    }

    private void connect() throws SQLException {
        closeQuietly();
        connection = DriverManager.getConnection(url, user, password);
        ViewSchema.create(connection);
        connection.setAutoCommit(false);
        try (var statement = connection.createStatement();
             var position = statement.executeQuery("SELECT log_index, log_position FROM ledger_feed WHERE id = 1")) {
            if (position.next()) {
                lastIndex = position.getLong(1);
                lastPosition = position.getInt(2);
            } else {
                statement.executeUpdate("INSERT INTO ledger_feed (id, log_index, log_position) VALUES (1, 0, -1)");
                lastIndex = 0;
                lastPosition = -1;
            }
        }
        connection.commit();
        insertAccount = connection.prepareStatement("INSERT INTO ledger_accounts "
                + "(id, ledger, flags, debits_posted, credits_posted, created_at_ms, updated_at_ms) VALUES (?, ?, ?, 0, 0, ?, ?)");
        insertTransfer = connection.prepareStatement("INSERT INTO ledger_transfers "
                + "(id, debit_account_id, credit_account_id, amount, ledger, posted_at_ms, log_index, log_position) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        insertEntry = connection.prepareStatement("INSERT INTO ledger_entries "
                + "(account_id, log_index, log_position, transfer_id, side, amount, debits_posted, credits_posted, posted_at_ms) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        updateAccount = connection.prepareStatement(
                "UPDATE ledger_accounts SET debits_posted = ?, credits_posted = ?, updated_at_ms = ? WHERE id = ?");
        updateFeed = connection.prepareStatement("UPDATE ledger_feed SET log_index = ?, log_position = ? WHERE id = 1");
    }

    @Override
    public void write(List<LedgerEvent> events) throws IOException {
        long backoff = 50;
        while (true) {
            try {
                writeOnce(events);
                return;
            } catch (SQLException e) {
                if (closing) {
                    throw new IOException("ledger view closed while writing", e);
                }
                log.warn("Ledger view write failed, retrying in {} ms: {}", backoff, e.getMessage());
                rollbackQuietly();
                sleep(backoff);
                backoff = Math.min(MAX_BACKOFF_MS, backoff * 2);
                try {
                    connect(); // vị trí đọc lại từ cơ sở dữ liệu: phần đã commit sẽ được bỏ qua
                } catch (SQLException again) {
                    log.warn("Ledger view reconnect failed: {}", again.getMessage());
                }
            }
        }
    }

    private void writeOnce(List<LedgerEvent> events) throws SQLException {
        if (connection == null) {
            throw new SQLException("not connected");
        }
        // số dư sau cùng của mỗi tài khoản trong lô: {nợ, có, thời điểm}
        Map<Long, long[]> balances = new LinkedHashMap<>();
        LedgerEvent newest = null;
        int accounts = 0;
        int transfers = 0;
        for (LedgerEvent event : events) {
            if (!event.isAfter(lastIndex, lastPosition) || (newest != null && !event.isAfter(newest.index(), newest.position()))) {
                continue;
            }
            newest = event;
            switch (event.type()) {
                case ACCOUNT_CREATED -> {
                    insertAccount.setLong(1, event.id());
                    insertAccount.setInt(2, event.ledger());
                    insertAccount.setInt(3, event.flags());
                    insertAccount.setLong(4, event.timestamp());
                    insertAccount.setLong(5, event.timestamp());
                    insertAccount.addBatch();
                    accounts++;
                }
                case TRANSFER_POSTED -> {
                    insertTransfer.setLong(1, event.id());
                    insertTransfer.setLong(2, event.debitAccountId());
                    insertTransfer.setLong(3, event.creditAccountId());
                    insertTransfer.setLong(4, event.amount());
                    insertTransfer.setInt(5, event.ledger());
                    insertTransfer.setLong(6, event.timestamp());
                    insertTransfer.setLong(7, event.index());
                    insertTransfer.setInt(8, event.position());
                    insertTransfer.addBatch();
                    entry(event, event.debitAccountId(), "D", event.debitAccountDebitsPosted(), event.debitAccountCreditsPosted());
                    entry(event, event.creditAccountId(), "C", event.creditAccountDebitsPosted(), event.creditAccountCreditsPosted());
                    balances.put(event.debitAccountId(),
                            new long[]{event.debitAccountDebitsPosted(), event.debitAccountCreditsPosted(), event.timestamp()});
                    balances.put(event.creditAccountId(),
                            new long[]{event.creditAccountDebitsPosted(), event.creditAccountCreditsPosted(), event.timestamp()});
                    transfers++;
                }
            }
        }
        if (newest == null) {
            return;
        }
        // tài khoản mới trước, rồi giao dịch và bút toán, rồi số dư: thứ tự này đúng cả khi tài khoản được tạo trong cùng lô
        if (accounts > 0) {
            insertAccount.executeBatch();
        }
        if (transfers > 0) {
            insertTransfer.executeBatch();
            insertEntry.executeBatch();
            for (var balance : balances.entrySet()) {
                updateAccount.setLong(1, balance.getValue()[0]);
                updateAccount.setLong(2, balance.getValue()[1]);
                updateAccount.setLong(3, balance.getValue()[2]);
                updateAccount.setLong(4, balance.getKey());
                updateAccount.addBatch();
            }
            updateAccount.executeBatch();
        }
        updateFeed.setLong(1, newest.index());
        updateFeed.setInt(2, newest.position());
        updateFeed.executeUpdate();
        connection.commit();
        lastIndex = newest.index();
        lastPosition = newest.position();
        rowsWritten += accounts + transfers * 3L + balances.size();
    }

    private void entry(LedgerEvent event, long account, String side, long debits, long credits) throws SQLException {
        insertEntry.setLong(1, account);
        insertEntry.setLong(2, event.index());
        insertEntry.setInt(3, event.position());
        insertEntry.setLong(4, event.id());
        insertEntry.setString(5, side);
        insertEntry.setLong(6, event.amount());
        insertEntry.setLong(7, debits);
        insertEntry.setLong(8, credits);
        insertEntry.setLong(9, event.timestamp());
        insertEntry.addBatch();
    }

    @Override
    public long lastIndex() {
        return lastIndex;
    }

    @Override
    public int lastPosition() {
        return lastPosition;
    }

    /** số dòng đã ghi hoặc cập nhật từ lúc mở; cho giám sát */
    public long rowsWritten() {
        return rowsWritten;
    }

    private void rollbackQuietly() {
        try {
            if (connection != null) {
                connection.rollback();
            }
        } catch (SQLException ignored) {
            // kết nối đã hỏng: sẽ mở lại
        }
        // lô dở dang trong các PreparedStatement không được mang sang lần thử sau
        for (var statement : new PreparedStatement[]{insertAccount, insertTransfer, insertEntry, updateAccount}) {
            try {
                if (statement != null) {
                    statement.clearBatch();
                }
            } catch (SQLException ignored) {
                // kết nối đã hỏng
            }
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void closeQuietly() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
                // đang bỏ kết nối này
            }
            connection = null;
        }
    }

    @Override
    public void close() {
        closing = true;
        closeQuietly();
    }
}

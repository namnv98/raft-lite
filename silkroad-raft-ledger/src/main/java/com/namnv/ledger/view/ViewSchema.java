package com.namnv.ledger.view;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Các bảng của phía truy vấn (CQRS), dựng lại hoàn toàn từ dòng sự kiện của sổ cái. SQL chuẩn, chạy trên PostgreSQL và H2.
 * Thời điểm là epoch ms ({@code *_at_ms}), đúng như trong sự kiện.
 * <pre>
 * ledger_accounts   một dòng mỗi tài khoản: sổ, cờ, tổng nợ / tổng có hiện tại
 * ledger_transfers  một dòng mỗi giao dịch đã ghi
 * ledger_entries    hai bút toán mỗi giao dịch (D: bên nợ, C: bên có) kèm tổng nợ / tổng có của tài khoản ngay sau bút
 *                   toán: sao kê của một tài khoản là các dòng của nó theo (log_index, log_position)
 * ledger_feed       vị trí của sự kiện cuối cùng đã ghi; cập nhật trong cùng transaction với dữ liệu nên mỗi sự kiện được
 *                   ghi đúng một lần
 * </pre>
 */
public final class ViewSchema {
    static final String[] TABLES = {
            """
            CREATE TABLE IF NOT EXISTS ledger_accounts (
                id BIGINT PRIMARY KEY,
                ledger INT NOT NULL,
                flags INT NOT NULL,
                debits_posted BIGINT NOT NULL,
                credits_posted BIGINT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL)""",
            """
            CREATE TABLE IF NOT EXISTS ledger_transfers (
                id BIGINT PRIMARY KEY,
                debit_account_id BIGINT NOT NULL,
                credit_account_id BIGINT NOT NULL,
                amount BIGINT NOT NULL,
                ledger INT NOT NULL,
                posted_at_ms BIGINT NOT NULL,
                log_index BIGINT NOT NULL,
                log_position INT NOT NULL)""",
            """
            CREATE TABLE IF NOT EXISTS ledger_entries (
                account_id BIGINT NOT NULL,
                log_index BIGINT NOT NULL,
                log_position INT NOT NULL,
                transfer_id BIGINT NOT NULL,
                side CHAR(1) NOT NULL,
                amount BIGINT NOT NULL,
                debits_posted BIGINT NOT NULL,
                credits_posted BIGINT NOT NULL,
                posted_at_ms BIGINT NOT NULL,
                PRIMARY KEY (account_id, log_index, log_position))""",
            """
            CREATE TABLE IF NOT EXISTS ledger_feed (
                id INT PRIMARY KEY,
                log_index BIGINT NOT NULL,
                log_position INT NOT NULL)""",
    };

    private ViewSchema() {
    }

    /** tạo các bảng nếu chưa có */
    public static void create(Connection connection) throws SQLException {
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(true);
        try (Statement statement = connection.createStatement()) {
            for (String table : TABLES) {
                statement.execute(table);
            }
        } finally {
            connection.setAutoCommit(autoCommit);
        }
    }
}

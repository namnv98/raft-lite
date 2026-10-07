package com.namnv.ledger.view;

import com.namnv.ledger.event.LedgerEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Phía truy vấn trên H2 (chế độ PostgreSQL): các bảng dựng từ sự kiện, sao kê, chống ghi trùng khi phát lại. */
class JdbcEventSinkTest {
    private static final int USD = 840;

    @TempDir
    Path dir;

    private LedgerView view;

    @AfterEach
    void tearDown() {
        if (view != null) {
            view.close();
        }
    }

    private String url() {
        return "jdbc:h2:file:" + dir.resolve("view") + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE";
    }

    // ngân hàng (1) nạp cho khách 2, khách 2 trả cho khách 3 hai lần, ở các entry và thời điểm khác nhau
    private static final List<LedgerEvent> EVENTS = List.of(
            LedgerEvent.accountCreated(3, 0, 1_000, 1, USD, 0),
            LedgerEvent.accountCreated(3, 1, 1_000, 2, USD, 1),
            LedgerEvent.accountCreated(3, 2, 1_000, 3, USD, 1),
            LedgerEvent.transferPosted(4, 0, 2_000, 100, 1, 2, 500, USD, 500, 0, 0, 500),
            LedgerEvent.transferPosted(5, 0, 3_000, 101, 2, 3, 120, USD, 120, 500, 0, 120),
            LedgerEvent.transferPosted(5, 1, 3_000, 102, 2, 3, 30, USD, 150, 500, 0, 150));

    @Test
    void eventsBecomeAccountsTransfersAndAStatement() throws Exception {
        try (var sink = new JdbcEventSink(url(), "sa", "")) {
            sink.write(EVENTS);
            assertEquals(5, sink.lastIndex());
            assertEquals(1, sink.lastPosition());
        }
        view = new LedgerView(url(), "sa", "", 2);
        var customer = view.account(2).get(5, TimeUnit.SECONDS);
        assertEquals(new LedgerView.Account(2, USD, 1, 150, 500, 1_000, 3_000), customer);
        assertEquals(350, customer.creditBalance());
        assertNull(view.account(99).get(5, TimeUnit.SECONDS));
        assertEquals(new LedgerView.Transfer(101, 2, 3, 120, USD, 3_000, 5, 0), view.transfer(101).get(5, TimeUnit.SECONDS));

        // sao kê của khách 2: mới trước cũ sau, mỗi bút toán kèm số dư ngay sau nó
        var statement = view.entries(2, 10, null).get(5, TimeUnit.SECONDS);
        assertEquals(List.of(
                new LedgerView.Entry(2, 5, 1, 102, "D", 30, 150, 500, 3_000),
                new LedgerView.Entry(2, 5, 0, 101, "D", 120, 120, 500, 3_000),
                new LedgerView.Entry(2, 4, 0, 100, "C", 500, 0, 500, 2_000)), statement);
        assertEquals(List.of(350L, 380L, 500L), statement.stream().map(LedgerView.Entry::creditBalance).toList());
        // phân trang: trang hai bắt đầu sau bút toán cuối của trang một
        var first = view.entries(2, 2, null).get(5, TimeUnit.SECONDS);
        var next = view.entries(2, 2, new LedgerView.Position(first.getLast().logIndex(), first.getLast().logPosition()))
                .get(5, TimeUnit.SECONDS);
        assertEquals(statement.subList(2, 3), next);
        assertEquals(new LedgerView.Position(5, 1), view.position().get(5, TimeUnit.SECONDS));
    }

    @Test
    void reopeningSkipsEverythingAlreadyWritten() throws Exception {
        try (var sink = new JdbcEventSink(url(), "sa", "")) {
            sink.write(EVENTS.subList(0, 4));
        }
        // node khởi động lại phát lại từ index 3: chỉ hai giao dịch cuối là mới
        try (var sink = new JdbcEventSink(url(), "sa", "")) {
            assertEquals(4, sink.lastIndex());
            sink.write(EVENTS);
            sink.write(EVENTS.subList(4, 6)); // và phát lại lần nữa: không đổi gì
        }
        try (var connection = DriverManager.getConnection(url(), "sa", "");
             var statement = connection.createStatement()) {
            for (var table : List.of("ledger_accounts:3", "ledger_transfers:3", "ledger_entries:6")) {
                var name = table.split(":")[0];
                try (var rows = statement.executeQuery("SELECT COUNT(*) FROM " + name)) {
                    rows.next();
                    assertEquals(Long.parseLong(table.split(":")[1]), rows.getLong(1), name);
                }
            }
            try (var rows = statement.executeQuery("SELECT debits_posted, credits_posted FROM ledger_accounts WHERE id = 3")) {
                rows.next();
                assertEquals(0, rows.getLong(1));
                assertEquals(150, rows.getLong(2));
            }
        }
    }
}

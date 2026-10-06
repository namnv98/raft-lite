package com.namnv.ledger;

import com.namnv.core.Status;
import com.namnv.entity.LogEntry;
import com.namnv.statemachine.snapshot.SnapshotReader;
import com.namnv.statemachine.snapshot.SnapshotWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.namnv.ledger.LedgerAccount.CREDITS_MUST_NOT_EXCEED_DEBITS;
import static com.namnv.ledger.LedgerAccount.DEBITS_MUST_NOT_EXCEED_CREDITS;
import static com.namnv.ledger.LedgerResult.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LedgerTest {
    private static final int USD = 840;
    private static final int VND = 704;

    @TempDir
    Path dir;

    private long index;
    private int ledgers;
    private final List<Ledger> opened = new ArrayList<>();

    // đoạn rất nhỏ (64 giao dịch): lịch sử xuống RocksDB ngay, nên mọi test đều đi qua cả đường đọc từ RocksDB
    private Ledger newLedger() {
        var ledger = new Ledger(dir.resolve("ledger" + ledgers++), 16, 1 << 16, 64);
        opened.add(ledger);
        return ledger;
    }

    @org.junit.jupiter.api.AfterEach
    void closeLedgers() {
        opened.forEach(Ledger::close);
    }

    private LedgerResult apply(Ledger ledger, byte[] command) {
        return LedgerCodec.result(ledger.onApplyWithResult("n", new LogEntry(++index, 1, command)));
    }

    private LedgerResult account(Ledger ledger, long id, int currency, int flags) {
        return apply(ledger, LedgerCodec.createAccount(new LedgerAccount(id, currency, flags)));
    }

    private LedgerResult transfer(Ledger ledger, long id, long debit, long credit, long amount, int currency) {
        return apply(ledger, LedgerCodec.transfer(new LedgerTransfer(id, debit, credit, amount, currency)));
    }

    // ngân hàng (1) được nợ tuỳ ý; khách hàng (2, 3) không được âm
    private Ledger bankWithTwoCustomers() {
        var ledger = newLedger();
        assertEquals(OK, account(ledger, 1, USD, 0));
        assertEquals(OK, account(ledger, 2, USD, DEBITS_MUST_NOT_EXCEED_CREDITS));
        assertEquals(OK, account(ledger, 3, USD, DEBITS_MUST_NOT_EXCEED_CREDITS));
        return ledger;
    }

    @Test
    void accountsAreCreatedOnceAndValidated() {
        var ledger = newLedger();
        assertEquals(OK, account(ledger, 7, USD, 0));
        assertEquals(EXISTS, account(ledger, 7, USD, 0));
        assertEquals(EXISTS_WITH_DIFFERENT_FIELDS, account(ledger, 7, VND, 0));
        assertEquals(EXISTS_WITH_DIFFERENT_FIELDS, account(ledger, 7, USD, DEBITS_MUST_NOT_EXCEED_CREDITS));
        assertEquals(ID_MUST_NOT_BE_ZERO, account(ledger, 0, USD, 0));
        assertEquals(LEDGER_MUST_NOT_BE_ZERO, account(ledger, 8, 0, 0));
        assertEquals(MALFORMED, account(ledger, 9, USD, DEBITS_MUST_NOT_EXCEED_CREDITS | CREDITS_MUST_NOT_EXCEED_DEBITS));
        assertEquals(MALFORMED, account(ledger, 9, USD, 64));
        assertEquals(1, ledger.totals().accounts());
    }

    @Test
    void transferPostsBothSidesAndKeepsTheBooksBalanced() {
        var ledger = bankWithTwoCustomers();
        assertEquals(OK, transfer(ledger, 100, 1, 2, 500, USD)); // ngân hàng nạp 500 cho khách 2
        assertEquals(OK, transfer(ledger, 101, 2, 3, 200, USD)); // khách 2 chuyển 200 cho khách 3
        assertEquals(new LedgerBalance(1, USD, 0, 500, 0), ledger.balance(1));
        assertEquals(300, ledger.balance(2).creditBalance());
        assertEquals(200, ledger.balance(3).creditBalance());
        var totals = ledger.totals();
        assertEquals(2, totals.transfers());
        assertEquals(700, totals.debitsPosted());
        assertTrue(totals.balanced());
    }

    @Test
    void customerCannotSpendMoreThanTheyHave() {
        var ledger = bankWithTwoCustomers();
        assertEquals(OK, transfer(ledger, 100, 1, 2, 100, USD));
        assertEquals(EXCEEDS_CREDITS, transfer(ledger, 101, 2, 3, 101, USD));
        assertEquals(OK, transfer(ledger, 102, 2, 3, 100, USD)); // đúng bằng số dư thì được
        assertEquals(0, ledger.balance(2).creditBalance());
        assertEquals(EXCEEDS_CREDITS, transfer(ledger, 103, 2, 3, 1, USD));
    }

    @Test
    void creditLimitedAccountCannotReceiveMoreThanItOwes() {
        var ledger = newLedger();
        assertEquals(OK, account(ledger, 1, USD, 0));
        assertEquals(OK, account(ledger, 2, USD, CREDITS_MUST_NOT_EXCEED_DEBITS)); // ví dụ khoản vay
        assertEquals(EXCEEDS_DEBITS, transfer(ledger, 10, 1, 2, 5, USD));
        assertEquals(OK, transfer(ledger, 11, 2, 1, 5, USD)); // giải ngân trước
        assertEquals(OK, transfer(ledger, 12, 1, 2, 5, USD)); // trả lại đúng số đã nợ
        assertEquals(EXCEEDS_DEBITS, transfer(ledger, 13, 1, 2, 1, USD));
    }

    @Test
    void invalidTransfersAreRejectedAndLeaveNoTrace() {
        var ledger = bankWithTwoCustomers();
        assertEquals(OK, account(ledger, 4, VND, 0));
        assertEquals(ID_MUST_NOT_BE_ZERO, transfer(ledger, 0, 1, 2, 5, USD));
        assertEquals(AMOUNT_MUST_BE_POSITIVE, transfer(ledger, 10, 1, 2, 0, USD));
        assertEquals(AMOUNT_MUST_BE_POSITIVE, transfer(ledger, 10, 1, 2, -5, USD));
        assertEquals(ACCOUNTS_MUST_BE_DIFFERENT, transfer(ledger, 10, 1, 1, 5, USD));
        assertEquals(DEBIT_ACCOUNT_NOT_FOUND, transfer(ledger, 10, 99, 2, 5, USD));
        assertEquals(CREDIT_ACCOUNT_NOT_FOUND, transfer(ledger, 10, 1, 99, 5, USD));
        assertEquals(LEDGER_MISMATCH, transfer(ledger, 10, 1, 4, 5, USD)); // USD sang tài khoản VND
        assertEquals(LEDGER_MISMATCH, transfer(ledger, 10, 1, 2, 5, VND)); // giao dịch khác sổ với tài khoản
        assertEquals(LEDGER_MUST_NOT_BE_ZERO, transfer(ledger, 10, 1, 2, 5, 0));
        assertEquals(MALFORMED, apply(ledger, new byte[]{LedgerCodec.TRANSFER, 1, 2}));
        assertEquals(0, ledger.totals().transfers());
        assertEquals(0, ledger.totals().debitsPosted());
        // id của một giao dịch bị từ chối vẫn dùng được: gửi lại sau khi điều kiện thay đổi thì thành công
        assertEquals(EXCEEDS_CREDITS, transfer(ledger, 20, 2, 3, 50, USD));
        assertEquals(OK, transfer(ledger, 21, 1, 2, 50, USD));
        assertEquals(OK, transfer(ledger, 20, 2, 3, 50, USD));
    }

    @Test
    void resendingATransferNeverPostsItTwice() {
        var ledger = bankWithTwoCustomers();
        assertEquals(OK, transfer(ledger, 100, 1, 2, 500, USD));
        assertEquals(EXISTS, transfer(ledger, 100, 1, 2, 500, USD));
        assertEquals(EXISTS_WITH_DIFFERENT_FIELDS, transfer(ledger, 100, 1, 2, 501, USD));
        assertEquals(EXISTS_WITH_DIFFERENT_FIELDS, transfer(ledger, 100, 1, 3, 500, USD));
        assertEquals(500, ledger.balance(2).creditBalance());
        assertEquals(1, ledger.totals().transfers());
    }

    @Test
    void overflowIsRejected() {
        var ledger = bankWithTwoCustomers();
        assertEquals(OK, transfer(ledger, 1, 1, 2, Long.MAX_VALUE, USD));
        assertEquals(OVERFLOW, transfer(ledger, 2, 1, 3, 1, USD));
    }

    @Test
    void queriesFindAccountsAndTransfers() {
        var ledger = bankWithTwoCustomers();
        assertEquals(OK, transfer(ledger, 100, 1, 2, 500, USD));
        var ids = List.of(2L, 42L, 1L);
        var balances = LedgerCodec.accounts(ids, ledger.query(LedgerCodec.lookupAccounts(ids)));
        assertEquals(new LedgerBalance(2, USD, DEBITS_MUST_NOT_EXCEED_CREDITS, 0, 500), balances.get(0));
        assertNull(balances.get(1));
        assertEquals(500, balances.get(2).debitsPosted());
        var transfers = LedgerCodec.transfers(List.of(100L, 101L),
                ledger.query(LedgerCodec.lookupTransfers(List.of(100L, 101L))));
        assertEquals(new LedgerTransfer(100, 1, 2, 500, USD), transfers.get(0));
        assertNull(transfers.get(1));
        assertEquals(ledger.totals(), LedgerCodec.totals(ledger.query(LedgerCodec.totals())));
    }

    @Test
    void historyMovesToRocksDbButStaysExactAndMemoryStaysBounded() throws Exception {
        var ledger = bankWithTwoCustomers();
        for (int i = 0; i < 20_000; i++) {
            assertEquals(OK, transfer(ledger, 1_000_000 + i, 1, 2 + (i % 2), 1 + i % 7, USD));
            // không bao giờ quá vài đoạn trong bộ nhớ, dù đã ghi hàng nghìn đoạn
            assertTrue(ledger.unwrittenTransferSegments() <= TransferStore.MAX_UNWRITTEN_SEGMENTS);
        }
        // giao dịch rất cũ (đã xuống RocksDB) vẫn bị nhận ra khi gửi lại
        assertEquals(EXISTS, transfer(ledger, 1_000_000, 1, 2, 1, USD));
        assertEquals(EXISTS_WITH_DIFFERENT_FIELDS, transfer(ledger, 1_000_000, 1, 2, 2, USD));
        assertEquals(EXISTS, transfer(ledger, 1_019_999, 1, 3, 1 + 19_999 % 7, USD));
        var ids = List.of(1_000_000L, 1_010_001L, 1_019_999L, 5L);
        var found = LedgerCodec.transfers(ids, ledger.query(LedgerCodec.lookupTransfers(ids)));
        assertEquals(new LedgerTransfer(1_000_000, 1, 2, 1, USD), found.get(0));
        assertEquals(new LedgerTransfer(1_010_001, 1, 3, 1 + 10_001 % 7, USD), found.get(1));
        assertEquals(new LedgerTransfer(1_019_999, 1, 3, 1 + 19_999 % 7, USD), found.get(2));
        assertNull(found.get(3));
        assertEquals(20_000, ledger.totals().transfers());
        assertTrue(ledger.totals().balanced());
    }

    // ---------- dòng sự kiện ----------

    // sink trong bộ nhớ, bỏ qua sự kiện không đứng sau vị trí cuối như sink thật
    private static final class ListSink implements EventSink {
        final List<LedgerEvent> events = new java.util.concurrent.CopyOnWriteArrayList<>();
        long lastIndex;
        int lastPosition = -1;

        @Override
        public void write(List<LedgerEvent> batch) {
            for (var event : batch) {
                if (event.isAfter(lastIndex, lastPosition)) {
                    events.add(event);
                    lastIndex = event.index();
                    lastPosition = event.position();
                }
            }
        }

        @Override
        public long lastIndex() {
            return lastIndex;
        }

        @Override
        public int lastPosition() {
            return lastPosition;
        }

        @Override
        public void close() {
        }
    }

    @Test
    void onlySuccessfulChangesBecomeEventsWithTheirPositionInTheBatch() throws Exception {
        var sink = new ListSink();
        var publisher = new EventPublisher(sink, 1000);
        var ledger = newLedger();
        ledger.publishTo(publisher);
        // một lô ở index 5: tạo hai tài khoản, một giao dịch bị từ chối, một giao dịch thành công
        ledger.onApplyWithResult("n", new LogEntry(5, 1, LedgerCodec.createAccount(new LedgerAccount(1, USD, 0))));
        ledger.onApplyWithResult("n", new LogEntry(5, 1, LedgerCodec.createAccount(new LedgerAccount(2, USD, DEBITS_MUST_NOT_EXCEED_CREDITS))));
        ledger.onApplyWithResult("n", new LogEntry(5, 1, LedgerCodec.transfer(new LedgerTransfer(10, 2, 1, 5, USD))));
        ledger.onApplyWithResult("n", new LogEntry(5, 1, LedgerCodec.transfer(new LedgerTransfer(11, 1, 2, 7, USD))));
        // gửi lại ở entry sau: EXISTS không sinh sự kiện
        ledger.onApplyWithResult("n", new LogEntry(6, 1, LedgerCodec.transfer(new LedgerTransfer(11, 1, 2, 7, USD))));
        var done = new CompletableFuture<Void>();
        publisher.afterPublished(() -> done.complete(null));
        done.get(5, TimeUnit.SECONDS);
        assertEquals(List.of(
                LedgerEvent.accountCreated(5, 0, 1, USD, 0),
                LedgerEvent.accountCreated(5, 1, 2, USD, DEBITS_MUST_NOT_EXCEED_CREDITS),
                LedgerEvent.transferPosted(5, 3, 11, 1, 2, 7, USD)), sink.events);
        publisher.close();
    }

    @Test
    void jsonLinesSinkSkipsReplayedEventsAndDropsATornLine() throws Exception {
        Path file = dir.resolve("events.jsonl");
        try (var sink = new JsonLinesEventSink(file)) {
            sink.write(List.of(LedgerEvent.accountCreated(3, 0, 1, USD, 0), LedgerEvent.transferPosted(4, 0, 9, 1, 2, 5, USD)));
        }
        // mất điện giữa lúc ghi dòng kế tiếp
        Files.writeString(file, "{\"index\":5,\"posi", java.nio.file.StandardOpenOption.APPEND);
        try (var sink = new JsonLinesEventSink(file)) {
            assertEquals(4, sink.lastIndex());
            assertEquals(0, sink.lastPosition());
            // node khởi động lại phát lại từ index 3: chỉ sự kiện mới được ghi
            sink.write(List.of(LedgerEvent.accountCreated(3, 0, 1, USD, 0), LedgerEvent.transferPosted(4, 0, 9, 1, 2, 5, USD),
                    LedgerEvent.transferPosted(5, 0, 10, 1, 2, 6, USD)));
        }
        var lines = Files.readAllLines(file);
        assertEquals(3, lines.size());
        assertTrue(lines.get(2).contains("\"id\":10"));
    }

    @Test
    void idFilterNeverForgetsAnIdAndRarelyClaimsAnUnknownOne() {
        var filter = new IdFilter(1 << 16);
        var random = new java.util.Random(7);
        var added = new long[200_000]; // hơn ba đoạn: filter phải tự thêm đoạn
        for (int i = 0; i < added.length; i++) {
            added[i] = random.nextLong();
            filter.add(added[i]);
        }
        for (long id : added) {
            assertTrue(filter.mightContain(id));
        }
        int falsePositives = 0;
        for (int i = 0; i < 100_000; i++) {
            falsePositives += filter.mightContain(random.nextLong()) ? 1 : 0;
        }
        // mỗi đoạn khoảng 1%, bốn đoạn thì vài phần trăm
        assertTrue(falsePositives < 6_000, "false positives: " + falsePositives);
    }

    @Test
    void snapshotRestoresTheExactBooks() throws Exception {
        var ledger = bankWithTwoCustomers();
        for (int i = 0; i < 3000; i++) { // đủ nhiều để các mảng phải nới ra vài lần
            assertEquals(OK, account(ledger, 1000 + i, USD, 0));
            assertEquals(OK, transfer(ledger, 50_000 + i, 1, 1000 + i, i + 1, USD));
        }
        Path snapshot = Files.createDirectories(dir.resolve("snapshot"));
        var writer = new SnapshotWriter(snapshot.toString(), new ArrayList<>());
        var saved = new CompletableFuture<Status>();
        ledger.onSnapshotSave(writer, saved::complete);
        // ghi tiếp sau lúc chụp: không được lọt vào snapshot
        assertEquals(OK, transfer(ledger, 99_999, 1, 2, 7, USD));
        assertTrue(saved.get(10, TimeUnit.SECONDS).isOk());

        var restored = newLedger();
        assertEquals(OK, account(restored, 77, USD, 0)); // state cũ phải bị thay hoàn toàn
        assertTrue(restored.onSnapshotLoad(new SnapshotReader(snapshot.toString())));
        assertNull(restored.balance(77));
        assertEquals(new LedgerTotals(3003, 3000, ledger.totals().debitsPosted() - 7, ledger.totals().creditsPosted() - 7),
                restored.totals());
        var ids = new ArrayList<Long>();
        for (long i = 1000; i < 4000; i += 97) {
            ids.add(i);
        }
        assertArrayEquals(ledger.query(LedgerCodec.lookupAccounts(ids)), restored.query(LedgerCodec.lookupAccounts(ids)));
        assertEquals(EXISTS, transfer(restored, 50_000, 1, 1000, 1, USD)); // chống trùng vẫn còn sau khi nạp
        // tài khoản tạo sau khi nạp dùng lại vị trí trong mảng: phải bắt đầu từ 0
        assertEquals(OK, account(restored, 5_000_000, USD, 0));
        assertEquals(new LedgerBalance(5_000_000, USD, 0, 0, 0), restored.balance(5_000_000));
        assertTrue(Arrays.asList(LedgerResult.values()).contains(EXISTS));
    }
}

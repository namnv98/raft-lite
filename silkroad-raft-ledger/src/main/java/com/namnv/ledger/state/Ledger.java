package com.namnv.ledger.state;

import com.namnv.entity.LogEntry;
import com.namnv.ledger.codec.LedgerCodec;
import com.namnv.ledger.event.EventPublisher;
import com.namnv.ledger.event.LedgerEvent;
import com.namnv.ledger.model.LedgerAccount;
import com.namnv.ledger.model.LedgerBalance;
import com.namnv.ledger.model.LedgerResult;
import com.namnv.ledger.model.LedgerTotals;
import com.namnv.raft.StateMachine;
import com.namnv.storage.snapshot.SnapshotReader;
import com.namnv.storage.snapshot.SnapshotWriter;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Sổ cái kép (double-entry) làm state machine của Raft, theo cách của Binance Ledger và TigerBeetle:
 * <ul>
 * <li>Mọi lệnh được apply lần lượt trên một thread (thread của node), nên không có khoá và không có tranh chấp. Một tài
 * khoản "nóng" nhận hàng nghìn giao dịch mỗi giây không làm chậm hệ thống như khoá dòng trong cơ sở dữ liệu quan hệ.</li>
 * <li>Mỗi giao dịch cộng cùng một số tiền vào tổng nợ của tài khoản nợ và tổng có của tài khoản có, nên tổng nợ của cả
 * sổ luôn bằng tổng có ({@link LedgerTotals#balanced()}).</li>
 * <li>Mọi kiểm tra (tài khoản tồn tại, cùng sổ, không vượt số dư, không trùng id...) chỉ phụ thuộc vào state và lệnh,
 * nên mọi node tính ra cùng kết quả, và kết quả đó được gửi về client ({@link LedgerResult}).</li>
 * <li>Id của giao dịch do client chọn và là duy nhất: gửi lại một giao dịch đã ghi trả về {@link LedgerResult#EXISTS},
 * không ghi lần hai. Vì vậy client không cần phiên chống trùng của Raft và vẫn nhận đúng kết quả khi gửi lại.</li>
 * <li>Tài khoản nằm trong bộ nhớ dưới dạng các mảng số nguyên thuỷ. Lịch sử giao dịch nằm trong {@link TransferStore}:
 * phần mới trong bộ nhớ, phần cũ trong RocksDB, nên bộ nhớ không tăng theo số giao dịch.</li>
 * </ul>
 * Như mọi state machine của Silk Road Raft, sổ cái bắt đầu rỗng mỗi lần khởi động (thư mục của nó bị xoá) rồi được dựng
 * lại từ snapshot và phần log sau nó.
 */
@Slf4j
public class Ledger implements StateMachine, AutoCloseable {
    private static final String SNAPSHOT_FILE = "ledger.data";
    private static final int SNAPSHOT_MAGIC = 0x4C454733; // "LEG3"
    private static final int INITIAL_ACCOUNTS = 1024;

    // tài khoản: vị trí i trong các mảng
    private final LongIndex accountIndex;
    private long[] accountIds;
    private int[] accountLedgers;
    private int[] accountFlags;
    private long[] debitsPosted;
    private long[] creditsPosted;
    private int accountCount;

    private final TransferStore transfers;
    // chỗ đọc một giao dịch: {nợ, có, số tiền, ledger, thời điểm}
    private final long[] found = new long[5];

    private long totalDebits;
    private long totalCredits;

    // dòng sự kiện (null: không phát); vị trí của lệnh đang apply và của sự kiện cuối cùng đã phát
    private EventPublisher events;
    private boolean loading;
    private long applyingIndex = -1;
    private int applyingPosition;
    // thời điểm leader tạo entry đang apply: thời gian của mọi thay đổi trong entry đó
    private long applyingTimestamp;
    private long lastEventIndex;
    private int lastEventPosition = -1;

    /** lịch sử giao dịch trong {@code dir}, với dung lượng mặc định */
    public Ledger(Path dir) {
        this(dir, INITIAL_ACCOUNTS, 1_000_000, 1 << 20);
    }

    /**
     * @param dir                thư mục của lịch sử giao dịch (bị xoá khi khởi tạo)
     * @param expectedAccounts   cấp phát sẵn chỗ cho chừng này tài khoản (các mảng tự nới khi đầy, nhưng nới mảng lớn làm
     *                           thread của node đứng lại)
     * @param expectedTransfers  kích thước mỗi đoạn của bloom filter (khoảng 1,5 byte mỗi giao dịch); vượt quá thì thêm đoạn
     * @param segmentTransfers   số giao dịch mới giữ trong bộ nhớ trước khi ghi xuống RocksDB (mỗi giao dịch khoảng 60 byte)
     */
    public Ledger(Path dir, int expectedAccounts, long expectedTransfers, int segmentTransfers) {
        int accounts = Math.max(16, expectedAccounts);
        accountIndex = new LongIndex(accounts);
        accountIds = new long[accounts];
        accountLedgers = new int[accounts];
        accountFlags = new int[accounts];
        debitsPosted = new long[accounts];
        creditsPosted = new long[accounts];
        transfers = new TransferStore(dir, Math.max(16, segmentTransfers), expectedTransfers);
    }

    /**
     * Phát mọi thay đổi đã commit (tài khoản mới, giao dịch đã ghi) ra {@code publisher}. Gọi trước khi node khởi động.
     * Snapshot chỉ được báo xong khi mọi sự kiện tới thời điểm chụp đã nằm trong sink, nên khởi động lại không làm mất sự
     * kiện nào; sự kiện được sinh lại sau khi khởi động lại thì sink tự bỏ qua theo vị trí của chúng.
     */
    public synchronized void publishTo(EventPublisher publisher) {
        this.events = publisher;
    }

    private void emit(LedgerEvent event) {
        if (loading) {
            return; // tài khoản dựng lại từ snapshot không phải thay đổi mới
        }
        lastEventIndex = event.index();
        lastEventPosition = event.position();
        if (events != null) {
            events.publish(event);
        }
    }

    // ---------- ghi ----------

    @Override
    public void onApply(String node, LogEntry entry) {
        onApplyWithResult(node, entry);
    }

    @Override
    public synchronized byte[] onApplyWithResult(String node, LogEntry entry) {
        byte[] command = entry.getCommand();
        if (command == null || command.length == 0) {
            return null;
        }
        // các lệnh của một lô có chung index: vị trí trong lô phân biệt chúng
        if (entry.getIndex() != applyingIndex) {
            applyingIndex = entry.getIndex();
            applyingPosition = 0;
        } else {
            applyingPosition++;
        }
        applyingTimestamp = entry.getTimestamp();
        switch (command[0]) {
            case LedgerCodec.CREATE_ACCOUNT -> {
                if (command.length != LedgerCodec.CREATE_ACCOUNT_BYTES) {
                    return LedgerResult.MALFORMED.bytes();
                }
                var in = ByteBuffer.wrap(command, 1, command.length - 1);
                return createAccount(in.getLong(), in.getInt(), in.getInt()).bytes();
            }
            case LedgerCodec.TRANSFER -> {
                if (command.length != LedgerCodec.TRANSFER_BYTES) {
                    return LedgerResult.MALFORMED.bytes();
                }
                var in = ByteBuffer.wrap(command, 1, command.length - 1);
                return transfer(in.getLong(), in.getLong(), in.getLong(), in.getLong(), in.getInt()).bytes();
            }
            default -> {
                return null; // không phải lệnh của sổ cái: mọi node bỏ qua giống nhau
            }
        }
    }

    private LedgerResult createAccount(long id, int ledger, int flags) {
        if (id == 0) {
            return LedgerResult.ID_MUST_NOT_BE_ZERO;
        }
        if (ledger == 0) {
            return LedgerResult.LEDGER_MUST_NOT_BE_ZERO;
        }
        int both = LedgerAccount.DEBITS_MUST_NOT_EXCEED_CREDITS | LedgerAccount.CREDITS_MUST_NOT_EXCEED_DEBITS;
        if ((flags & ~both) != 0 || flags == both) {
            return LedgerResult.MALFORMED; // cờ lạ, hoặc hai ràng buộc ngược nhau
        }
        int existing = accountIndex.get(id);
        if (existing >= 0) {
            return accountLedgers[existing] == ledger && accountFlags[existing] == flags
                    ? LedgerResult.EXISTS : LedgerResult.EXISTS_WITH_DIFFERENT_FIELDS;
        }
        // mọi cấp phát xảy ra trước khi state đổi: hết bộ nhớ thì lệnh không để lại gì dở dang
        if (accountCount == accountIds.length) {
            int capacity = accountIds.length * 2;
            accountIds = Arrays.copyOf(accountIds, capacity);
            accountLedgers = Arrays.copyOf(accountLedgers, capacity);
            accountFlags = Arrays.copyOf(accountFlags, capacity);
            debitsPosted = Arrays.copyOf(debitsPosted, capacity);
            creditsPosted = Arrays.copyOf(creditsPosted, capacity);
        }
        accountIndex.reserve(1);
        int slot = accountCount++;
        accountIds[slot] = id;
        accountLedgers[slot] = ledger;
        accountFlags[slot] = flags;
        // vị trí có thể đã được dùng trước khi nạp snapshot
        debitsPosted[slot] = 0;
        creditsPosted[slot] = 0;
        accountIndex.put(id, slot);
        emit(LedgerEvent.accountCreated(applyingIndex, applyingPosition, applyingTimestamp, id, ledger, flags));
        return LedgerResult.OK;
    }

    private LedgerResult transfer(long id, long debitId, long creditId, long amount, int ledger) {
        if (id == 0) {
            return LedgerResult.ID_MUST_NOT_BE_ZERO;
        }
        if (ledger == 0) {
            return LedgerResult.LEDGER_MUST_NOT_BE_ZERO;
        }
        if (amount <= 0) {
            return LedgerResult.AMOUNT_MUST_BE_POSITIVE;
        }
        if (debitId == creditId) {
            return LedgerResult.ACCOUNTS_MUST_BE_DIFFERENT;
        }
        if (transfers.find(id, found)) {
            return found[0] == debitId && found[1] == creditId && found[2] == amount && found[3] == ledger
                    ? LedgerResult.EXISTS : LedgerResult.EXISTS_WITH_DIFFERENT_FIELDS;
        }
        int debit = accountIndex.get(debitId);
        if (debit < 0) {
            return LedgerResult.DEBIT_ACCOUNT_NOT_FOUND;
        }
        int credit = accountIndex.get(creditId);
        if (credit < 0) {
            return LedgerResult.CREDIT_ACCOUNT_NOT_FOUND;
        }
        if (accountLedgers[debit] != ledger || accountLedgers[credit] != ledger) {
            return LedgerResult.LEDGER_MISMATCH;
        }
        long newDebits = debitsPosted[debit] + amount;
        long newCredits = creditsPosted[credit] + amount;
        if (newDebits < 0 || newCredits < 0 || totalDebits + amount < 0 || totalCredits + amount < 0) {
            return LedgerResult.OVERFLOW;
        }
        if ((accountFlags[debit] & LedgerAccount.DEBITS_MUST_NOT_EXCEED_CREDITS) != 0 && newDebits > creditsPosted[debit]) {
            return LedgerResult.EXCEEDS_CREDITS;
        }
        if ((accountFlags[credit] & LedgerAccount.CREDITS_MUST_NOT_EXCEED_DEBITS) != 0 && newCredits > debitsPosted[credit]) {
            return LedgerResult.EXCEEDS_DEBITS;
        }
        // mọi cấp phát (và việc chờ thread ghi lịch sử) xảy ra trước khi state đổi: giao dịch không bao giờ dở dang
        transfers.reserve();
        debitsPosted[debit] = newDebits;
        creditsPosted[credit] = newCredits;
        totalDebits += amount;
        totalCredits += amount;
        transfers.add(id, debitId, creditId, amount, ledger, applyingTimestamp);
        emit(LedgerEvent.transferPosted(applyingIndex, applyingPosition, applyingTimestamp, id, debitId, creditId, amount,
                ledger, debitsPosted[debit], creditsPosted[debit], debitsPosted[credit], creditsPosted[credit]));
        return LedgerResult.OK;
    }

    // ---------- đọc ----------

    /** Trả lời một truy vấn của {@link LedgerCodec}; dùng làm hàm truy vấn của {@code RaftClientService}. */
    public synchronized byte[] query(byte[] request) {
        if (request == null || request.length == 0) {
            throw new IllegalArgumentException("empty ledger query");
        }
        switch (request[0]) {
            case LedgerCodec.LOOKUP_ACCOUNTS -> {
                var in = ids(request);
                int count = in.remaining() / Long.BYTES;
                var out = LedgerCodec.accountRows(count);
                for (int i = 0; i < count; i++) {
                    int slot = accountIndex.get(in.getLong());
                    if (slot < 0) {
                        out.put((byte) 0).putInt(0).putInt(0).putLong(0).putLong(0);
                    } else {
                        out.put((byte) 1).putInt(accountLedgers[slot]).putInt(accountFlags[slot])
                                .putLong(debitsPosted[slot]).putLong(creditsPosted[slot]);
                    }
                }
                return out.array();
            }
            case LedgerCodec.LOOKUP_TRANSFERS -> {
                var in = ids(request);
                int count = in.remaining() / Long.BYTES;
                var out = LedgerCodec.transferRows(count);
                for (int i = 0; i < count; i++) {
                    if (transfers.find(in.getLong(), found)) {
                        out.put((byte) 1).putLong(found[0]).putLong(found[1]).putLong(found[2]).putInt((int) found[3])
                                .putLong(found[4]);
                    } else {
                        out.put((byte) 0).putLong(0).putLong(0).putLong(0).putInt(0).putLong(0);
                    }
                }
                return out.array();
            }
            case LedgerCodec.TOTALS -> {
                return ByteBuffer.allocate(32).putLong(accountCount).putLong(transfers.count())
                        .putLong(totalDebits).putLong(totalCredits).array();
            }
            default -> throw new IllegalArgumentException("unknown ledger query " + request[0]);
        }
    }

    private static ByteBuffer ids(byte[] request) {
        var in = ByteBuffer.wrap(request, 1, request.length - 1);
        int count = in.getInt();
        if (count < 0 || count * (long) Long.BYTES != in.remaining()) {
            throw new IllegalArgumentException("malformed ledger query");
        }
        return in;
    }

    /** tổng hiện tại, cho test và giám sát */
    public synchronized LedgerTotals totals() {
        return new LedgerTotals(accountCount, transfers.count(), totalDebits, totalCredits);
    }

    /** tài khoản, hoặc null; cho test và giám sát */
    public synchronized LedgerBalance balance(long id) {
        int slot = accountIndex.get(id);
        return slot < 0 ? null
                : new LedgerBalance(id, accountLedgers[slot], accountFlags[slot], debitsPosted[slot], creditsPosted[slot]);
    }

    /** số đoạn giao dịch đang nằm trong bộ nhớ chờ ghi xuống RocksDB; cho test và giám sát */
    public int unwrittenTransferSegments() {
        return transfers.unwrittenSegments();
    }

    // ---------- snapshot ----------

    // các tài khoản và tổng tại thời điểm chụp
    private record Accounts(long[] ids, int[] ledgers, int[] flags, long[] debits, long[] credits,
                            long transferCount, long totalDebits, long totalCredits, long lastEventIndex,
                            int lastEventPosition) {
    }

    @Override
    public synchronized CompletableFuture<Void> onSnapshotSave(SnapshotWriter writer) {
        var done = new CompletableFuture<Void>();
        // Trong lock của node: chép các mảng tài khoản (nhỏ so với lịch sử giao dịch) và để kho giao dịch tạo checkpoint
        // RocksDB đúng lúc mọi giao dịch tới thời điểm này đã xuống đĩa. Việc ghi file diễn ra ở thread nền.
        int a = accountCount;
        var image = new Accounts(Arrays.copyOf(accountIds, a), Arrays.copyOf(accountLedgers, a), Arrays.copyOf(accountFlags, a),
                Arrays.copyOf(debitsPosted, a), Arrays.copyOf(creditsPosted, a), transfers.count(), totalDebits, totalCredits,
                lastEventIndex, lastEventPosition);
        // snapshot chỉ xong khi mọi sự kiện tới thời điểm này đã nằm trong sink: sau khi khởi động lại từ snapshot này,
        // không sự kiện nào trước nó cần được phát lại
        var eventsWritten = new CompletableFuture<Void>();
        if (events != null) {
            events.afterPublished(() -> eventsWritten.complete(null));
        } else {
            eventsWritten.complete(null);
        }
        transfers.snapshot(Path.of(writer.getPath()), dir -> {
            eventsWritten.join();
            return List.of(writeAccounts(dir, image));
        }, (files, error) -> {
            if (error != null) {
                log.error("Ledger snapshot save failed", error);
                done.completeExceptionally(error);
                return;
            }
            files.forEach(writer::addFile);
            log.info("Ledger snapshot saved: {} accounts, {} transfers", image.ids().length, image.transferCount());
            done.complete(null);
        });
        return done;
    }

    private static String writeAccounts(Path dir, Accounts image) throws IOException {
        try (var stream = new FileOutputStream(dir.resolve(SNAPSHOT_FILE).toFile());
             var out = new DataOutputStream(new BufferedOutputStream(stream, 1 << 20))) {
            out.writeInt(SNAPSHOT_MAGIC);
            out.writeLong(image.transferCount());
            out.writeLong(image.totalDebits());
            out.writeLong(image.totalCredits());
            out.writeLong(image.lastEventIndex());
            out.writeInt(image.lastEventPosition());
            out.writeInt(image.ids().length);
            for (int i = 0; i < image.ids().length; i++) {
                out.writeLong(image.ids()[i]);
                out.writeInt(image.ledgers()[i]);
                out.writeInt(image.flags()[i]);
                out.writeLong(image.debits()[i]);
                out.writeLong(image.credits()[i]);
            }
            out.flush();
            stream.getFD().sync();
        }
        return SNAPSHOT_FILE;
    }

    @Override
    public synchronized boolean onSnapshotLoad(SnapshotReader reader) {
        Path dir = Path.of(reader.getPath());
        try {
            accountIndex.clear();
            accountCount = 0;
            totalDebits = 0;
            totalCredits = 0;
            applyingIndex = -1;
            lastEventIndex = 0;
            lastEventPosition = -1;
            transfers.load(dir);
            transfers.setCount(0);
            Path file = dir.resolve(SNAPSHOT_FILE);
            if (!Files.exists(file)) {
                return true;
            }
            loading = true;
            try (var in = new DataInputStream(new BufferedInputStream(new FileInputStream(file.toFile()), 1 << 20))) {
                if (in.readInt() != SNAPSHOT_MAGIC) {
                    throw new IOException("not a ledger snapshot");
                }
                transfers.setCount(in.readLong());
                long debits = in.readLong();
                long credits = in.readLong();
                long eventIndex = in.readLong();
                int eventPosition = in.readInt();
                int accounts = in.readInt();
                for (int i = 0; i < accounts; i++) {
                    long id = in.readLong();
                    int ledger = in.readInt();
                    int flags = in.readInt();
                    if (createAccount(id, ledger, flags) != LedgerResult.OK) {
                        throw new IOException("bad account " + id + " in ledger snapshot");
                    }
                    debitsPosted[accountCount - 1] = in.readLong();
                    creditsPosted[accountCount - 1] = in.readLong();
                }
                totalDebits = debits;
                totalCredits = credits;
                lastEventIndex = eventIndex;
                lastEventPosition = eventPosition;
                // createAccount ở trên không được tính là sự kiện mới
                if (events != null && events.sinkIndex() < eventIndex) {
                    // snapshot mang state mà sink chưa từng nhận sự kiện của nó (ví dụ node tụt quá xa và nhận snapshot
                    // từ leader): những sự kiện đó không phát lại được, nơi nhận phải đồng bộ lại từ một bản chụp
                    log.error("Ledger event feed has a gap: the sink stops at index {} but the snapshot covers events up "
                            + "to index {}; consumers must resynchronise from a ledger snapshot", events.sinkIndex(), eventIndex);
                }
            }
            log.info("Ledger snapshot loaded: {} accounts, {} transfers", accountCount, transfers.count());
            return true;
        } catch (IOException | RuntimeException e) {
            log.error("Ledger snapshot load failed", e);
            return false;
        } finally {
            loading = false;
        }
    }

    @Override
    public void close() {
        transfers.close();
    }
}

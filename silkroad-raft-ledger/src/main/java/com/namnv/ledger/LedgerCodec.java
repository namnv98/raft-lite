package com.namnv.ledger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Dạng nhị phân của lệnh, truy vấn và kết quả của sổ cái (big-endian, độ dài cố định):
 * <pre>
 * tạo tài khoản:   [1][id 8][ledger 4][flags 4]                                          17 byte
 * chuyển tiền:     [2][id 8][tài khoản nợ 8][tài khoản có 8][số tiền 8][ledger 4]          37 byte
 * kết quả:         [mã 1]
 * tra tài khoản:   [10][số id 4][id 8]...   -> [số 4]([có 1][ledger 4][flags 4][nợ 8][có 8])...
 * tra giao dịch:   [11][số id 4][id 8]...   -> [số 4]([có 1][nợ 8][có 8][số tiền 8][ledger 4])...
 * tổng:            [12]                     -> [số tài khoản 8][số giao dịch 8][tổng nợ 8][tổng có 8]
 * </pre>
 */
public final class LedgerCodec {
    static final byte CREATE_ACCOUNT = 1;
    static final byte TRANSFER = 2;
    static final byte LOOKUP_ACCOUNTS = 10;
    static final byte LOOKUP_TRANSFERS = 11;
    static final byte TOTALS = 12;

    static final int CREATE_ACCOUNT_BYTES = 1 + 8 + 4 + 4;
    static final int TRANSFER_BYTES = 1 + 8 + 8 + 8 + 8 + 4;
    private static final int ACCOUNT_ROW_BYTES = 1 + 4 + 4 + 8 + 8;
    private static final int TRANSFER_ROW_BYTES = 1 + 8 + 8 + 8 + 4;

    private LedgerCodec() {
    }

    public static byte[] createAccount(LedgerAccount account) {
        return ByteBuffer.allocate(CREATE_ACCOUNT_BYTES).put(CREATE_ACCOUNT)
                .putLong(account.id()).putInt(account.ledger()).putInt(account.flags()).array();
    }

    public static byte[] transfer(LedgerTransfer transfer) {
        return ByteBuffer.allocate(TRANSFER_BYTES).put(TRANSFER)
                .putLong(transfer.id()).putLong(transfer.debitAccountId()).putLong(transfer.creditAccountId())
                .putLong(transfer.amount()).putInt(transfer.ledger()).array();
    }

    public static LedgerResult result(byte[] result) {
        if (result == null || result.length != 1) {
            throw new IllegalArgumentException("not a ledger result");
        }
        return LedgerResult.of(result[0]);
    }

    // ---------- truy vấn ----------

    public static byte[] lookupAccounts(List<Long> ids) {
        return lookup(LOOKUP_ACCOUNTS, ids);
    }

    public static byte[] lookupTransfers(List<Long> ids) {
        return lookup(LOOKUP_TRANSFERS, ids);
    }

    public static byte[] totals() {
        return new byte[]{TOTALS};
    }

    private static byte[] lookup(byte op, List<Long> ids) {
        var buffer = ByteBuffer.allocate(1 + 4 + 8 * ids.size()).put(op).putInt(ids.size());
        ids.forEach(buffer::putLong);
        return buffer.array();
    }

    /** kết quả của {@link #lookupAccounts}: null ở vị trí của id không có */
    public static List<LedgerBalance> accounts(List<Long> ids, byte[] answer) {
        var in = ByteBuffer.wrap(answer);
        int count = in.getInt();
        var result = new ArrayList<LedgerBalance>(count);
        for (int i = 0; i < count; i++) {
            boolean found = in.get() != 0;
            int ledger = in.getInt();
            int flags = in.getInt();
            long debits = in.getLong();
            long credits = in.getLong();
            result.add(found ? new LedgerBalance(ids.get(i), ledger, flags, debits, credits) : null);
        }
        return result;
    }

    /** kết quả của {@link #lookupTransfers}: null ở vị trí của id không có */
    public static List<LedgerTransfer> transfers(List<Long> ids, byte[] answer) {
        var in = ByteBuffer.wrap(answer);
        int count = in.getInt();
        var result = new ArrayList<LedgerTransfer>(count);
        for (int i = 0; i < count; i++) {
            boolean found = in.get() != 0;
            long debit = in.getLong();
            long credit = in.getLong();
            long amount = in.getLong();
            int ledger = in.getInt();
            result.add(found ? new LedgerTransfer(ids.get(i), debit, credit, amount, ledger) : null);
        }
        return result;
    }

    public static LedgerTotals totals(byte[] answer) {
        var in = ByteBuffer.wrap(answer);
        return new LedgerTotals(in.getLong(), in.getLong(), in.getLong(), in.getLong());
    }

    // ---------- phía state machine ----------

    static ByteBuffer accountRows(int count) {
        return ByteBuffer.allocate(4 + count * ACCOUNT_ROW_BYTES).putInt(count);
    }

    static ByteBuffer transferRows(int count) {
        return ByteBuffer.allocate(4 + count * TRANSFER_ROW_BYTES).putInt(count);
    }
}

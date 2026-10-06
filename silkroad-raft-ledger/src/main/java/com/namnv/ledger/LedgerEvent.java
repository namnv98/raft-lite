package com.namnv.ledger;

/**
 * Một thay đổi đã được commit và apply vào sổ cái: tài khoản mới hoặc giao dịch đã ghi. Lệnh bị từ chối hay đã có từ
 * trước ({@link LedgerResult#EXISTS}) không sinh sự kiện.
 * <p>
 * {@code (index, position)} là vị trí của sự kiện trong dòng sự kiện, tăng dần và giống nhau trên mọi node: index của entry
 * trong Raft log, và thứ tự của lệnh trong lô của entry đó. Khi một node dựng lại sổ cái từ snapshot và log, các sự kiện sau
 * snapshot được sinh lại với đúng vị trí cũ, nên nơi nhận bỏ qua được những gì đã có.
 */
public record LedgerEvent(long index, int position, Type type, long id, long debitAccountId, long creditAccountId,
                          long amount, int ledger, int flags) {

    public enum Type {
        ACCOUNT_CREATED,
        TRANSFER_POSTED
    }

    static LedgerEvent accountCreated(long index, int position, long id, int ledger, int flags) {
        return new LedgerEvent(index, position, Type.ACCOUNT_CREATED, id, 0, 0, 0, ledger, flags);
    }

    static LedgerEvent transferPosted(long index, int position, long id, long debit, long credit, long amount, int ledger) {
        return new LedgerEvent(index, position, Type.TRANSFER_POSTED, id, debit, credit, amount, ledger, 0);
    }

    /** true nếu sự kiện này đứng sau vị trí (index, position) */
    public boolean isAfter(long index, int position) {
        return this.index > index || (this.index == index && this.position > position);
    }
}

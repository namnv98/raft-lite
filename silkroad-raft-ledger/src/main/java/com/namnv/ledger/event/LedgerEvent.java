package com.namnv.ledger.event;

/**
 * Một thay đổi đã được commit và apply vào sổ cái: tài khoản mới hoặc giao dịch đã ghi. Lệnh bị từ chối hay đã có từ
 * trước ({@code EXISTS}) không sinh sự kiện.
 * <p>
 * {@code (index, position)} là vị trí của sự kiện trong dòng sự kiện, tăng dần và giống nhau trên mọi node: index của entry
 * trong Raft log, và thứ tự của lệnh trong lô của entry đó. Khi một node dựng lại sổ cái từ snapshot và log, các sự kiện sau
 * snapshot được sinh lại với đúng vị trí cũ, nên nơi nhận bỏ qua được những gì đã có.
 * <p>
 * {@code timestamp} (epoch ms) là thời điểm leader tạo entry, giống nhau trên mọi node và không giảm dọc theo dòng sự kiện.
 * Với {@link Type#TRANSFER_POSTED}, bốn trường cuối là tổng nợ / tổng có của tài khoản nợ và của tài khoản có ngay sau
 * giao dịch: mỗi giao dịch là hai bút toán (một bên nợ, một bên có) và nơi nhận dựng được sao kê của từng tài khoản kèm số
 * dư sau mỗi bút toán mà không phải tự cộng dồn từ đầu.
 */
public record LedgerEvent(long index, int position, Type type, long timestamp, long id, long debitAccountId,
                          long creditAccountId, long amount, int ledger, int flags,
                          long debitAccountDebitsPosted, long debitAccountCreditsPosted,
                          long creditAccountDebitsPosted, long creditAccountCreditsPosted) {

    public enum Type {
        ACCOUNT_CREATED,
        TRANSFER_POSTED
    }

    public static LedgerEvent accountCreated(long index, int position, long timestamp, long id, int ledger, int flags) {
        return new LedgerEvent(index, position, Type.ACCOUNT_CREATED, timestamp, id, 0, 0, 0, ledger, flags, 0, 0, 0, 0);
    }

    /**
     * @param debitAccount  {tổng nợ, tổng có} của tài khoản nợ sau giao dịch
     * @param creditAccount {tổng nợ, tổng có} của tài khoản có sau giao dịch
     */
    public static LedgerEvent transferPosted(long index, int position, long timestamp, long id, long debit, long credit,
                                             long amount, int ledger, long debitAccountDebits, long debitAccountCredits,
                                             long creditAccountDebits, long creditAccountCredits) {
        return new LedgerEvent(index, position, Type.TRANSFER_POSTED, timestamp, id, debit, credit, amount, ledger, 0,
                debitAccountDebits, debitAccountCredits, creditAccountDebits, creditAccountCredits);
    }

    /** true nếu sự kiện này đứng sau vị trí (index, position) */
    public boolean isAfter(long index, int position) {
        return this.index > index || (this.index == index && this.position > position);
    }
}

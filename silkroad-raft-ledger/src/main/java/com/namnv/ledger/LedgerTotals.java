package com.namnv.ledger;

/**
 * Tổng của cả sổ. Ghi sổ kép: mỗi giao dịch cộng cùng một số tiền vào tổng nợ và tổng có, nên hai tổng luôn bằng nhau.
 */
public record LedgerTotals(long accounts, long transfers, long debitsPosted, long creditsPosted) {
    public boolean balanced() {
        return debitsPosted == creditsPosted;
    }
}

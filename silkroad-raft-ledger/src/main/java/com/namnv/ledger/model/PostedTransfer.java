package com.namnv.ledger.model;

/**
 * Một giao dịch đã được ghi vào sổ: các trường của {@link LedgerTransfer} cùng thời điểm ghi ({@code timestamp}, epoch ms),
 * là thời điểm leader tạo entry chứa nó trong Raft log, giống nhau trên mọi node và không giảm theo thứ tự ghi.
 */
public record PostedTransfer(long id, long debitAccountId, long creditAccountId, long amount, int ledger, long timestamp) {

    /** phần do client gửi */
    public LedgerTransfer transfer() {
        return new LedgerTransfer(id, debitAccountId, creditAccountId, amount, ledger);
    }
}

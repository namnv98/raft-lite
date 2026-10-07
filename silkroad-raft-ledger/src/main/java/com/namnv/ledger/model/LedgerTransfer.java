package com.namnv.ledger.model;

/**
 * Chuyển {@code amount} từ tài khoản nợ sang tài khoản có: tổng nợ của tài khoản nợ và tổng có của tài khoản có cùng tăng
 * thêm {@code amount} (ghi sổ kép). {@code id} do client chọn và là duy nhất: gửi lại cùng giao dịch được trả về
 * {@link LedgerResult#EXISTS}, không bị ghi hai lần.
 */
public record LedgerTransfer(long id, long debitAccountId, long creditAccountId, long amount, int ledger) {
}

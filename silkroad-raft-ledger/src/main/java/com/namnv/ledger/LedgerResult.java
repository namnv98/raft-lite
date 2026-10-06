package com.namnv.ledger;

/**
 * Kết quả của một lệnh tạo tài khoản hoặc chuyển tiền. Mọi node tính ra cùng một kết quả vì nó chỉ phụ thuộc vào
 * state và lệnh. Lệnh bị từ chối không làm thay đổi gì và không được lưu lại: gửi lại sau có thể thành công.
 */
public enum LedgerResult {
    OK(0),
    /** đã có đúng tài khoản / giao dịch này (lần gửi trước đã thành công): coi như thành công */
    EXISTS(1),
    /** id đã được dùng cho một tài khoản / giao dịch khác */
    EXISTS_WITH_DIFFERENT_FIELDS(2),
    ID_MUST_NOT_BE_ZERO(3),
    LEDGER_MUST_NOT_BE_ZERO(4),
    AMOUNT_MUST_BE_POSITIVE(5),
    ACCOUNTS_MUST_BE_DIFFERENT(6),
    DEBIT_ACCOUNT_NOT_FOUND(7),
    CREDIT_ACCOUNT_NOT_FOUND(8),
    /** hai tài khoản, hoặc giao dịch, không cùng một sổ (đơn vị tiền) */
    LEDGER_MISMATCH(9),
    /** tài khoản nợ không được nợ quá số đã có (cờ DEBITS_MUST_NOT_EXCEED_CREDITS): không đủ tiền */
    EXCEEDS_CREDITS(10),
    /** tài khoản có không được có quá số đã nợ (cờ CREDITS_MUST_NOT_EXCEED_DEBITS) */
    EXCEEDS_DEBITS(11),
    /** tổng nợ hoặc tổng có vượt quá giới hạn của long */
    OVERFLOW(12),
    MALFORMED(13);

    private static final LedgerResult[] BY_CODE = values();

    final byte code;
    // kết quả gửi về client: mảng dùng chung, không ai được sửa
    final byte[] bytes;

    LedgerResult(int code) {
        this.code = (byte) code;
        this.bytes = new byte[]{(byte) code};
    }

    /** thành công, kể cả khi lệnh đã được thực hiện từ lần gửi trước */
    public boolean succeeded() {
        return this == OK || this == EXISTS;
    }

    static LedgerResult of(byte code) {
        if (code < 0 || code >= BY_CODE.length) {
            throw new IllegalArgumentException("unknown ledger result " + code);
        }
        return BY_CODE[code];
    }
}

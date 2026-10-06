package com.namnv.ledger;

/**
 * Một tài khoản. {@code ledger} là sổ (thường là một đơn vị tiền): chỉ chuyển được tiền giữa hai tài khoản cùng sổ.
 *
 * @param flags {@link #DEBITS_MUST_NOT_EXCEED_CREDITS}, {@link #CREDITS_MUST_NOT_EXCEED_DEBITS} hoặc 0
 */
public record LedgerAccount(long id, int ledger, int flags) {
    /** tổng nợ không được vượt tổng có: tài khoản của khách hàng, không được âm */
    public static final int DEBITS_MUST_NOT_EXCEED_CREDITS = 1;
    /** tổng có không được vượt tổng nợ */
    public static final int CREDITS_MUST_NOT_EXCEED_DEBITS = 2;
}

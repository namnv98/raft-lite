package com.namnv.ledger.model;

/** Số liệu của một tài khoản: tổng nợ và tổng có đã ghi. */
public record LedgerBalance(long id, int ledger, int flags, long debitsPosted, long creditsPosted) {
    /** số dư theo phía có (có trừ nợ), như số dư tài khoản của khách hàng */
    public long creditBalance() {
        return creditsPosted - debitsPosted;
    }
}

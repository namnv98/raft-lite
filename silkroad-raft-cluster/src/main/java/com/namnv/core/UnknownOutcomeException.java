package com.namnv.core;

/**
 * Lệnh có thể đã được apply hoặc chưa: node không phải leader, mất quyền giữa chừng, quá hạn hay quá tải.
 * Gửi lại sau với cùng định danh (clientId và sequence, hoặc id nghiệp vụ của lệnh) là an toàn.
 * Không mang stack trace: đây là kết quả bình thường của một hệ phân tán, không phải lỗi lập trình.
 */
public final class UnknownOutcomeException extends RuntimeException {
    public UnknownOutcomeException(String message) {
        super(message, null, false, false);
    }
}

package com.namnv.ledger;

import java.io.IOException;
import java.util.List;

/**
 * Nơi nhận dòng sự kiện của sổ cái: một topic Kafka, một bảng trong cơ sở dữ liệu để truy vấn (CQRS), hay một file.
 * Được gọi từ một thread duy nhất.
 */
public interface EventSink extends AutoCloseable {
    /**
     * Ghi bền vững một lô sự kiện (theo đúng thứ tự). Có thể nhận lại những sự kiện đã ghi sau khi node khởi động lại:
     * sink bỏ qua sự kiện không đứng sau {@link #lastIndex()} / {@link #lastPosition()}.
     */
    void write(List<LedgerEvent> events) throws IOException;

    /** index của sự kiện cuối cùng đã ghi bền vững, hoặc 0 */
    long lastIndex();

    /** position của sự kiện cuối cùng đã ghi bền vững, hoặc -1 */
    int lastPosition();

    @Override
    void close() throws IOException;
}

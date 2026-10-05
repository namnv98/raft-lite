package com.namnv.storage;

import com.namnv.entity.LogEntry;

import java.util.List;
import java.util.function.Supplier;

public interface LogStorage {
    long lastIndex();

    long lastTerm();

    // append chưa fsync: entry chỉ được tính là bền vững sau khi sync()
    void appendEntry(LogEntry entry);

    void appendEntries(List<LogEntry> entries);

    // fsync mọi entry đã append; gọi được ngoài lock của node, nhiều lời gọi đồng thời được gộp lại
    void sync();

    long durableIndex();

    // ép mọi entry đã append xuống đĩa, kể cả khi sync() được cấu hình không chờ đĩa (NodeOptions.logSync = false)
    default void flush() {
        sync();
    }

    default List<LogEntry> readFrom(long startIndexInclusive) {
        return readFrom(startIndexInclusive, Integer.MAX_VALUE);
    }

    // nhiều nhất maxEntries entry liên tiếp kể từ startIndexInclusive. Có thể phải đọc đĩa (xem isCached);
    // gọi được ngoài lock của node, cùng lúc với các thao tác khác trên log
    List<LogEntry> readFrom(long startIndexInclusive, int maxEntries);

    // true nếu readFrom với cùng tham số trả lời được ngay từ bộ nhớ, không phải đọc đĩa
    boolean isCached(long startIndexInclusive, int maxEntries);

    /**
     * Một đoạn entry liền nhau dưới dạng các khung nhị phân (xem EntryFrame) mà log đã dựng sẵn, để gửi cho follower
     * mà không phải mã hoá lại từng entry. Các khung chỉ được nối lại thành một mảng khi copy() được gọi.
     */
    final class Block {
        private final int count;
        private final int bytes;
        private final Supplier<byte[]> copier;

        public Block(int count, int bytes, Supplier<byte[]> copier) {
            this.count = count;
            this.bytes = bytes;
            this.copier = copier;
        }

        public int count() {
            return count;
        }

        public int bytes() {
            return bytes;
        }

        public byte[] copy() {
            return copier.get();
        }
    }

    // nhiều nhất maxEntries entry liên tiếp kể từ startIndexInclusive dưới dạng khung thô; null nếu store không lưu
    // entry ở dạng đó hoặc không có entry nào tại startIndexInclusive
    default Block readBlock(long startIndexInclusive, int maxEntries) {
        return null;
    }

    // index của config entry mới nhất còn trong log và không lớn hơn upTo; 0 nếu không có
    long lastConfigurationIndex(long upTo);

    LogEntry get(long index);

    void truncateSuffix(long firstIndexRemoved);

    // Chỉ cập nhật bộ nhớ nên gọi được trong lock của node; file của các segment không còn cần được xoá ở cleanup()
    void truncatePrefix(long firstIndexKept);

    // Xoá khỏi đĩa những gì truncatePrefix đã bỏ. Xoá file lớn có thể mất hàng chục mili giây, nên gọi ngoài lock của node.
    // Nếu tiến trình tắt trước khi gọi, lần mở log sau sẽ tự xoá
    default void cleanup() {
    }

    // bỏ toàn bộ log, bắt đầu lại ngay sau snapshot
    void reset(long baseIndex, long baseTerm);

    long getBaseIndex();

    long getBaseTerm();

    void close();
}

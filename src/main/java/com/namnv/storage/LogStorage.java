package com.namnv.storage;

import com.namnv.entity.LogEntry;

import java.util.List;

public interface LogStorage {
    long lastIndex();

    long lastTerm();

    // append chưa fsync: entry chỉ được tính là bền vững sau khi sync()
    void appendEntry(LogEntry entry);

    void appendEntries(List<LogEntry> entries);

    // fsync mọi entry đã append; gọi được ngoài lock của node, nhiều lời gọi đồng thời được gộp lại
    void sync();

    long durableIndex();

    default List<LogEntry> readFrom(long startIndexInclusive) {
        return readFrom(startIndexInclusive, Integer.MAX_VALUE);
    }

    // nhiều nhất maxEntries entry liên tiếp kể từ startIndexInclusive
    List<LogEntry> readFrom(long startIndexInclusive, int maxEntries);

    // index của config entry mới nhất còn trong log và không lớn hơn upTo; 0 nếu không có
    long lastConfigurationIndex(long upTo);

    LogEntry get(long index);

    void truncateSuffix(long firstIndexRemoved);

    void truncatePrefix(long firstIndexKept);

    // bỏ toàn bộ log, bắt đầu lại ngay sau snapshot
    void reset(long baseIndex, long baseTerm);

    long getBaseIndex();

    long getBaseTerm();

    void close();
}

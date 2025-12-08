package com.namnv.storage;

import com.namnv.entity.LogEntry;

import java.util.List;

public interface LogStorage {
    long lastIndex();

    long lastTerm();

    void appendEntry(LogEntry entry);

    List<LogEntry> readFrom(long startIndexInclusive);

    LogEntry get(long index);

    void truncateSuffix(long lastIndexKept);

    void truncatePrefix(long firstIndexKept);

    long getBaseIndex();

    long getBaseTerm();

    void setBaseIndex(long baseIndex);

    void setBaseTerm(long baseTerm);
}
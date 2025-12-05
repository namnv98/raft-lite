package com.namnv.storage;

import com.namnv.entity.LogEntry;

import java.util.List;

public interface LogStore {
    long lastIndex();

    long lastTerm();

    void append(LogEntry entry);

    List<LogEntry> readFrom(long startIndexInclusive);

    LogEntry get(long index);

    void truncateSuffix(long fromIndexInclusive);

    void truncatePrefix(long index);

    long getBaseIndex();

    long getBaseTerm();

    void setBaseIndex(long baseIndex);

    void setBaseTerm(long baseTerm);
}
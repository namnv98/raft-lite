package com.namnv.ledger.state;

/**
 * Một đoạn giao dịch trong bộ nhớ, dung lượng cố định và cấp phát một lần: các mảng số nguyên thuỷ cùng một
 * {@link LongIndex} đủ lớn để không bao giờ phải nới. Đầy thì đoạn được khoá lại và ghi xuống RocksDB, rồi được
 * dùng lại cho các giao dịch kế tiếp.
 */
final class TransferSegment {
    final int capacity;
    final long[] ids;
    final long[] debits;
    final long[] credits;
    final long[] amounts;
    final int[] ledgers;
    final long[] timestamps;
    private final LongIndex index;
    int count;

    TransferSegment(int capacity) {
        this.capacity = capacity;
        ids = new long[capacity];
        debits = new long[capacity];
        credits = new long[capacity];
        amounts = new long[capacity];
        ledgers = new int[capacity];
        timestamps = new long[capacity];
        index = new LongIndex(capacity);
        index.reserve(capacity);
    }

    boolean full() {
        return count == capacity;
    }

    void add(long id, long debit, long credit, long amount, int ledger, long timestamp) {
        int slot = count++;
        ids[slot] = id;
        debits[slot] = debit;
        credits[slot] = credit;
        amounts[slot] = amount;
        ledgers[slot] = ledger;
        timestamps[slot] = timestamp;
        index.put(id, slot);
    }

    /** vị trí của giao dịch, hoặc -1 */
    int find(long id) {
        return index.get(id);
    }

    void clear() {
        count = 0;
        index.clear();
    }
}

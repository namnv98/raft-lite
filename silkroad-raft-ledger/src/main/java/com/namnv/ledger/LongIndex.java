package com.namnv.ledger;

import java.util.Arrays;

/**
 * Bảng băm địa chỉ mở từ id (long, khác 0) sang vị trí trong các mảng dữ liệu của sổ cái. Không boxing, không tạo object
 * cho mỗi phần tử: tra cứu chỉ là vài phép tính trên hai mảng số nguyên thuỷ. Không xoá được phần tử (sổ cái không xoá).
 */
final class LongIndex {
    private static final long EMPTY = 0;

    private long[] keys;
    private int[] slots;
    private int size;
    private int mask;

    LongIndex(int expected) {
        int capacity = Integer.highestOneBit(Math.max(16, expected * 2 - 1)) << 1;
        keys = new long[capacity];
        slots = new int[capacity];
        mask = capacity - 1;
    }

    int size() {
        return size;
    }

    /** vị trí của id, hoặc -1 nếu chưa có */
    int get(long id) {
        int i = hash(id) & mask;
        while (true) {
            long key = keys[i];
            if (key == id) {
                return slots[i];
            }
            if (key == EMPTY) {
                return -1;
            }
            i = (i + 1) & mask;
        }
    }

    /** nới bảng trước (nếu cần) để {@code additional} lần put kế tiếp chắc chắn không phải cấp phát */
    void reserve(int additional) {
        while ((size + (long) additional) * 3 > (long) keys.length * 2) {
            grow();
        }
    }

    /** ghi nhận id ở vị trí slot; id chưa được có trong bảng */
    void put(long id, int slot) {
        if (id == EMPTY) {
            throw new IllegalArgumentException("id must not be zero");
        }
        reserve(1);
        insert(id, slot);
        size++;
    }

    private void insert(long id, int slot) {
        int i = hash(id) & mask;
        while (keys[i] != EMPTY) {
            i = (i + 1) & mask;
        }
        keys[i] = id;
        slots[i] = slot;
    }

    private void grow() {
        long[] oldKeys = keys;
        int[] oldSlots = slots;
        keys = new long[oldKeys.length * 2];
        slots = new int[oldKeys.length * 2];
        mask = keys.length - 1;
        for (int i = 0; i < oldKeys.length; i++) {
            if (oldKeys[i] != EMPTY) {
                insert(oldKeys[i], oldSlots[i]);
            }
        }
    }

    void clear() {
        Arrays.fill(keys, EMPTY);
        size = 0;
    }

    // trộn bit để id liên tiếp (rất hay gặp) không dồn vào một cụm
    private static int hash(long id) {
        long h = id * 0x9E3779B97F4A7C15L;
        return (int) (h ^ (h >>> 32));
    }
}

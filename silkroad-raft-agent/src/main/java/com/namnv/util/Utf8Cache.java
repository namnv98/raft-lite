package com.namnv.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Mã hoá và giải mã UTF-8 cho các chuỗi lặp lại liên tục trên đường nóng: clientId (mỗi lệnh ghi), id của leader (mỗi
 * AppendEntries). Mỗi thread một bảng nhỏ, mỗi ô giữ một chuỗi cùng dạng byte của nó: chuỗi đã gặp thì không phải cấp
 * phát lại byte[] hay String. Chuỗi trùng ô thì chỉ thay chỗ nhau, không sai.
 */
public final class Utf8Cache {
    private static final int SLOTS = 4096; // luỹ thừa của 2
    private static final int MAX_CACHED_BYTES = 256;

    private static final class Table {
        final String[] strings = new String[SLOTS];
        final byte[][] bytes = new byte[SLOTS][];
    }

    // một bảng cho mỗi chiều: chiều mã hoá tìm theo String.hashCode, chiều giải mã theo băm của các byte
    private static final ThreadLocal<Table> ENCODED = ThreadLocal.withInitial(Table::new);
    private static final ThreadLocal<Table> DECODED = ThreadLocal.withInitial(Table::new);

    private Utf8Cache() {
    }

    /** dạng UTF-8 của {@code value}; mảng trả về có thể dùng chung nên người gọi không được sửa nó */
    public static byte[] encode(String value) {
        var table = ENCODED.get();
        int slot = value.hashCode() & (SLOTS - 1);
        String cached = table.strings[slot];
        if (cached != null && cached.equals(value)) {
            return table.bytes[slot];
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= MAX_CACHED_BYTES) {
            table.strings[slot] = value;
            table.bytes[slot] = bytes;
        }
        return bytes;
    }

    /** chuỗi UTF-8 nằm ở các byte [offset, offset + length) của {@code buffer}; không đổi position của buffer */
    public static String decode(ByteBuffer buffer, int offset, int length) {
        if (length > MAX_CACHED_BYTES) {
            byte[] bytes = new byte[length];
            buffer.get(offset, bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        int hash = 1;
        for (int i = 0; i < length; i++) {
            hash = 31 * hash + buffer.get(offset + i);
        }
        var table = DECODED.get();
        int slot = (hash ^ (hash >>> 16)) & (SLOTS - 1);
        byte[] cached = table.bytes[slot];
        if (cached != null && cached.length == length && sameBytes(cached, buffer, offset)) {
            return table.strings[slot];
        }
        byte[] bytes = new byte[length];
        buffer.get(offset, bytes);
        String value = new String(bytes, StandardCharsets.UTF_8);
        table.strings[slot] = value;
        table.bytes[slot] = bytes;
        return value;
    }

    private static boolean sameBytes(byte[] cached, ByteBuffer buffer, int offset) {
        if (buffer.hasArray()) {
            int start = buffer.arrayOffset() + offset;
            return Arrays.equals(cached, 0, cached.length, buffer.array(), start, start + cached.length);
        }
        for (int i = 0; i < cached.length; i++) {
            if (cached[i] != buffer.get(offset + i)) {
                return false;
            }
        }
        return true;
    }
}

package com.namnv.kv;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Lệnh của kho KV, đi qua Raft dưới dạng {@code byte[]} như mọi lệnh khác ({@code RaftClient.write} và {@code read}).
 * <pre>
 * put:    [1][độ dài key: 2 byte][key][value]
 * delete: [2][độ dài key: 2 byte][key]
 * get:    [3][độ dài key: 2 byte][key]          (truy vấn đọc, không đi qua log)
 * kết quả get: [1][value] nếu có key, [0] nếu không
 * </pre>
 */
public final class KvCommands {
    static final byte PUT = 1;
    static final byte DELETE = 2;
    static final byte GET = 3;
    private static final int HEADER_BYTES = 3;
    private static final int MAX_KEY_BYTES = 0xFFFF;

    private KvCommands() {
    }

    public static byte[] put(byte[] key, byte[] value) {
        return encode(PUT, key, value);
    }

    public static byte[] delete(byte[] key) {
        return encode(DELETE, key, null);
    }

    public static byte[] get(byte[] key) {
        return encode(GET, key, null);
    }

    /** null nếu kết quả của get cho biết không có key */
    public static byte[] value(byte[] getResult) {
        return getResult.length == 0 || getResult[0] == 0 ? null : Arrays.copyOfRange(getResult, 1, getResult.length);
    }

    static byte[] found(byte[] value) {
        if (value == null) {
            return new byte[]{0};
        }
        var result = new byte[value.length + 1];
        result[0] = 1;
        System.arraycopy(value, 0, result, 1, value.length);
        return result;
    }

    private static byte[] encode(byte op, byte[] key, byte[] value) {
        if (key.length == 0 || key.length > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("key must be 1.." + MAX_KEY_BYTES + " bytes");
        }
        int valueBytes = value == null ? 0 : value.length;
        var buffer = ByteBuffer.allocate(HEADER_BYTES + key.length + valueBytes);
        buffer.put(op).putShort((short) key.length).put(key);
        if (value != null) {
            buffer.put(value);
        }
        return buffer.array();
    }

    // ---------- giải mã, dùng bởi state machine ----------

    static byte op(byte[] command) {
        return command.length >= HEADER_BYTES ? command[0] : 0;
    }

    static byte[] key(byte[] command) {
        int length = keyLength(command);
        return Arrays.copyOfRange(command, HEADER_BYTES, HEADER_BYTES + length);
    }

    static byte[] value(byte[] command, int keyLength) {
        return Arrays.copyOfRange(command, HEADER_BYTES + keyLength, command.length);
    }

    static int keyLength(byte[] command) {
        int length = ((command[1] & 0xFF) << 8) | (command[2] & 0xFF);
        if (length == 0 || HEADER_BYTES + length > command.length) {
            throw new IllegalArgumentException("malformed KV command");
        }
        return length;
    }
}

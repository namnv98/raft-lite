package com.namnv.storage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * CRC32 cho dữ liệu trên đĩa, để một bit hỏng âm thầm bị phát hiện khi đọc lại thay vì được dùng như dữ liệu đúng.
 */
public final class Checksum {
    private static final int HEX_LENGTH = 8;

    private Checksum() {
    }

    public static long crc32(byte[] data, int offset, int length) {
        CRC32 crc = new CRC32();
        crc.update(data, offset, length);
        return crc.getValue();
    }

    private static final byte[] HEX_DIGITS = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);

    // 8 chữ số hex của crc, ghi vào target[offset, offset + 8)
    private static void putHex(long crc, byte[] target, int offset) {
        for (int i = HEX_LENGTH - 1; i >= 0; i--) {
            target[offset + i] = HEX_DIGITS[(int) (crc & 0xf)];
            crc >>>= 4;
        }
    }

    private static boolean matches(byte[] data, int hexStart, int payloadStart, int payloadEnd) {
        try {
            long expected = Long.parseLong(new String(data, hexStart, HEX_LENGTH, StandardCharsets.US_ASCII), 16);
            return expected == crc32(data, payloadStart, payloadEnd - payloadStart);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // cả một file: dòng đầu là crc của phần còn lại
    public static byte[] wrap(byte[] payload) {
        byte[] file = new byte[HEX_LENGTH + 1 + payload.length];
        putHex(crc32(payload, 0, payload.length), file, 0);
        file[HEX_LENGTH] = '\n';
        System.arraycopy(payload, 0, file, HEX_LENGTH + 1, payload.length);
        return file;
    }

    public static byte[] unwrap(byte[] file, String name) throws IOException {
        int payloadStart = HEX_LENGTH + 1;
        if (file.length < payloadStart || file[HEX_LENGTH] != '\n' || !matches(file, 0, payloadStart, file.length)) {
            throw new IOException(name + " is corrupted: checksum mismatch");
        }
        return Arrays.copyOfRange(file, payloadStart, file.length);
    }
}

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

    private static byte[] hex(long crc) {
        return String.format("%08x", crc).getBytes(StandardCharsets.US_ASCII);
    }

    private static boolean matches(byte[] data, int hexStart, int payloadStart, int payloadEnd) {
        try {
            long expected = Long.parseLong(new String(data, hexStart, HEX_LENGTH, StandardCharsets.US_ASCII), 16);
            return expected == crc32(data, payloadStart, payloadEnd - payloadStart);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // một dòng của log: "<crc> <payload>\n"; payload không được chứa ký tự xuống dòng
    public static byte[] encodeLine(byte[] payload) {
        byte[] line = new byte[HEX_LENGTH + 1 + payload.length + 1];
        System.arraycopy(hex(crc32(payload, 0, payload.length)), 0, line, 0, HEX_LENGTH);
        line[HEX_LENGTH] = ' ';
        System.arraycopy(payload, 0, line, HEX_LENGTH + 1, payload.length);
        line[line.length - 1] = '\n';
        return line;
    }

    // payload của dòng data[start, end) (không gồm ký tự xuống dòng), null nếu dòng không còn nguyên vẹn
    public static byte[] decodeLine(byte[] data, int start, int end) {
        int payloadStart = start + HEX_LENGTH + 1;
        if (payloadStart > end || data[start + HEX_LENGTH] != ' ' || !matches(data, start, payloadStart, end)) {
            return null;
        }
        return Arrays.copyOfRange(data, payloadStart, end);
    }

    // cả một file: dòng đầu là crc của phần còn lại
    public static byte[] wrap(byte[] payload) {
        byte[] file = new byte[HEX_LENGTH + 1 + payload.length];
        System.arraycopy(hex(crc32(payload, 0, payload.length)), 0, file, 0, HEX_LENGTH);
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

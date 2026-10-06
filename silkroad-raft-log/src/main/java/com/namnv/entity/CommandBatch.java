package com.namnv.entity;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;

/**
 * Nhiều lệnh của một client đi chung một entry của log: {@code [số lệnh][độ dài][byte của lệnh]...} (int big-endian).
 * Cả lô được commit, apply và chống ghi trùng cùng nhau như một lệnh, nên chi phí của Raft cho mỗi entry (khung trong
 * log, một lần đi qua mạng, một future, một lần ghi nhận phiên client) được chia cho mọi lệnh trong lô.
 */
public final class CommandBatch {
    private CommandBatch() {
    }

    public static byte[] encode(List<byte[]> commands) {
        long size = Integer.BYTES;
        for (byte[] command : commands) {
            size += Integer.BYTES + command.length;
        }
        if (size > Integer.MAX_VALUE - 64) {
            throw new IllegalArgumentException("command batch of " + size + " bytes is too large");
        }
        var buffer = ByteBuffer.allocate((int) size).putInt(commands.size());
        for (byte[] command : commands) {
            buffer.putInt(command.length).put(command);
        }
        return buffer.array();
    }

    /**
     * Gọi {@code each} cho từng lệnh của lô theo đúng thứ tự.
     *
     * @throws IllegalArgumentException nếu lô sai định dạng; lô đã được kiểm tra khi vào log nên điều này không xảy ra
     */
    public static void forEach(byte[] batch, Consumer<byte[]> each) {
        var buffer = ByteBuffer.wrap(batch);
        int count = buffer.getInt();
        for (int i = 0; i < count; i++) {
            int length = buffer.getInt();
            if (length < 0 || length > buffer.remaining()) {
                throw new IllegalArgumentException("malformed command batch");
            }
            byte[] command = new byte[length];
            buffer.get(command);
            each.accept(command);
        }
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("malformed command batch");
        }
    }

    /** true nếu {@code batch} đúng định dạng; leader kiểm tra trước khi đưa lô vào log */
    public static boolean isValid(byte[] batch) {
        if (batch == null || batch.length < Integer.BYTES) {
            return false;
        }
        var buffer = ByteBuffer.wrap(batch);
        int count = buffer.getInt();
        if (count < 0 || count > buffer.remaining() / Integer.BYTES) {
            return false;
        }
        for (int i = 0; i < count; i++) {
            if (buffer.remaining() < Integer.BYTES) {
                return false;
            }
            int length = buffer.getInt();
            if (length < 0 || length > buffer.remaining()) {
                return false;
            }
            buffer.position(buffer.position() + length);
        }
        return !buffer.hasRemaining();
    }
}

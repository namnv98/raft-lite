package com.namnv.storage.binary;

import com.namnv.entity.ConfigurationEntry;
import com.namnv.entity.LogEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Một entry của log nằm trong file dưới dạng một khung nhị phân, little-endian, bắt đầu ở vị trí chia hết cho 32:
 * <pre>
 *  0  int  độ dài khung (header + thân, chưa tính phần đệm căn lề); 0 nghĩa là chưa có khung nào ở đây
 *  4  int  CRC32 của các byte [8, độ dài)
 *  8  long index
 * 16  long term
 * 24  long sequence
 * 32  byte cờ, rồi clientId, cấu hình (nếu có) và cuối cùng là lệnh: nguyên các byte client gửi, không mã hoá lại
 * </pre>
 * Độ dài được ghi sau cùng: khung chưa ghi xong thì độ dài vẫn là 0 và coi như chưa tồn tại.
 * <p>
 * Đây cũng là dạng của entry trên dây trong AppendEntries: một dãy khung nối nhau, mỗi khung căn lề 32 byte. Nhờ vậy
 * leader gửi cho follower đúng các khung nó đã dựng để ghi file, không phải mã hoá lại từng entry cho từng follower.
 */
public final class EntryFrame {
    static final int HEADER_LENGTH = 32;
    static final int ALIGNMENT = 32;

    private static final int LENGTH_OFFSET = 0;
    private static final int CRC_OFFSET = 4;
    private static final int INDEX_OFFSET = 8;
    private static final int TERM_OFFSET = 16;
    private static final int SEQUENCE_OFFSET = 24;

    private static final int CONFIGURATION_ENTRY = 1;
    private static final int SESSION_CLOSE = 2;
    private static final int HAS_COMMAND = 4;
    private static final int HAS_CLIENT_ID = 8;
    private static final int HAS_CONFIGURATION = 16;

    private EntryFrame() {
    }

    static int align(int length) {
        return (length + ALIGNMENT - 1) & -ALIGNMENT;
    }

    // phần thân đứng trước lệnh: cờ, clientId và cấu hình
    static byte[] meta(LogEntry entry) {
        byte[] clientId = entry.getClientId() == null ? null : entry.getClientId().getBytes(StandardCharsets.UTF_8);
        byte[] configuration = entry.getConfiguration() == null ? null : encode(entry.getConfiguration());
        int flags = (entry.isConfigurationEntry() ? CONFIGURATION_ENTRY : 0)
                | (entry.isSessionClose() ? SESSION_CLOSE : 0)
                | (entry.getCommand() != null ? HAS_COMMAND : 0)
                | (clientId != null ? HAS_CLIENT_ID : 0)
                | (configuration != null ? HAS_CONFIGURATION : 0);
        var meta = ByteBuffer.allocate(1 + (clientId == null ? 0 : Integer.BYTES + clientId.length)
                + (configuration == null ? 0 : configuration.length)).order(ByteOrder.LITTLE_ENDIAN);
        meta.put((byte) flags);
        if (clientId != null) {
            meta.putInt(clientId.length).put(clientId);
        }
        if (configuration != null) {
            meta.put(configuration);
        }
        return meta.array();
    }

    static int length(LogEntry entry, byte[] meta) {
        return HEADER_LENGTH + meta.length + (entry.getCommand() == null ? 0 : entry.getCommand().length);
    }

    static void write(ByteBuffer buffer, int offset, LogEntry entry, byte[] meta) {
        int length = length(entry, meta);
        buffer.putLong(offset + INDEX_OFFSET, entry.getIndex());
        buffer.putLong(offset + TERM_OFFSET, entry.getTerm());
        buffer.putLong(offset + SEQUENCE_OFFSET, entry.getSequence());
        buffer.put(offset + HEADER_LENGTH, meta);
        if (entry.getCommand() != null) {
            buffer.put(offset + HEADER_LENGTH + meta.length, entry.getCommand());
        }
        buffer.putInt(offset + CRC_OFFSET, crc(buffer, offset, length));
        buffer.putInt(offset + LENGTH_OFFSET, length);
    }

    // khung của entry trong một mảng riêng, dài đúng bằng phần nó chiếm trong file (đã căn lề, phần đệm là byte 0)
    static byte[] encode(LogEntry entry) {
        byte[] meta = meta(entry);
        byte[] frame = new byte[align(length(entry, meta))];
        write(ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN), 0, entry, meta);
        return frame;
    }

    static LogEntry decode(byte[] frame) {
        return read(ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN), 0, frame.length);
    }

    static int frameLength(ByteBuffer buffer, int offset) {
        return buffer.getInt(offset + LENGTH_OFFSET);
    }

    private static int crc(ByteBuffer buffer, int offset, int length) {
        var crc = new CRC32();
        crc.update(buffer.slice(offset + INDEX_OFFSET, length - INDEX_OFFSET));
        return (int) crc.getValue();
    }

    // entry của khung tại offset; null nếu ở đó không có khung nguyên vẹn (chưa ghi, ghi dở hoặc hỏng)
    static LogEntry read(ByteBuffer buffer, int offset, int limit) {
        if (offset < 0 || offset > limit - HEADER_LENGTH - 1) {
            return null;
        }
        int length = frameLength(buffer, offset);
        if (length <= HEADER_LENGTH || length > limit - offset || crc(buffer, offset, length) != buffer.getInt(offset + CRC_OFFSET)) {
            return null;
        }
        try {
            var body = buffer.slice(offset + HEADER_LENGTH, length - HEADER_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
            int flags = body.get();
            var entry = new LogEntry();
            entry.setIndex(buffer.getLong(offset + INDEX_OFFSET));
            entry.setTerm(buffer.getLong(offset + TERM_OFFSET));
            entry.setSequence(buffer.getLong(offset + SEQUENCE_OFFSET));
            entry.setConfigurationEntry((flags & CONFIGURATION_ENTRY) != 0);
            entry.setSessionClose((flags & SESSION_CLOSE) != 0);
            if ((flags & HAS_CLIENT_ID) != 0) {
                entry.setClientId(readString(body));
            }
            if ((flags & HAS_CONFIGURATION) != 0) {
                entry.setConfiguration(decodeConfiguration(body));
            }
            if ((flags & HAS_COMMAND) != 0) {
                byte[] command = new byte[body.remaining()];
                body.get(command);
                entry.setCommand(command);
            }
            return body.hasRemaining() ? null : entry;
        } catch (RuntimeException e) {
            return null; // CRC khớp nhưng nội dung không đọc được
        }
    }

    // các entry dưới dạng một dãy khung nối nhau, như chúng nằm trong file
    public static ByteBuffer encodeAll(List<LogEntry> entries) {
        byte[][] metas = new byte[entries.size()][];
        int total = 0;
        for (int i = 0; i < metas.length; i++) {
            metas[i] = meta(entries.get(i));
            total += align(length(entries.get(i), metas[i]));
        }
        var block = ByteBuffer.allocate(total).order(ByteOrder.LITTLE_ENDIAN);
        int offset = 0;
        for (int i = 0; i < metas.length; i++) {
            write(block, offset, entries.get(i), metas[i]);
            offset += align(length(entries.get(i), metas[i]));
        }
        return block;
    }

    /**
     * Giải mã đúng {@code count} khung nối nhau chiếm trọn {@code block}; mỗi khung được kiểm tra CRC.
     *
     * @throws IOException nếu có khung không nguyên vẹn, hoặc số khung không khớp với kích thước của block
     */
    public static List<LogEntry> readAll(ByteBuffer block, int count) throws IOException {
        var frames = block.slice().order(ByteOrder.LITTLE_ENDIAN);
        var entries = new ArrayList<LogEntry>(count);
        int offset = 0;
        for (int i = 0; i < count; i++) {
            LogEntry entry = read(frames, offset, frames.capacity());
            if (entry == null) {
                throw new IOException("Damaged log entry frame " + i + " of " + count);
            }
            entries.add(entry);
            offset += align(frameLength(frames, offset));
        }
        if (offset != frames.capacity()) {
            throw new IOException((frames.capacity() - offset) + " unexpected bytes after " + count + " log entry frames");
        }
        return entries;
    }

    private static byte[] encode(ConfigurationEntry configuration) {
        var strings = new ArrayList<byte[]>();
        int size = 1 + 2 * Integer.BYTES;
        for (List<String> nodes : List.of(configuration.getOldNodes(), configuration.getNewNodes())) {
            for (String node : nodes) {
                byte[] bytes = node.getBytes(StandardCharsets.UTF_8);
                strings.add(bytes);
                size += Integer.BYTES + bytes.length;
            }
        }
        var out = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        out.put((byte) (configuration.isJoint() ? 1 : 0));
        int next = 0;
        for (List<String> nodes : List.of(configuration.getOldNodes(), configuration.getNewNodes())) {
            out.putInt(nodes.size());
            for (int i = 0; i < nodes.size(); i++) {
                byte[] bytes = strings.get(next++);
                out.putInt(bytes.length).put(bytes);
            }
        }
        return out.array();
    }

    private static ConfigurationEntry decodeConfiguration(ByteBuffer in) {
        boolean joint = in.get() != 0;
        return new ConfigurationEntry(readStrings(in), readStrings(in), joint);
    }

    private static List<String> readStrings(ByteBuffer in) {
        int count = in.getInt();
        if (count < 0 || count > in.remaining()) {
            throw new IllegalArgumentException("Invalid element count " + count);
        }
        var values = new ArrayList<String>(count);
        for (int i = 0; i < count; i++) {
            values.add(readString(in));
        }
        return values;
    }

    private static String readString(ByteBuffer in) {
        int length = in.getInt();
        if (length < 0 || length > in.remaining()) {
            throw new IllegalArgumentException("Invalid field length " + length);
        }
        byte[] bytes = new byte[length];
        in.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}

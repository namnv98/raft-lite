package com.namnv.storage.binary;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * Một file segment có kích thước cố định. 32 byte đầu là header của segment:
 * <pre>
 *  0  int  magic
 *  4  int  version
 *  8  long index của entry đầu tiên
 * 16  long vị trí mà mọi byte đứng trước nó đã chắc chắn nằm trên đĩa
 * </pre>
 * Các khung entry nối nhau ngay sau header; phần chưa ghi tới luôn là byte 0.
 * <p>
 * Mọi thao tác trên file (và các trường writtenOffset, forcedOffset, durableOffset) chỉ do thread đang giữ quyền ghi
 * của store thực hiện; xem {@link BinaryLogStorage}.
 */
final class Segment {
    static final int HEADER_LENGTH = 32;
    private static final int MAGIC = 0x5446_4152; // "RAFT"
    private static final int VERSION = 1;
    private static final int FIRST_INDEX_OFFSET = 8;
    private static final int DURABLE_OFFSET = 16;
    private static final int PREALLOCATE_STEP_BYTES = 4 << 20;

    final Path file;
    final long firstIndex;
    final int capacity;
    // vị trí kết thúc của khung cuối đã append vào segment này (trong bộ nhớ, có thể chưa xuống file)
    int endOffset = HEADER_LENGTH;
    // segment đã rời khỏi log: không ghi thêm gì vào file của nó nữa
    volatile boolean dropped;

    // file đã tồn tại trên đĩa chưa: nó chỉ được tạo khi khung đầu tiên của segment được ghi xuống
    private boolean created;
    private FileChannel channel;
    // mọi byte trước vị trí này đã được ghi vào file / đã được fsync / được header ghi nhận là đã lên đĩa
    int writtenOffset = HEADER_LENGTH;
    int forcedOffset = HEADER_LENGTH;
    int durableOffset = HEADER_LENGTH;

    Segment(Path file, long firstIndex, int capacity) {
        this.file = file;
        this.firstIndex = firstIndex;
        this.capacity = capacity;
    }

    // segment đọc lại từ đĩa; header là 32 byte đầu của file. null nếu đó không phải header của một segment
    static Segment existing(Path file, long firstIndex, ByteBuffer header, long size) {
        if (size < HEADER_LENGTH + EntryFrame.HEADER_LENGTH || size > Integer.MAX_VALUE) {
            return null;
        }
        long durable = header.getLong(DURABLE_OFFSET);
        if (header.getInt(0) != MAGIC || header.getInt(4) != VERSION || header.getLong(FIRST_INDEX_OFFSET) != firstIndex
                || durable < HEADER_LENGTH || durable > size) {
            return null;
        }
        var segment = new Segment(file, firstIndex, (int) size);
        segment.created = true;
        segment.durableOffset = (int) durable;
        return segment;
    }

    boolean created() {
        return created;
    }

    boolean open() throws IOException {
        return open(null);
    }

    /**
     * @param preallocated file đã được cấp phát sẵn trên đĩa (xem {@link #preallocate}) để dùng làm segment này,
     *                     hoặc null để tạo một file thưa
     * @return true nếu lời gọi này vừa tạo file của segment
     */
    boolean open(Path preallocated) throws IOException {
        if (channel != null) {
            return false;
        }
        if (created) {
            channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
            return false;
        }
        Files.deleteIfExists(file);
        FileChannel opened;
        if (preallocated != null) {
            Files.move(preallocated, file, StandardCopyOption.ATOMIC_MOVE);
            opened = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } else {
            var raf = new RandomAccessFile(file.toFile(), "rw");
            try {
                // file thưa dài đúng kích thước segment: phần chưa ghi đọc ra là byte 0 mà không tốn chỗ trên đĩa
                raf.setLength(capacity);
            } catch (IOException | RuntimeException e) {
                raf.close();
                throw e;
            }
            opened = raf.getChannel();
        }
        channel = opened;
        try {
            var header = ByteBuffer.allocate(HEADER_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
            header.putInt(0, MAGIC).putInt(4, VERSION).putLong(FIRST_INDEX_OFFSET, firstIndex).putLong(DURABLE_OFFSET, HEADER_LENGTH);
            write(header, 0);
        } catch (IOException | RuntimeException e) {
            channel = null;
            opened.close();
            throw e;
        }
        created = true;
        return true;
    }

    /**
     * Tạo một file dài {@code capacity} byte mà mọi block của nó đã được cấp phát và ghi byte 0 xuống đĩa. fsync sau khi ghi
     * vào file như vậy chỉ phải đẩy dữ liệu; với file thưa nó còn phải chờ filesystem ghi nhận các block mới cấp phát,
     * chậm hơn vài lần.
     */
    static void preallocate(Path file, int capacity) throws IOException {
        try (FileChannel target = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            var zeros = ByteBuffer.allocateDirect(Math.min(1 << 20, capacity));
            long position = 0;
            long forced = 0;
            while (position < capacity) {
                zeros.clear().limit((int) Math.min(zeros.capacity(), capacity - position));
                position += target.write(zeros, position);
                // fsync từng phần nhỏ: đẩy cả file xuống một lượt làm các lần fsync của log đang chạy phải chờ rất lâu
                if (position - forced >= PREALLOCATE_STEP_BYTES) {
                    target.force(false);
                    forced = position;
                }
            }
            target.force(true);
        }
    }

    void write(ByteBuffer source, int offset) throws IOException {
        open();
        int position = offset;
        while (source.hasRemaining()) {
            position += channel.write(source, position);
        }
    }

    void force() throws IOException {
        if (created) {
            open();
            channel.force(false);
        }
    }

    // ghi nhận trong header rằng mọi byte trước offset đã lên đĩa; bản thân header lên đĩa cùng lần fsync kế tiếp
    void markDurable(int offset) throws IOException {
        var marker = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        marker.putLong(0, offset);
        write(marker, DURABLE_OFFSET);
        durableOffset = offset;
    }

    void zero(int from, int to) throws IOException {
        var zeros = ByteBuffer.allocate(Math.min(1 << 16, Math.max(1, to - from)));
        int position = from;
        while (position < to) {
            zeros.clear().limit(Math.min(zeros.capacity(), to - position));
            int length = zeros.remaining();
            write(zeros, position);
            position += length;
        }
    }

    void close() {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                // không còn gì để làm với một file đang đóng
            }
            channel = null;
        }
    }

    // đóng và xoá file nếu nó đã được tạo
    boolean delete() throws IOException {
        close();
        if (!created) {
            return false;
        }
        created = false;
        return Files.deleteIfExists(file);
    }
}

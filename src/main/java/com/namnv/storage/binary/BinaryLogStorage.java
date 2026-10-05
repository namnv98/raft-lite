package com.namnv.storage.binary;

import com.namnv.config.NodeOptions;
import com.namnv.entity.LogEntry;
import com.namnv.storage.DiskFaultInjector;
import com.namnv.storage.FileUtil;
import com.namnv.storage.LogStorage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Log của node, lưu theo kiểu Aeron Archive: các file segment log_&lt;firstIndex&gt;.rec có kích thước cố định, mỗi entry là một
 * khung nhị phân căn lề 32 byte (xem {@link EntryFrame}), lệnh của client được chép nguyên byte.
 * <p>
 * Như Aeron, việc ghi chia làm hai tầng:
 * <ol>
 * <li>{@code appendEntries} chỉ dựng khung trong bộ nhớ và xếp nó vào hàng chờ ghi. Nó chạy trong lock của node và không
 * bao giờ chạm tới đĩa, nên không thể bị hệ điều hành giữ lại.</li>
 * <li>{@link #sync()} (chạy ngoài lock của node) ghi cả khối khung đang chờ xuống file bằng một lời gọi, rồi fsync.
 * Entry chỉ được coi là bền vững sau bước này.</li>
 * </ol>
 * Mỗi lúc chỉ một thread giữ quyền ghi: nó là thread duy nhất đụng tới các file. Các thao tác hiếm phải sửa file ngay
 * (cắt đuôi log, reset) chờ thread đó xong rồi mới làm.
 * <p>
 * Khi mở lại sau crash, mỗi segment được đọc tới khung nguyên vẹn cuối cùng. Phần đứng sau mốc "đã lên đĩa" ghi trong
 * header của segment là đuôi chưa từng được ack nên được bỏ; khung hỏng đứng trước mốc đó là dữ liệu đã ghi xong bị hỏng
 * và làm việc mở log thất bại.
 */
public class BinaryLogStorage implements LogStorage {
    private static final String PREFIX = "log_";
    public static final String SUFFIX = ".rec";

    // Một AppendEntries không mang quá chừng này byte entry (trừ khi chỉ một entry đã lớn hơn), để khung RPC không vượt
    // giới hạn của transport. Không đặt nhỏ hơn: follower fsync một lần cho mỗi request, nên request càng lớn thì càng
    // nhiều entry đi chung một lần fsync.
    private static final int MAX_BLOCK_BYTES = 16 << 20;
    // bộ đệm gom các khung nhỏ trước mỗi lời gọi ghi file
    private static final int IO_BUFFER_BYTES = 1 << 20;
    // Khung chỉ được giữ lại để gửi cho follower, mà follower thường chỉ chậm hơn leader vài chục mili giây. Giữ khung của
    // mọi entry trong cache làm chúng sống qua nhiều lượt GC, và GC phải chép đi chép lại chúng.
    private static final int MAX_RETAINED_FRAME_BYTES = 8 << 20;

    private static final class LongList {
        private long[] values = new long[1024];
        private int size;

        void add(long value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, size * 2);
            }
            values[size++] = value;
        }

        long get(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException(index);
            }
            return values[index];
        }

        void truncate(int newSize) {
            size = newSize;
        }

        void dropFirst(int count) {
            System.arraycopy(values, count, values, 0, size - count);
            size -= count;
        }

        int size() {
            return size;
        }
    }

    // khung của một entry đã append nhưng chưa được ghi xuống file, cùng chỗ của nó trong file
    private record Pending(long index, Segment segment, int offset, byte[] frame) {
    }

    // count entry liền nhau, bắt đầu từ index first, nằm ở các byte [start, end) của một file segment
    private record Range(Path file, int start, int end, long first, int count) {
    }

    private long baseIndex;
    private long baseTerm;

    private final Path folder;
    private final DiskFaultInjector faults;
    private final int segmentBytes;
    // NodeOptions.logSync: false thì sync() chỉ ghi xuống file mà không fsync; việc fsync do flush() làm
    private final boolean forceToDisk;
    // Các entry mới nhất (vòng đệm theo index): object để đường chạy thường xuyên không phải giải mã lại, và khung của
    // chúng để gửi cho follower mà không phải mã hoá lại. frames[i] có thể null khi cache[i] có (entry đọc lại lúc mở log).
    private final LogEntry[] cache;
    private final byte[][] frames;
    // tổng kích thước các khung đang giữ, và index nhỏ nhất có thể còn khung
    private long retainedFrameBytes;
    private long oldestFrameIndex;
    // offsets.get(i): vị trí bắt đầu khung của entry baseIndex + 1 + i trong segment chứa nó
    private final LongList offsets = new LongList();
    // index của các config entry còn trong log, tăng dần
    private final LongList configurationIndexes = new LongList();
    private final List<Segment> segments = new ArrayList<>();
    // vị trí của khung kế tiếp trong segment cuối
    private int writeOffset;
    // các khung chưa xuống file, theo thứ tự index liền nhau; phần tử cuối luôn là entry cuối của log
    private final ArrayList<Pending> tail = new ArrayList<>();
    // các segment mà truncatePrefix đã bỏ, chờ cleanup() xoá file
    private final List<Segment> garbage = new ArrayList<>();
    private boolean dirDirty;

    // index lớn nhất đã được ghi vào file / đã được fsync / được coi là bền vững (theo forceToDisk)
    private long writtenIndex;
    private long forcedIndex;
    private long durableIndex;
    // tăng mỗi lần cắt bỏ entry, để readFrom() đang chạy dở biết kết quả của nó không còn đúng
    private long generation;
    // một thread đang ghi file ngoài monitor; mọi thao tác khác trên file phải chờ nó xong
    private boolean writerActive;
    // chỉ thread đang giữ quyền ghi dùng
    private final ByteBuffer ioBuffer;

    // File cấp phát sẵn cho segment kế tiếp, do một thread nền chuẩn bị. Chỉ dùng khi log được fsync: lúc đó mỗi lần
    // fsync trên file cấp phát sẵn nhanh hơn vài lần so với trên file thưa. Không fsync thì không có gì để nhanh hơn,
    // mà việc ghi sẵn cả file lại tốn gấp đôi lượng ghi đĩa.
    // Và chỉ khi log lớn chậm: file cấp phát sẵn phải được ghi hai lần (byte 0 rồi dữ liệu). Segment đầy trong chưa tới
    // MIN_SEGMENT_NANOS thì lượng ghi đã đủ lớn để lần ghi thêm đó làm chậm hơn là giúp.
    private static final long MIN_SEGMENT_NANOS = 1_500_000_000L;
    private long lastRollNanos = System.nanoTime();
    private boolean preallocateNext = true;
    private final boolean preallocate;
    private final Path spareFile;
    private boolean spareReady;
    private boolean sparePreparing;
    private boolean closed;

    public BinaryLogStorage(NodeOptions nodeOptions, long baseIndex, long baseTerm) throws IOException {
        this.folder = Path.of(nodeOptions.getLogUri());
        this.faults = nodeOptions.getDiskFaults();
        // đủ chỗ cho header của segment và ít nhất một khung nhỏ, và là bội số của độ căn lề
        this.segmentBytes = EntryFrame.align(Math.max(nodeOptions.getLogSegmentBytes(), 1024));
        this.cache = new LogEntry[Math.max(1, nodeOptions.getLogCacheEntries())];
        this.frames = new byte[cache.length][];
        this.forceToDisk = nodeOptions.isLogSync();
        this.preallocate = forceToDisk && nodeOptions.isLogPreallocate();
        this.ioBuffer = ByteBuffer.allocateDirect(Math.min(IO_BUFFER_BYTES, segmentBytes));
        Files.createDirectories(folder);
        this.spareFile = folder.resolve("segment.spare");
        Files.deleteIfExists(spareFile);
        this.baseIndex = baseIndex;
        this.baseTerm = baseTerm;
        load();
        prepareSpare();
    }

    private static long firstIndexOf(Path file) {
        String name = file.getFileName().toString();
        if (!name.startsWith(PREFIX) || !name.endsWith(SUFFIX)) {
            return -1;
        }
        try {
            return Long.parseLong(name.substring(PREFIX.length(), name.length() - SUFFIX.length()));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private synchronized void load() throws IOException {
        List<Path> files = new ArrayList<>();
        try (var children = Files.list(folder)) {
            children.filter(p -> firstIndexOf(p) > 0).forEach(files::add);
        }
        files.sort(Comparator.comparingLong(BinaryLogStorage::firstIndexOf));

        long nextIndex = -1;
        // đã gặp chỗ log kết thúc: mọi segment phía sau chỉ chứa entry chưa từng được ack
        boolean ended = false;
        int end = Segment.HEADER_LENGTH;
        for (int f = 0; f < files.size(); f++) {
            Path file = files.get(f);
            long firstIndex = firstIndexOf(file);
            SegmentReader data = null;
            Segment segment = null;
            if (!ended) {
                data = new SegmentReader(file);
                segment = data.size() < Segment.HEADER_LENGTH ? null
                        : Segment.existing(file, firstIndex, data.window(0, Segment.HEADER_LENGTH), data.size());
            }
            if (segment == null && data != null) {
                data.close();
            }
            if (segment == null) {
                // segment tạo dở khi crash chỉ có thể là file cuối cùng
                if (!ended && f != files.size() - 1) {
                    throw new IOException("Log segment " + file + " has no valid header");
                }
                Files.delete(file);
                dirDirty = true;
                continue;
            }
            if (nextIndex >= 0 && firstIndex != nextIndex) {
                // Segment trước kết thúc sớm hơn chỗ segment này bắt đầu. Chỗ kết thúc đó nằm sau mốc đã lên đĩa (đã kiểm
                // tra bên dưới), nên các entry bị thiếu và mọi thứ sau chúng chưa từng được ack.
                if (firstIndex < nextIndex) {
                    throw new IOException("Log segment " + file + " does not follow index " + (nextIndex - 1));
                }
                ended = true;
                Files.delete(file);
                dirDirty = true;
                continue;
            }
            nextIndex = firstIndex;
            segments.add(segment);

            int offset = Segment.HEADER_LENGTH;
            while (true) {
                LogEntry entry = data.entryAt(offset);
                if (entry == null) {
                    break;
                }
                if (entry.getIndex() != nextIndex) {
                    throw new IOException("Log segment " + file + " has index " + entry.getIndex()
                            + " where " + nextIndex + " was expected");
                }
                // entry <= baseIndex đã nằm trong snapshot, chỉ bỏ qua
                if (entry.getIndex() > baseIndex) {
                    if (entry.getIndex() != lastIndex() + 1) {
                        throw new IOException("Log has a gap before index " + entry.getIndex());
                    }
                    remember(entry, offset, null);
                }
                nextIndex++;
                offset += EntryFrame.align(data.frameLengthAt(offset));
            }
            end = offset;
            if (end < segment.durableOffset) {
                data.close();
                throw new IOException("Corrupted log segment " + file + " at offset " + end);
            }
            boolean torn = end < segment.capacity && data.frameLengthAt(end) != 0;
            // phía sau khung cuối phải toàn byte 0, nếu không một khung ghi sau này có thể bị đọc nối với rác cũ
            boolean clean = data.isZeroFrom(end);
            data.close();
            if (!clean) {
                segment.zero(end, segment.capacity);
                segment.force();
            }
            segment.close();
            segment.endOffset = end;
            segment.writtenOffset = end;
            segment.forcedOffset = end;
            ended = torn;
        }
        dropCoveredSegments(false);
        if (!segments.isEmpty()) {
            writeOffset = end;
        }
        if (dirDirty) {
            FileUtil.syncDirectory(folder);
            dirDirty = false;
        }
        writtenIndex = lastIndex();
        forcedIndex = writtenIndex;
        durableIndex = writtenIndex;
    }

    // Đọc một file segment theo từng cửa sổ: file dài đúng kích thước segment dù chỉ mới ghi một phần, nên không đọc cả
    // file vào bộ nhớ.
    private static final class SegmentReader {
        private static final int WINDOW_BYTES = 1 << 20;
        private final FileChannel channel;
        private final int size;
        private ByteBuffer window = ByteBuffer.allocate(0);
        private int windowStart;

        SegmentReader(Path file) throws IOException {
            this.channel = FileChannel.open(file, StandardOpenOption.READ);
            this.size = (int) Math.min(channel.size(), Integer.MAX_VALUE);
        }

        int size() {
            return size;
        }

        // các byte [offset, offset + length) của file (ngắn hơn nếu chạm cuối file), vị trí 0 của buffer ứng với offset
        ByteBuffer window(int offset, int length) throws IOException {
            int wanted = Math.min(length, size - offset);
            if (offset < windowStart || offset + wanted > windowStart + window.limit()) {
                var fresh = ByteBuffer.allocate(Math.min(Math.max(wanted, WINDOW_BYTES), size - offset));
                while (fresh.hasRemaining() && channel.read(fresh, offset + fresh.position()) >= 0) {
                    // đọc cho đầy cửa sổ
                }
                fresh.flip();
                window = fresh;
                windowStart = offset;
            }
            return window.slice(offset - windowStart, Math.min(wanted, window.limit() - (offset - windowStart)))
                    .order(ByteOrder.LITTLE_ENDIAN);
        }

        int frameLengthAt(int offset) throws IOException {
            return offset + Integer.BYTES > size ? 0 : window(offset, Integer.BYTES).getInt(0);
        }

        // entry của khung tại offset, null nếu ở đó không có khung nguyên vẹn
        LogEntry entryAt(int offset) throws IOException {
            if (offset + EntryFrame.HEADER_LENGTH >= size) {
                return null;
            }
            int length = frameLengthAt(offset);
            if (length <= EntryFrame.HEADER_LENGTH || length > size - offset) {
                return null;
            }
            var frame = window(offset, length);
            return EntryFrame.read(frame, 0, frame.capacity());
        }

        boolean isZeroFrom(int from) throws IOException {
            int position = from;
            while (position < size) {
                var chunk = window(position, WINDOW_BYTES);
                for (int i = 0; i < chunk.capacity(); i++) {
                    if (chunk.get(i) != 0) {
                        return false;
                    }
                }
                position += chunk.capacity();
            }
            return true;
        }

        void close() throws IOException {
            channel.close();
        }
    }

    // Xoá các segment mà mọi entry đều đã nằm trong snapshot. deferDeletion: chỉ bỏ khỏi log, file được xoá ở cleanup()
    private void dropCoveredSegments(boolean deferDeletion) throws IOException {
        while (!segments.isEmpty()
                && (segments.size() > 1 ? segments.get(1).firstIndex <= baseIndex + 1 : offsets.size() == 0)) {
            Segment removed = segments.remove(0);
            removed.dropped = true;
            if (deferDeletion) {
                garbage.add(removed);
            } else if (removed.delete()) {
                dirDirty = true;
            }
        }
    }

    // ghi nhận một entry vừa nằm vào cuối log
    private void remember(LogEntry entry, int offset, byte[] frame) {
        offsets.add(offset);
        int slot = (int) (entry.getIndex() % cache.length);
        if (frames[slot] != null) {
            retainedFrameBytes -= frames[slot].length;
        }
        cache[slot] = entry;
        frames[slot] = frame;
        if (frame != null) {
            if (retainedFrameBytes == 0) {
                oldestFrameIndex = entry.getIndex();
            }
            retainedFrameBytes += frame.length;
            // bỏ khung của các entry cũ nhất; entry của chúng vẫn nằm trong cache
            while (retainedFrameBytes > MAX_RETAINED_FRAME_BYTES && oldestFrameIndex < entry.getIndex()) {
                int oldest = (int) (oldestFrameIndex % cache.length);
                if (frames[oldest] != null && cached(oldestFrameIndex) != null) {
                    retainedFrameBytes -= frames[oldest].length;
                    frames[oldest] = null;
                }
                oldestFrameIndex++;
            }
        }
        if (entry.isConfigurationEntry()) {
            configurationIndexes.add(entry.getIndex());
        }
    }

    private LogEntry cached(long index) {
        LogEntry entry = cache[(int) (index % cache.length)];
        return entry != null && entry.getIndex() == index ? entry : null;
    }

    // khung chưa xuống file của entry index, null nếu entry đó đã nằm trong file
    private byte[] pendingFrame(long index) {
        if (tail.isEmpty() || index < tail.get(0).index()) {
            return null;
        }
        return tail.get((int) (index - tail.get(0).index())).frame();
    }

    private Segment segmentOf(long index) {
        for (int i = segments.size() - 1; i >= 0; i--) {
            if (segments.get(i).firstIndex <= index) {
                return segments.get(i);
            }
        }
        throw new IllegalStateException("No log segment holds index " + index);
    }

    private int offsetOf(long index) {
        return (int) offsets.get((int) (index - baseIndex - 1));
    }

    // chuẩn bị file cho segment kế tiếp ở một thread nền, nếu chưa có sẵn
    private synchronized void prepareSpare() {
        if (!preallocate || !preallocateNext || closed || spareReady || sparePreparing) {
            return;
        }
        sparePreparing = true;
        Thread thread = new Thread(() -> {
            boolean prepared = false;
            try {
                Segment.preallocate(spareFile, segmentBytes);
                prepared = true;
            } catch (IOException | RuntimeException e) {
                // không chuẩn bị được thì segment kế tiếp được tạo như một file thưa
            }
            synchronized (this) {
                sparePreparing = false;
                spareReady = prepared && !closed;
                if (closed) {
                    try {
                        Files.deleteIfExists(spareFile);
                    } catch (IOException e) {
                        // lần mở log sau sẽ xoá
                    }
                }
            }
        }, "raft-log-preallocate");
        thread.setDaemon(true);
        thread.start();
    }

    // file cấp phát sẵn nếu đã có, null nếu chưa; người gọi phải dùng nó ngay
    private synchronized Path takeSpare() {
        if (!spareReady) {
            return null;
        }
        spareReady = false;
        return spareFile;
    }

    // chờ tới khi không thread nào đang ghi file; gọi trong monitor
    private void awaitWriterIdle() {
        boolean interrupted = false;
        while (writerActive) {
            try {
                wait();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    // ---------- CORE LOG STORE ----------
    @Override
    public synchronized long getBaseIndex() {
        return baseIndex;
    }

    @Override
    public synchronized long getBaseTerm() {
        return baseTerm;
    }

    @Override
    public synchronized long lastIndex() {
        return baseIndex + offsets.size();
    }

    @Override
    public synchronized long lastTerm() {
        if (offsets.size() == 0) return baseTerm; // nếu empty thì term = baseTerm
        return get(lastIndex()).getTerm();
    }

    @Override
    public synchronized LogEntry get(long index) {
        if (index <= baseIndex || index > lastIndex()) return null;
        LogEntry entry = cached(index);
        if (entry != null) {
            return entry;
        }
        byte[] frame = pendingFrame(index);
        if (frame != null) {
            return EntryFrame.decode(frame);
        }
        try {
            return read(locate(index, index).get(0)).get(0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized long lastConfigurationIndex(long upTo) {
        for (int i = configurationIndexes.size() - 1; i >= 0; i--) {
            if (configurationIndexes.get(i) <= upTo) {
                return configurationIndexes.get(i);
            }
        }
        return 0;
    }

    @Override
    public synchronized void appendEntry(LogEntry entry) {
        appendEntries(List.of(entry));
    }

    // chỉ làm việc trong bộ nhớ: khung được ghi xuống file ở lần sync() kế tiếp
    @Override
    public synchronized void appendEntries(List<LogEntry> newEntries) {
        if (newEntries.isEmpty()) {
            return;
        }
        if (newEntries.get(0).getIndex() != lastIndex() + 1) {
            throw new IllegalArgumentException("Append index " + newEntries.get(0).getIndex()
                    + " does not follow last index " + lastIndex());
        }
        try {
            faults.beforeWrite("log.append");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        for (LogEntry entry : newEntries) {
            byte[] frame = EntryFrame.encode(entry);
            if (frame.length > segmentBytes - Segment.HEADER_LENGTH) {
                throw new IllegalArgumentException("Log entry of " + frame.length + " bytes does not fit in a segment of "
                        + segmentBytes + " bytes");
            }
            // một khung không bao giờ vắt qua hai segment: không đủ chỗ thì phần còn lại của segment để trống
            if (segments.isEmpty() || frame.length > segments.get(segments.size() - 1).capacity - writeOffset) {
                // một segment vừa đầy: nó tồn tại đủ lâu thì segment kế tiếp đáng được cấp phát sẵn
                long now = System.nanoTime();
                if (!segments.isEmpty()) {
                    preallocateNext = now - lastRollNanos >= MIN_SEGMENT_NANOS;
                }
                lastRollNanos = now;
                segments.add(new Segment(folder.resolve(PREFIX + entry.getIndex() + SUFFIX), entry.getIndex(), segmentBytes));
                writeOffset = Segment.HEADER_LENGTH;
            }
            Segment segment = segments.get(segments.size() - 1);
            tail.add(new Pending(entry.getIndex(), segment, writeOffset, frame));
            remember(entry, writeOffset, frame);
            writeOffset += frame.length;
            segment.endOffset = writeOffset;
        }
    }

    /**
     * Ghi mọi khung đang chờ xuống file rồi, với logSync = true, fsync. Với logSync = false, entry được coi là bền vững
     * ngay khi đã nằm trong file (page cache của hệ điều hành); việc fsync do flush() làm.
     */
    @Override
    public void sync() {
        write(forceToDisk);
    }

    @Override
    public void flush() {
        write(true);
    }

    private void write(boolean force) {
        List<Pending> batch;
        Set<Segment> toForce = new LinkedHashSet<>();
        long target;
        boolean syncDir;
        synchronized (this) {
            awaitWriterIdle();
            target = lastIndex();
            if (tail.isEmpty() && !(force && (forcedIndex < target || dirDirty))) {
                return;
            }
            batch = new ArrayList<>(tail);
            if (force) {
                // cả những segment đã được ghi từ trước mà chưa fsync (các lần sync với logSync = false)
                for (Segment segment : segments) {
                    if (segment.forcedOffset < segment.endOffset) {
                        toForce.add(segment);
                    }
                }
            }
            syncDir = force && dirDirty;
            writerActive = true;
        }
        boolean created = false;
        boolean done = false;
        try {
            faults.beforeWrite("log.sync");
            created = writeFrames(batch);
            if (force) {
                for (Segment segment : toForce) {
                    if (!segment.dropped && segment.created()) {
                        segment.force();
                        segment.forcedOffset = segment.writtenOffset;
                        // mốc này chỉ tiến sau khi dữ liệu thật sự được fsync: phần đã ghi mà chưa fsync vẫn được coi là
                        // đuôi có thể ghi dở khi mở lại
                        segment.markDurable(segment.writtenOffset);
                    }
                }
                if (syncDir || created) {
                    FileUtil.syncDirectory(folder);
                }
            }
            done = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            synchronized (this) {
                writerActive = false;
                notifyAll();
                if (done) {
                    // bỏ khỏi hàng chờ những khung vừa ghi; khung append trong lúc đang ghi vẫn nằm lại
                    int written = 0;
                    while (written < tail.size() && tail.get(written).index() <= target) {
                        written++;
                    }
                    tail.subList(0, written).clear();
                    writtenIndex = Math.max(writtenIndex, target);
                    if (force) {
                        forcedIndex = Math.max(forcedIndex, target);
                        dirDirty = false;
                    } else {
                        dirDirty |= created;
                    }
                    durableIndex = Math.max(durableIndex, forceToDisk ? forcedIndex : writtenIndex);
                } else {
                    dirDirty |= created || syncDir;
                }
            }
        }
    }

    // Ghi các khung theo từng khối liền nhau trong cùng một segment, mỗi khối một lời gọi ghi file.
    // Trả về true nếu có file segment mới được tạo.
    private boolean writeFrames(List<Pending> batch) throws IOException {
        boolean created = false;
        Segment segment = null;
        int blockStart = 0;
        ioBuffer.clear();
        for (Pending pending : batch) {
            // segment vừa bị snapshot compact mất: entry của nó đã nằm trong snapshot, không cần ghi nữa
            if (pending.segment().dropped) {
                continue;
            }
            if (pending.segment() != segment || pending.frame().length > ioBuffer.remaining()) {
                flushBlock(segment, blockStart);
                segment = pending.segment();
                blockStart = pending.offset();
                if (!segment.created()) {
                    created |= segment.open(takeSpare());
                    prepareSpare();
                }
            }
            if (pending.frame().length > ioBuffer.capacity()) {
                segment.write(ByteBuffer.wrap(pending.frame()), pending.offset());
                segment.writtenOffset = Math.max(segment.writtenOffset, pending.offset() + pending.frame().length);
                segment = null;
            } else {
                ioBuffer.put(pending.frame());
            }
        }
        flushBlock(segment, blockStart);
        return created;
    }

    private void flushBlock(Segment segment, int blockStart) throws IOException {
        if (segment != null && ioBuffer.position() > 0) {
            int end = blockStart + ioBuffer.position();
            ioBuffer.flip();
            segment.write(ioBuffer, blockStart);
            segment.writtenOffset = Math.max(segment.writtenOffset, end);
        }
        ioBuffer.clear();
    }

    @Override
    public synchronized long durableIndex() {
        return durableIndex;
    }

    @Override
    public synchronized boolean isCached(long fromIndex, int maxEntries) {
        long first = Math.max(fromIndex, baseIndex + 1);
        // bộ nhớ giữ một đoạn liền nhau ở cuối log, nên entry đầu có mặt thì cả đoạn sau nó cũng có
        return first > lastIndex() || cached(first) != null;
    }

    /**
     * Các khung của [fromIndex, ...] như chúng đã được dựng lúc append, để gửi cho follower mà không mã hoá lại.
     * null nếu khung của fromIndex không còn trong bộ nhớ.
     */
    @Override
    public synchronized Block readBlock(long fromIndex, int maxEntries) {
        if (fromIndex <= baseIndex || fromIndex > lastIndex() || maxEntries <= 0) {
            return null;
        }
        long last = Math.min(lastIndex(), fromIndex + maxEntries - 1L);
        List<byte[]> parts = new ArrayList<>();
        int bytes = 0;
        for (long index = fromIndex; index <= last; index++) {
            int slot = (int) (index % cache.length);
            byte[] frame = cached(index) == null ? null : frames[slot];
            if (frame == null || (!parts.isEmpty() && bytes + frame.length > MAX_BLOCK_BYTES)) {
                break;
            }
            parts.add(frame);
            bytes += frame.length;
        }
        if (parts.isEmpty()) {
            return null;
        }
        int total = bytes;
        // các khung không bao giờ bị sửa sau khi dựng, nên block vẫn đúng với request dù log có đổi sau đó
        return new Block(parts.size(), total, () -> {
            byte[] copy = new byte[total];
            int position = 0;
            for (byte[] part : parts) {
                System.arraycopy(part, 0, copy, position, part.length);
                position += part.length;
            }
            return copy;
        });
    }

    // các entry [first, last] nằm ở đâu trong các file; mọi entry đó phải đã được ghi xuống file
    private List<Range> locate(long first, long last) {
        List<Range> ranges = new ArrayList<>();
        long next = first;
        while (next <= last) {
            Segment segment = segmentOf(next);
            int position = segments.indexOf(segment);
            long lastOfSegment = position + 1 < segments.size() ? segments.get(position + 1).firstIndex - 1 : lastIndex();
            long lastInSegment = Math.min(last, lastOfSegment);
            int end = lastInSegment == lastOfSegment ? segment.endOffset : offsetOf(lastInSegment + 1);
            ranges.add(new Range(segment.file, offsetOf(next), end, next, (int) (lastInSegment - next + 1)));
            next = lastInSegment + 1;
        }
        return ranges;
    }

    // mở file riêng để đọc, không đụng tới state của store, nên gọi được ngoài monitor
    private static List<LogEntry> read(Range range) throws IOException {
        var data = ByteBuffer.allocate(range.end() - range.start()).order(ByteOrder.LITTLE_ENDIAN);
        try (FileChannel reader = FileChannel.open(range.file(), StandardOpenOption.READ)) {
            while (data.hasRemaining()) {
                if (reader.read(data, range.start() + data.position()) < 0) {
                    throw new IOException("Log segment " + range.file() + " is shorter than expected");
                }
            }
        }
        List<LogEntry> result = new ArrayList<>(range.count());
        int offset = 0;
        for (int i = 0; i < range.count(); i++) {
            LogEntry entry = EntryFrame.read(data, offset, data.capacity());
            if (entry == null || entry.getIndex() != range.first() + i) {
                throw new IOException("Corrupted log segment " + range.file() + " at index " + (range.first() + i));
            }
            result.add(entry);
            offset += EntryFrame.align(EntryFrame.frameLength(data, offset));
        }
        return result;
    }

    /**
     * Phần không còn trong bộ nhớ được đọc từ file ngoài monitor: trong lúc đó các thao tác khác trên log vẫn chạy.
     */
    @Override
    public List<LogEntry> readFrom(long fromIndex, int maxEntries) {
        while (true) {
            // xen kẽ theo thứ tự index: LogEntry lấy được ngay, hoặc Range cần đọc từ file
            List<Object> pieces = new ArrayList<>();
            long plannedGeneration;
            long plannedBaseIndex;
            boolean needsFile = false;
            synchronized (this) {
                plannedGeneration = generation;
                plannedBaseIndex = baseIndex;
                long first = Math.max(fromIndex, baseIndex + 1);
                long last = Math.min(lastIndex(), first + maxEntries - 1L);
                long next = first;
                while (next <= last) {
                    LogEntry entry = cached(next);
                    if (entry == null) {
                        byte[] frame = pendingFrame(next);
                        entry = frame == null ? null : EntryFrame.decode(frame);
                    }
                    if (entry != null) {
                        pieces.add(entry);
                        next++;
                        continue;
                    }
                    // đoạn chỉ còn trong file: tới chỗ bộ nhớ bắt đầu có lại
                    long gapEnd = next;
                    while (gapEnd < last && cached(gapEnd + 1) == null && pendingFrame(gapEnd + 1) == null) {
                        gapEnd++;
                    }
                    pieces.addAll(locate(next, gapEnd));
                    needsFile = true;
                    next = gapEnd + 1;
                }
            }
            List<LogEntry> result = new ArrayList<>();
            IOException failure = null;
            try {
                for (Object piece : pieces) {
                    if (piece instanceof Range range) {
                        result.addAll(read(range));
                    } else {
                        result.add((LogEntry) piece);
                    }
                }
            } catch (IOException e) {
                failure = e;
            }
            if (!needsFile) {
                return result;
            }
            synchronized (this) {
                // log bị cắt đuôi, reset hay compact trong lúc đọc thì vị trí đã lấy không còn đúng: làm lại từ đầu
                if (generation != plannedGeneration || baseIndex != plannedBaseIndex) {
                    continue;
                }
            }
            if (failure != null) {
                throw new UncheckedIOException(failure);
            }
            return result;
        }
    }

    @Override
    public synchronized void truncateSuffix(long firstIndexRemoved) {
        if (firstIndexRemoved > lastIndex()) return;
        // phần bị cắt có thể đã nằm trong file: sửa file ngay tại đây, sau khi thread đang ghi (nếu có) xong việc
        awaitWriterIdle();
        if (firstIndexRemoved > lastIndex()) return;
        long first = Math.max(firstIndexRemoved, baseIndex + 1);
        int pos = (int) (first - baseIndex - 1);
        long oldLast = lastIndex();
        Segment holder = segmentOf(first);
        int cut = offsetOf(first);
        generation++;
        while (!tail.isEmpty() && tail.get(tail.size() - 1).index() >= first) {
            tail.remove(tail.size() - 1);
        }
        try {
            // xoá từ segment cuối trở về để phần còn lại trên đĩa luôn liền mạch
            // pos == 0: không còn entry nào sau snapshot nên bỏ hết
            var deleted = false;
            while (!segments.isEmpty() && (pos == 0 || segments.get(segments.size() - 1).firstIndex >= first)) {
                Segment removed = segments.remove(segments.size() - 1);
                removed.dropped = true;
                deleted |= removed.delete();
            }
            if (deleted) {
                // nếu mất điện làm segment vừa xoá sống lại, nó sẽ chồng lên các entry ghi sau đây
                FileUtil.syncDirectory(folder);
            }
            if (!segments.isEmpty()) {
                Segment last = segments.get(segments.size() - 1);
                if (last == holder) {
                    if (cut < last.writtenOffset) {
                        // hạ mốc "đã lên đĩa" trước khi xoá khung, nếu không lần mở lại sẽ coi chỗ vừa xoá là dữ liệu bị hỏng
                        if (last.durableOffset > cut) {
                            last.markDurable(cut);
                            last.force();
                        }
                        // việc xoá phải bền vững trước khi entry mới được ghi đè lên chỗ đó, nếu không mất điện có thể
                        // để lại khung cũ nối sau khung mới
                        last.zero(cut, last.writtenOffset);
                        last.force();
                        last.writtenOffset = cut;
                        last.forcedOffset = Math.min(last.forcedOffset, cut);
                    }
                    last.endOffset = cut;
                }
                writeOffset = last.endOffset;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        for (long index = first; index <= oldLast && index < first + cache.length; index++) {
            if (cached(index) != null) {
                int slot = (int) (index % cache.length);
                cache[slot] = null;
                if (frames[slot] != null) {
                    retainedFrameBytes -= frames[slot].length;
                    frames[slot] = null;
                }
            }
        }
        offsets.truncate(pos);
        while (configurationIndexes.size() > 0 && configurationIndexes.get(configurationIndexes.size() - 1) >= first) {
            configurationIndexes.truncate(configurationIndexes.size() - 1);
        }
        writtenIndex = Math.min(writtenIndex, first - 1);
        forcedIndex = Math.min(forcedIndex, first - 1);
        durableIndex = Math.min(durableIndex, first - 1);
    }

    @Override
    public synchronized void truncatePrefix(long firstIndexKept) {
        // index = lastIncludedIndex + 1
        if (firstIndexKept <= baseIndex) return;

        long pos = firstIndexKept - baseIndex - 1;
        if (pos <= 0) return;
        if (pos > offsets.size()) pos = offsets.size();

        // entry cuối cùng nằm trong snapshot trở thành mốc mới của log
        long newBaseIndex = baseIndex + pos;
        long newBaseTerm = get(newBaseIndex).getTerm();
        baseIndex = newBaseIndex;
        baseTerm = newBaseTerm;

        offsets.dropFirst((int) pos);
        int covered = 0;
        while (covered < configurationIndexes.size() && configurationIndexes.get(covered) <= baseIndex) {
            covered++;
        }
        configurationIndexes.dropFirst(covered);
        // khung còn chờ ghi của các entry này vẫn nằm trong hàng chờ: file của segment còn giữ lại phải liền mạch từ đầu
        writtenIndex = Math.max(writtenIndex, baseIndex);
        forcedIndex = Math.max(forcedIndex, baseIndex);
        durableIndex = Math.max(durableIndex, baseIndex);

        // Segment chỉ được bỏ khi mọi entry của nó đã nằm trong snapshot. Segment có kích thước cố định nên log trên đĩa
        // giữ thừa nhiều nhất một segment; không cần ngắt segment đang ghi sau mỗi snapshot.
        try {
            dropCoveredSegments(true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void cleanup() {
        List<Segment> dropped;
        synchronized (this) {
            if (garbage.isEmpty()) {
                return;
            }
            // thread đang ghi có thể còn dùng file của một segment vừa bị bỏ; các lượt ghi sau thì không
            awaitWriterIdle();
            dropped = new ArrayList<>(garbage);
            garbage.clear();
        }
        boolean deleted = discard(dropped);
        synchronized (this) {
            dirDirty |= deleted;
        }
    }

    private static boolean discard(List<Segment> dropped) {
        boolean deleted = false;
        for (Segment segment : dropped) {
            try {
                deleted |= segment.delete();
            } catch (IOException e) {
                // còn sót lại thì lần mở log sau sẽ xoá: mọi entry của nó đều đã nằm trong snapshot
            }
        }
        return deleted;
    }

    @Override
    public synchronized void reset(long baseIndex, long baseTerm) {
        awaitWriterIdle();
        generation++;
        discard(garbage);
        garbage.clear();
        try {
            for (Segment segment : segments) {
                segment.dropped = true;
                segment.delete();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        segments.clear();
        // các entry cũ không được sống lại sau mất điện và nằm sau snapshot mới
        FileUtil.syncDirectory(folder);
        this.baseIndex = baseIndex;
        this.baseTerm = baseTerm;
        Arrays.fill(cache, null);
        Arrays.fill(frames, null);
        retainedFrameBytes = 0;
        tail.clear();
        offsets.truncate(0);
        configurationIndexes.truncate(0);
        writeOffset = 0;
        writtenIndex = baseIndex;
        forcedIndex = baseIndex;
        durableIndex = baseIndex;
    }

    // khung chưa sync() không được ghi ở đây: chúng chưa từng được coi là bền vững
    @Override
    public synchronized void close() {
        awaitWriterIdle();
        closed = true;
        if (spareReady) {
            spareReady = false;
            try {
                Files.deleteIfExists(spareFile);
            } catch (IOException e) {
                // lần mở log sau sẽ xoá
            }
        }
        discard(garbage);
        garbage.clear();
        segments.forEach(Segment::close);
    }
}

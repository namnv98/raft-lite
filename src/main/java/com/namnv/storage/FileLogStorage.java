package com.namnv.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.namnv.config.NodeOptions;
import com.namnv.entity.LogEntry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Log chia thành các segment log_&lt;firstIndex&gt;.jsonl, mỗi dòng một entry kèm CRC32.
 * Nhờ vậy mọi thao tác trong lock của node đều rẻ: append chỉ ghi nối đuôi, truncate suffix là cắt file,
 * truncate prefix là xoá nguyên segment. Việc tốn kém duy nhất là fsync, nằm riêng trong sync().
 */
public class FileLogStorage implements LogStorage {
    private static final String PREFIX = "log_";
    private static final String SUFFIX = ".jsonl";

    private record Segment(Path file, long firstIndex) {
    }

    // danh sách long không đóng hộp: mỗi entry của log có một phần tử, nên List<Long> sẽ tạo thêm một object cho mỗi entry
    private static final class LongList {
        private long[] values = new long[1024];
        private int size;

        void add(long value) {
            if (size == values.length) {
                values = java.util.Arrays.copyOf(values, size * 2);
            }
            values[size++] = value;
        }

        long get(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException(index);
            }
            return values[index];
        }

        // giữ lại newSize phần tử đầu
        void truncate(int newSize) {
            size = newSize;
        }

        void dropFirst(int count) {
            System.arraycopy(values, count, values, 0, size - count);
            size -= count;
        }

        void clear() {
            size = 0;
        }

        int size() {
            return size;
        }

        void removeLast() {
            size--;
        }
    }

    // số entry đọc một lượt khi phải lấy entry cũ từ đĩa, để đọc tuần tự không mở file cho từng entry
    private static final int READ_AHEAD = 256;

    private long baseIndex;
    private long baseTerm;

    private final Path folder;
    private final DiskFaultInjector faults;
    // một lần ghi hỏng mà không dọn được phần ghi dở: file không còn khớp với bộ nhớ, từ chối ghi tiếp
    private boolean broken;
    // Chỉ các entry mới nhất nằm trong bộ nhớ (vòng đệm theo index); entry cũ hơn được đọc lại từ segment khi cần.
    // Giữ mọi entry làm object cho tới lần snapshot kế tiếp khiến chúng sống qua nhiều lượt GC, và ở tải cao chính việc
    // GC phải chép đi chép lại chúng làm ứng dụng dừng hàng chục mili giây.
    private final LogEntry[] cache;
    // khối entry cũ vừa đọc từ đĩa, liền nhau theo index
    private List<LogEntry> readAhead = List.of();
    // endOffsets.get(i): vị trí byte kết thúc của entry baseIndex + 1 + i trong segment chứa nó
    private final LongList endOffsets = new LongList();
    // vị trí byte bắt đầu của entry baseIndex + 1 trong segment chứa nó
    private long firstEntryStartOffset;
    // index của các config entry còn trong log, tăng dần
    private final LongList configurationIndexes = new LongList();
    private final List<Segment> segments = new ArrayList<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    // channel và vị trí ghi của segment cuối
    private FileChannel channel;
    private long writeOffset;
    // sau khi truncate prefix, entry mới sang segment mới để segment cũ xoá được ở lần snapshot sau
    private boolean rollOnAppend;
    // channel của segment đã thôi ghi nhưng chưa fsync
    private final List<FileChannel> retired = new ArrayList<>();
    private boolean dirDirty;

    // index lớn nhất đã chắc chắn nằm trên đĩa
    private long durableIndex;
    // tăng mỗi lần cắt bỏ entry, để sync() đang chạy dở biết kết quả của nó không còn đúng
    private long generation;
    // chỉ để các lời gọi fsync không chồng lên nhau, không bao giờ giữ cùng lúc với monitor của this
    private final ReentrantLock syncLock = new ReentrantLock();

    public FileLogStorage(NodeOptions nodeOptions, long baseIndex, long baseTerm) throws IOException {
        this.folder = Path.of(nodeOptions.getLogUri());
        this.faults = nodeOptions.getDiskFaults();
        this.cache = new LogEntry[Math.max(1, nodeOptions.getLogCacheEntries())];
        Files.createDirectories(folder);
        this.baseIndex = baseIndex;
        this.baseTerm = baseTerm;
        loadFromFiles();
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

    private synchronized void loadFromFiles() throws IOException {
        List<Path> files = new ArrayList<>();
        try (var children = Files.list(folder)) {
            children.filter(p -> firstIndexOf(p) > 0).forEach(files::add);
        }
        files.sort(Comparator.comparingLong(FileLogStorage::firstIndexOf));

        long nextIndex = -1;
        for (int f = 0; f < files.size(); f++) {
            Path file = files.get(f);
            boolean lastFile = f == files.size() - 1;
            long firstIndex = firstIndexOf(file);
            if (nextIndex >= 0 && firstIndex != nextIndex) {
                throw new IOException("Log segment " + file + " does not follow index " + (nextIndex - 1));
            }
            nextIndex = firstIndex;
            segments.add(new Segment(file, firstIndex));

            byte[] data = Files.readAllBytes(file);
            int lineStart = 0;
            while (lineStart < data.length) {
                int lineEnd = lineStart;
                while (lineEnd < data.length && data[lineEnd] != '\n') {
                    lineEnd++;
                }
                LogEntry entry = lineEnd < data.length ? decode(data, lineStart, lineEnd) : null;
                if (entry == null) {
                    // dòng hỏng chỉ được coi là đuôi ghi dở do crash (chưa từng được ack) khi sau nó không còn
                    // entry nguyên vẹn nào. Nếu còn thì đây là dữ liệu đã ghi xong bị hỏng: không được âm thầm cắt bỏ.
                    if (!lastFile || hasIntactLineAfter(data, lineEnd + 1)) {
                        throw new IOException("Corrupted log segment " + file + " at offset " + lineStart);
                    }
                    try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
                        ch.truncate(lineStart);
                        ch.force(true);
                    }
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
                    if (endOffsets.size() == 0) {
                        firstEntryStartOffset = lineStart;
                    }
                    remember(entry, lineEnd + 1L);
                }
                nextIndex++;
                lineStart = lineEnd + 1;
            }
        }
        dropCoveredSegments();
        if (!segments.isEmpty()) {
            openLastSegment();
        }
        durableIndex = lastIndex();
    }

    // entry của dòng data[start, end), null nếu sai checksum hoặc không đọc được
    private LogEntry decode(byte[] data, int start, int end) {
        byte[] payload = Checksum.decodeLine(data, start, end);
        if (payload == null) {
            return null;
        }
        try {
            return objectMapper.readValue(payload, LogEntry.class);
        } catch (IOException e) {
            return null;
        }
    }

    private boolean hasIntactLineAfter(byte[] data, int from) {
        int lineStart = from;
        while (lineStart < data.length) {
            int lineEnd = lineStart;
            while (lineEnd < data.length && data[lineEnd] != '\n') {
                lineEnd++;
            }
            if (lineEnd < data.length && decode(data, lineStart, lineEnd) != null) {
                return true;
            }
            lineStart = lineEnd + 1;
        }
        return false;
    }

    private void openLastSegment() throws IOException {
        channel = FileChannel.open(segments.get(segments.size() - 1).file(), StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
        writeOffset = channel.size();
    }

    private void closeChannel() throws IOException {
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    private void deleteLastSegment() throws IOException {
        Files.delete(segments.remove(segments.size() - 1).file());
        dirDirty = true;
    }

    // xoá các segment mà mọi entry đều đã nằm trong snapshot
    private void dropCoveredSegments() throws IOException {
        while (segments.size() > 1 && segments.get(1).firstIndex() <= baseIndex + 1) {
            Files.delete(segments.remove(0).file());
            dirDirty = true;
        }
        if (segments.size() == 1 && endOffsets.size() == 0) {
            closeChannel();
            deleteLastSegment();
        }
    }

    // ghi nhận một entry vừa nằm vào cuối log
    private void remember(LogEntry entry, long endOffset) {
        endOffsets.add(endOffset);
        cache[(int) (entry.getIndex() % cache.length)] = entry;
        if (entry.isConfigurationEntry()) {
            configurationIndexes.add(entry.getIndex());
        }
    }

    private LogEntry cached(long index) {
        LogEntry entry = cache[(int) (index % cache.length)];
        return entry != null && entry.getIndex() == index ? entry : null;
    }

    private Segment segmentOf(long index) {
        for (int i = segments.size() - 1; i >= 0; i--) {
            if (segments.get(i).firstIndex() <= index) {
                return segments.get(i);
            }
        }
        throw new IllegalStateException("No log segment holds index " + index);
    }

    private long startOffset(long index, Segment segment) {
        if (index == baseIndex + 1) {
            return firstEntryStartOffset;
        }
        return segment.firstIndex() == index ? 0 : endOffsets.get((int) (index - baseIndex - 2));
    }

    // đọc các entry [first, last] từ segment trên đĩa; cả hai phải nằm trong log
    private List<LogEntry> readFromDisk(long first, long last) {
        List<LogEntry> result = new ArrayList<>();
        long next = first;
        try {
            while (next <= last) {
                Segment segment = segmentOf(next);
                // phần cần đọc nằm trong segment này tới đâu
                long lastInSegment = last;
                int position = segments.indexOf(segment);
                if (position + 1 < segments.size()) {
                    lastInSegment = Math.min(last, segments.get(position + 1).firstIndex() - 1);
                }
                long start = startOffset(next, segment);
                long end = endOffsets.get((int) (lastInSegment - baseIndex - 1));
                byte[] data = new byte[(int) (end - start)];
                try (FileChannel reader = FileChannel.open(segment.file(), StandardOpenOption.READ)) {
                    ByteBuffer buffer = ByteBuffer.wrap(data);
                    while (buffer.hasRemaining()) {
                        if (reader.read(buffer, start + buffer.position()) < 0) {
                            throw new IOException("Log segment " + segment.file() + " is shorter than expected");
                        }
                    }
                }
                int lineStart = 0;
                while (lineStart < data.length) {
                    int lineEnd = lineStart;
                    while (lineEnd < data.length && data[lineEnd] != '\n') {
                        lineEnd++;
                    }
                    LogEntry entry = decode(data, lineStart, lineEnd);
                    if (entry == null || entry.getIndex() != next) {
                        throw new IOException("Corrupted log segment " + segment.file() + " at index " + next);
                    }
                    result.add(entry);
                    next++;
                    lineStart = lineEnd + 1;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return result;
    }

    private void rollSegment(long firstIndex) throws IOException {
        if (channel != null) {
            retired.add(channel);
        }
        Path file = folder.resolve(PREFIX + firstIndex + SUFFIX);
        channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        writeOffset = 0;
        segments.add(new Segment(file, firstIndex));
        dirDirty = true;
        rollOnAppend = false;
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
        return baseIndex + endOffsets.size();
    }

    @Override
    public synchronized long lastTerm() {
        if (endOffsets.size() == 0) return baseTerm; // nếu empty thì term = baseTerm
        return get(lastIndex()).getTerm();
    }

    @Override
    public synchronized LogEntry get(long index) {
        if (index <= baseIndex || index > lastIndex()) return null;
        LogEntry entry = cached(index);
        if (entry != null) {
            return entry;
        }
        if (!readAhead.isEmpty()) {
            long offset = index - readAhead.get(0).getIndex();
            if (offset >= 0 && offset < readAhead.size()) {
                return readAhead.get((int) offset);
            }
        }
        readAhead = readFromDisk(index, Math.min(lastIndex(), index + READ_AHEAD - 1));
        return readAhead.get(0);
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

    @Override
    public synchronized void appendEntries(List<LogEntry> newEntries) {
        if (newEntries.isEmpty()) {
            return;
        }
        if (newEntries.get(0).getIndex() != lastIndex() + 1) {
            throw new IllegalArgumentException("Append index " + newEntries.get(0).getIndex()
                    + " does not follow last index " + lastIndex());
        }
        if (broken) {
            throw new IllegalStateException("Log storage " + folder + " is broken after a failed write");
        }
        try {
            faults.beforeWrite("log.append");
            if (channel == null || rollOnAppend) {
                rollSegment(newEntries.get(0).getIndex());
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            long[] offsets = new long[newEntries.size()];
            int written = 0;
            for (LogEntry entry : newEntries) {
                out.write(Checksum.encodeLine(objectMapper.writeValueAsBytes(entry)));
                offsets[written++] = writeOffset + out.size();
            }
            writeFully(channel, ByteBuffer.wrap(out.toByteArray()));
            writeOffset += out.size();
            for (int i = 0; i < offsets.length; i++) {
                remember(newEntries.get(i), offsets[i]);
            }
        } catch (IOException e) {
            discardPartialWrite();
            throw new UncheckedIOException(e);
        }
    }

    protected void writeFully(FileChannel target, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            target.write(buffer);
        }
    }

    // lần ghi thất bại có thể đã để lại vài byte: cắt bỏ để file vẫn khớp với các entry trong bộ nhớ,
    // nếu không entry ghi sau đó sẽ nằm sau một dòng rác và bị bỏ khi mở lại
    private void discardPartialWrite() {
        if (channel == null) {
            return;
        }
        try {
            channel.truncate(writeOffset);
        } catch (IOException e) {
            broken = true;
        }
    }

    @Override
    public void sync() {
        while (true) {
            FileChannel target;
            List<FileChannel> toClose;
            long targetIndex;
            long targetGeneration;
            boolean syncDir;
            synchronized (this) {
                if (durableIndex >= lastIndex() && retired.isEmpty() && !dirDirty) {
                    return;
                }
                target = channel;
                toClose = new ArrayList<>(retired);
                retired.clear();
                targetIndex = lastIndex();
                targetGeneration = generation;
                syncDir = dirDirty;
                dirDirty = false;
            }
            syncLock.lock();
            try {
                faults.beforeWrite("log.sync");
                for (FileChannel old : toClose) {
                    force(old);
                    old.close();
                }
                if (target != null) {
                    force(target);
                }
                if (syncDir) {
                    forceDirectory();
                }
            } catch (IOException e) {
                synchronized (this) {
                    retired.addAll(toClose);
                    dirDirty |= syncDir;
                }
                throw new UncheckedIOException(e);
            } finally {
                syncLock.unlock();
            }
            synchronized (this) {
                // nếu trong lúc fsync có entry bị cắt bỏ thì targetIndex không còn nghĩa, làm lại
                if (generation == targetGeneration) {
                    durableIndex = Math.max(durableIndex, targetIndex);
                    return;
                }
            }
        }
    }

    private static void force(FileChannel ch) throws IOException {
        try {
            ch.force(false);
        } catch (ClosedChannelException e) {
            // segment đã bị xoá hoặc store đã đóng
        }
    }

    // để file segment mới tạo / vừa xoá không biến mất hay sống lại sau khi mất điện
    private void forceDirectory() {
        FileUtil.syncDirectory(folder);
    }

    @Override
    public synchronized long durableIndex() {
        return durableIndex;
    }

    @Override
    public synchronized List<LogEntry> readFrom(long fromIndex, int maxEntries) {
        long first = Math.max(fromIndex, baseIndex + 1);
        long last = Math.min(lastIndex(), first + maxEntries - 1L);
        List<LogEntry> result = new ArrayList<>();
        long next = first;
        while (next <= last) {
            LogEntry entry = cached(next);
            if (entry != null) {
                result.add(entry);
                next++;
                continue;
            }
            // đoạn không còn trong bộ nhớ: đọc từ đĩa tới chỗ bộ nhớ bắt đầu có lại
            long gapEnd = next;
            while (gapEnd < last && cached(gapEnd + 1) == null) {
                gapEnd++;
            }
            result.addAll(readFromDisk(next, gapEnd));
            next = gapEnd + 1;
        }
        return result;
    }

    @Override
    public synchronized void truncateSuffix(long firstIndexRemoved) {
        if (firstIndexRemoved > lastIndex()) return;
        long first = Math.max(firstIndexRemoved, baseIndex + 1);
        int pos = (int) (first - baseIndex - 1);
        long oldLast = lastIndex();
        generation++;
        try {
            // xoá từ segment cuối trở về để phần còn lại trên đĩa luôn liền mạch
            // pos == 0: không còn entry nào sau snapshot nên bỏ hết
            var deleted = false;
            while (!segments.isEmpty() && (pos == 0 || segments.get(segments.size() - 1).firstIndex() >= first)) {
                closeChannel();
                deleteLastSegment();
                deleted = true;
            }
            if (deleted) {
                // nếu mất điện làm segment vừa xoá sống lại, nó sẽ chồng lên các entry ghi sau đây
                FileUtil.syncDirectory(folder);
            }
            if (!segments.isEmpty()) {
                if (channel == null) {
                    openLastSegment();
                }
                writeOffset = endOffsets.get(pos - 1);
                channel.truncate(writeOffset);
                // việc cắt phải bền vững trước khi entry mới được ghi đè lên chỗ đó, nếu không mất điện có thể
                // để lại file trộn giữa nội dung cũ và mới
                channel.force(true);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        for (long index = first; index <= oldLast && index < first + cache.length; index++) {
            if (cached(index) != null) {
                cache[(int) (index % cache.length)] = null;
            }
        }
        readAhead = List.of();
        endOffsets.truncate(pos);
        while (configurationIndexes.size() > 0 && configurationIndexes.get(configurationIndexes.size() - 1) >= first) {
            configurationIndexes.removeLast();
        }
        if (pos == 0) {
            firstEntryStartOffset = 0;
        }
        durableIndex = Math.min(durableIndex, first - 1);
    }

    @Override
    public synchronized void truncatePrefix(long firstIndexKept) {
        // index = lastIncludedIndex + 1
        if (firstIndexKept <= baseIndex) return;

        long pos = firstIndexKept - baseIndex - 1;
        if (pos <= 0) return;
        if (pos > endOffsets.size()) pos = endOffsets.size();

        // entry cuối cùng nằm trong snapshot trở thành mốc mới của log
        long newBaseIndex = baseIndex + pos;
        long newBaseTerm = get(newBaseIndex).getTerm();
        long newBaseEndOffset = endOffsets.get((int) pos - 1);
        long newFirstStart = 0;
        if (pos < endOffsets.size()) {
            // entry đầu tiên còn lại bắt đầu ngay sau mốc mới nếu cả hai nằm chung một segment
            newFirstStart = segmentOf(newBaseIndex + 1).firstIndex() == newBaseIndex + 1 ? 0 : newBaseEndOffset;
        }
        baseIndex = newBaseIndex;
        baseTerm = newBaseTerm;
        firstEntryStartOffset = newFirstStart;

        // xóa prefix
        endOffsets.dropFirst((int) pos);
        int covered = 0;
        while (covered < configurationIndexes.size() && configurationIndexes.get(covered) <= baseIndex) {
            covered++;
        }
        configurationIndexes.dropFirst(covered);
        readAhead = List.of();
        durableIndex = Math.max(durableIndex, baseIndex);

        try {
            dropCoveredSegments();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        rollOnAppend = true;
    }

    @Override
    public synchronized void reset(long baseIndex, long baseTerm) {
        generation++;
        try {
            while (!segments.isEmpty()) {
                closeChannel();
                deleteLastSegment();
            }
            // các entry cũ không được sống lại sau mất điện và nằm sau snapshot mới
            FileUtil.syncDirectory(folder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.baseIndex = baseIndex;
        this.baseTerm = baseTerm;
        java.util.Arrays.fill(cache, null);
        readAhead = List.of();
        endOffsets.clear();
        configurationIndexes.clear();
        firstEntryStartOffset = 0;
        durableIndex = baseIndex;
    }

    @Override
    public synchronized void close() {
        try {
            for (FileChannel old : retired) {
                old.close();
            }
            retired.clear();
            closeChannel();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

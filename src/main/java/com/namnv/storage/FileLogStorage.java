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

    private long baseIndex;
    private long baseTerm;

    private final Path folder;
    private final DiskFaultInjector faults;
    // một lần ghi hỏng mà không dọn được phần ghi dở: file không còn khớp với bộ nhớ, từ chối ghi tiếp
    private boolean broken;
    private final List<LogEntry> entries = new ArrayList<>();
    // endOffsets.get(i): vị trí byte kết thúc của entries.get(i) trong segment chứa nó
    private final List<Long> endOffsets = new ArrayList<>();
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
                    if (entry.getIndex() != baseIndex + entries.size() + 1) {
                        throw new IOException("Log has a gap before index " + entry.getIndex());
                    }
                    entries.add(entry);
                    endOffsets.add((long) lineEnd + 1);
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
        if (segments.size() == 1 && entries.isEmpty()) {
            closeChannel();
            deleteLastSegment();
        }
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
        return baseIndex + entries.size();
    }

    @Override
    public synchronized long lastTerm() {
        if (entries.isEmpty()) return baseTerm; // nếu empty thì term = baseTerm
        return entries.get(entries.size() - 1).getTerm();
    }

    @Override
    public synchronized LogEntry get(long index) {
        if (index <= baseIndex) return null;
        long pos = index - baseIndex - 1;
        if (pos < 0 || pos >= entries.size()) return null;
        return entries.get((int) pos);
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
            List<Long> offsets = new ArrayList<>();
            for (LogEntry entry : newEntries) {
                out.write(Checksum.encodeLine(objectMapper.writeValueAsBytes(entry)));
                offsets.add(writeOffset + out.size());
            }
            writeFully(channel, ByteBuffer.wrap(out.toByteArray()));
            writeOffset += out.size();
            entries.addAll(newEntries);
            endOffsets.addAll(offsets);
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
    public synchronized List<LogEntry> readFrom(long fromIndex) {
        if (fromIndex <= baseIndex) fromIndex = baseIndex + 1;
        long pos = fromIndex - baseIndex - 1;
        if (pos < 0 || pos >= entries.size()) return new ArrayList<>();
        return new ArrayList<>(entries.subList((int) pos, entries.size()));
    }

    @Override
    public synchronized void truncateSuffix(long firstIndexRemoved) {
        if (firstIndexRemoved > lastIndex()) return;
        long first = Math.max(firstIndexRemoved, baseIndex + 1);
        int pos = (int) (first - baseIndex - 1);
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
        entries.subList(pos, entries.size()).clear();
        endOffsets.subList(pos, endOffsets.size()).clear();
        durableIndex = Math.min(durableIndex, first - 1);
    }

    @Override
    public synchronized void truncatePrefix(long firstIndexKept) {
        // index = lastIncludedIndex + 1
        if (firstIndexKept <= baseIndex) return;

        long pos = firstIndexKept - baseIndex - 1;
        if (pos <= 0) return;
        if (pos > entries.size()) pos = entries.size();

        // cập nhật baseIndex + baseTerm theo phần tử ngay trước pos
        LogEntry lastIncluded = entries.get((int) pos - 1);
        baseIndex = lastIncluded.getIndex();
        baseTerm = lastIncluded.getTerm();

        // xóa prefix
        entries.subList(0, (int) pos).clear();
        endOffsets.subList(0, (int) pos).clear();
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
        entries.clear();
        endOffsets.clear();
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

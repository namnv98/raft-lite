package com.namnv.storage.binary;

import com.namnv.config.NodeOptions;
import com.namnv.entity.ConfigurationEntry;
import com.namnv.entity.LogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BinaryLogStorageTest {
    // đủ cho khoảng 15 entry nhỏ, để các test đi qua nhiều segment
    private static final int SEGMENT_BYTES = 1024;

    @TempDir
    Path dir;

    private NodeOptions.NodeOptionsBuilder options() {
        return NodeOptions.builder().logUri(dir.toString()).logSegmentBytes(SEGMENT_BYTES);
    }

    private BinaryLogStorage open(long baseIndex, long baseTerm) throws IOException {
        return new BinaryLogStorage(options().build(), baseIndex, baseTerm);
    }

    private static List<LogEntry> entries(long from, long to, long term) {
        var list = new ArrayList<LogEntry>();
        for (long i = from; i <= to; i++) {
            list.add(new LogEntry(i, term, ("cmd" + i).getBytes(StandardCharsets.UTF_8)));
        }
        return list;
    }

    private List<String> segmentFiles() throws IOException {
        try (var files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString()).filter(name -> name.endsWith(".rec"))
                    .sorted(java.util.Comparator.comparingLong(name -> Long.parseLong(name.replaceAll("\\D", "")))).toList();
        }
    }

    // vị trí trong file của khung thứ n (từ 0) của một segment
    private long frameOffset(String segment, int n) throws IOException {
        var data = ByteBuffer.wrap(Files.readAllBytes(dir.resolve(segment))).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        int offset = Segment.HEADER_LENGTH;
        for (int i = 0; i < n; i++) {
            offset += EntryFrame.align(data.getInt(offset));
        }
        return offset;
    }

    private void overwrite(String segment, long position, byte[] bytes) throws IOException {
        try (FileChannel channel = FileChannel.open(dir.resolve(segment), StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(bytes), position);
        }
    }

    @Test
    void everyKindOfEntrySurvivesReopen() throws IOException {
        byte[] binary = new byte[300];
        new Random(7).nextBytes(binary);
        var written = List.of(
                new LogEntry(1, 1, null),
                new LogEntry(2, 1, binary, "client-1", 42),
                new LogEntry(3, 1, new byte[0], null, 0),
                new LogEntry(4, 2, new byte[]{1}, "quote\" slash\\ é", 7),
                LogEntry.newSessionClose(5, 2, "client-1"),
                LogEntry.newConfigurationEntry(6, 2, new ConfigurationEntry(List.of("a", "b"), List.of("b", "c"), true)),
                LogEntry.newConfigurationEntry(7, 2, new ConfigurationEntry(List.of("b", "c"))),
                new LogEntry(8, Long.MAX_VALUE, new byte[]{1, 2}, "c", Long.MAX_VALUE));
        var log = open(0, 0);
        log.appendEntries(written.subList(0, 3));
        for (LogEntry entry : written.subList(3, written.size())) {
            log.appendEntry(entry);
        }
        assertEquals(written, log.readFrom(1));
        assertEquals(7, log.lastConfigurationIndex(8));
        assertEquals(6, log.lastConfigurationIndex(6));
        log.sync();
        log.close();

        var reopened = open(0, 0);
        assertEquals(written, reopened.readFrom(1));
        assertEquals(Long.MAX_VALUE, reopened.lastTerm());
        assertEquals(7, reopened.lastConfigurationIndex(8));
        reopened.close();
    }

    @Test
    void durableIndexOnlyAdvancesOnSync() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 3, 1));
        assertEquals(0, log.durableIndex());

        log.sync();
        assertEquals(3, log.durableIndex());

        log.truncateSuffix(3);
        assertEquals(2, log.durableIndex());
        log.close();
    }

    @Test
    void framesNeverSpanSegmentsAndSegmentsHaveFixedSize() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 60, 1));
        log.sync();
        assertEquals(60, log.durableIndex());
        var files = segmentFiles();
        assertTrue(files.size() >= 3, "expected several segments but got " + files);
        for (String file : files) {
            assertEquals(SEGMENT_BYTES, Files.size(dir.resolve(file)));
        }
        log.close();

        var reopened = open(0, 0);
        assertEquals(entries(1, 60, 1), reopened.readFrom(1));
        reopened.appendEntries(entries(61, 70, 2));
        assertEquals(2, reopened.lastTerm());
        assertEquals(1, reopened.get(60).getTerm());
        reopened.close();
    }

    @Test
    void entryLargerThanASegmentIsRejected() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 2, 1));
        assertThrows(IllegalArgumentException.class,
                () -> log.appendEntry(new LogEntry(3, 1, new byte[SEGMENT_BYTES])));
        assertEquals(2, log.lastIndex());
        log.appendEntries(entries(3, 3, 1));
        assertEquals(3, log.lastIndex());
        log.close();
    }

    @Test
    void prefixTruncationDropsWholeSegmentsAndSurvivesReopen() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 60, 1));
        log.sync();
        var before = segmentFiles();
        assertTrue(before.size() >= 3, "expected several segments but got " + before);

        // snapshot tới index 50: chỉ những segment nằm trọn trong snapshot bị xoá, và chỉ khi cleanup() chạy
        log.truncatePrefix(51);
        assertEquals(before, segmentFiles());
        log.cleanup();
        var after = segmentFiles();
        assertTrue(after.size() < before.size() && before.containsAll(after), before + " -> " + after);
        // segment chứa entry 51 còn nguyên, kể cả các entry đứng trước 51 trong nó
        long firstKept = Long.parseLong(after.get(0).replaceAll("\\D", ""));
        assertTrue(firstKept <= 51 && firstKept > 1, "first kept segment starts at " + firstKept);
        assertNull(log.get(50));
        assertEquals(entries(51, 60, 1), log.readFrom(1));
        log.appendEntries(entries(61, 62, 2));
        log.sync();
        log.close();

        var reopened = open(50, 1);
        assertEquals(62, reopened.lastIndex());
        assertNull(reopened.get(50));
        assertEquals(51, reopened.get(51).getIndex());
        assertEquals(2, reopened.readFrom(61).size());
        assertEquals(after.get(0), segmentFiles().get(0));
        reopened.close();
    }

    @Test
    void suffixTruncationAcrossSegmentsSurvivesReopen() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 5, 1));
        log.truncatePrefix(4);
        log.cleanup();
        log.appendEntries(entries(6, 8, 1));
        log.sync();

        log.truncateSuffix(5); // bỏ cả segment thứ hai và entry 5 của segment đầu
        assertEquals(4, log.lastIndex());
        assertEquals(List.of("log_1.rec"), segmentFiles());

        log.appendEntries(entries(5, 6, 2));
        log.sync();
        log.close();

        var reopened = open(3, 1);
        assertEquals(6, reopened.lastIndex());
        assertEquals(1, reopened.get(4).getTerm());
        assertEquals(2, reopened.get(5).getTerm());
        assertEquals(2, reopened.lastTerm());

        reopened.truncateSuffix(4); // không còn gì sau snapshot
        assertEquals(3, reopened.lastIndex());
        assertEquals(List.of(), segmentFiles());
        reopened.appendEntries(entries(4, 4, 3));
        assertEquals(3, reopened.lastTerm());
        reopened.close();
    }

    @Test
    void entriesCutFromASyncedLogDoNotComeBack() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 40, 1));
        log.sync();
        // cắt giữa một segment cũ: các segment sau bị xoá, phần còn lại của segment này phải sạch
        log.truncateSuffix(5);
        log.appendEntry(new LogEntry(5, 2, new byte[]{9}));
        log.sync();
        assertEquals(5, log.durableIndex());
        log.close();

        var reopened = open(0, 0);
        assertEquals(5, reopened.lastIndex());
        assertEquals(2, reopened.lastTerm());
        assertEquals(entries(1, 4, 1), reopened.readFrom(1, 4));
        reopened.close();
    }

    @Test
    void halfWrittenFrameIsDiscardedOnReopen() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 3, 1));
        log.sync();
        log.close();
        // crash giữa lúc ghi khung thứ tư: thân đã có vài byte, độ dài chưa được ghi
        overwrite("log_1.rec", frameOffset("log_1.rec", 3) + 8, new byte[]{4, 0, 0, 0, 0, 0, 0, 0, 1});

        var reopened = open(0, 0);
        assertEquals(3, reopened.lastIndex());
        reopened.appendEntry(new LogEntry(4, 1, null)); // ngắn hơn phần rác để lại
        reopened.appendEntries(entries(5, 5, 1));
        reopened.sync();
        reopened.close();

        var again = open(0, 0);
        assertEquals(5, again.lastIndex());
        assertEquals("cmd5", new String(again.get(5).getCommand(), StandardCharsets.UTF_8));
        again.close();
    }

    @Test
    void unsyncedTailBehindAHoleIsDiscardedOnReopen() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 3, 1));
        log.sync();
        log.close();
        // ghi tiếp mà không fsync: các entry này nằm trong file nhưng chưa từng được coi là đã lên đĩa
        var unsynced = new BinaryLogStorage(options().logSync(false).build(), 0, 0);
        unsynced.appendEntries(entries(4, 30, 1)); // đã sang segment khác
        unsynced.sync();
        unsynced.close();
        assertTrue(segmentFiles().size() > 1);
        // mất điện: trang chứa khung 4 chưa kịp xuống đĩa, các khung sau nó thì đã xuống
        long hole = frameOffset("log_1.rec", 3);
        overwrite("log_1.rec", hole, new byte[64]);

        var reopened = open(0, 0);
        assertEquals(3, reopened.lastIndex());
        assertEquals(List.of("log_1.rec"), segmentFiles());
        reopened.appendEntries(entries(4, 5, 2));
        reopened.sync();
        reopened.close();

        var again = open(0, 0);
        assertEquals(5, again.lastIndex());
        assertEquals(2, again.lastTerm());
        again.close();
    }

    @Test
    void damagedSyncedEntryIsReportedNotTruncated() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 3, 1));
        log.sync();
        log.close();
        // một bit hỏng trong lệnh của entry 2, đã được sync
        long position = frameOffset("log_1.rec", 1) + EntryFrame.HEADER_LENGTH + 1;
        overwrite("log_1.rec", position, new byte[]{'X'});

        assertThrows(IOException.class, () -> open(0, 0));
    }

    @Test
    void damagedEntryInAnOlderSegmentIsReported() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 40, 1));
        log.sync();
        log.close();
        overwrite("log_1.rec", frameOffset("log_1.rec", 2) + EntryFrame.HEADER_LENGTH + 1, new byte[]{'X'});

        assertThrows(IOException.class, () -> open(0, 0));
    }

    @Test
    void failedSyncDoesNotMarkEntriesDurable() throws IOException {
        var failSync = new boolean[1];
        var log = new BinaryLogStorage(options().diskFaults(operation -> {
            if (failSync[0] && operation.equals("log.sync")) {
                throw new IOException("injected");
            }
        }).build(), 0, 0);
        log.appendEntries(entries(1, 20, 1));
        failSync[0] = true;
        assertThrows(UncheckedIOException.class, log::sync);
        assertEquals(0, log.durableIndex());

        failSync[0] = false;
        log.sync();
        assertEquals(20, log.durableIndex());
        log.close();
    }

    @Test
    void blockOfRawFramesTravelsThroughAppendEntriesUnchanged() throws IOException {
        var log = open(0, 0);
        var written = new ArrayList<>(entries(1, 40, 3));
        written.add(new LogEntry(41, 3, new byte[]{7}, "client-1", 9));
        written.add(LogEntry.newConfigurationEntry(42, 3, new ConfigurationEntry(List.of("a", "b"))));
        log.appendEntries(written);

        // nối các block lại phải ra đúng log, kể cả khi một block vắt qua nhiều segment
        var received = new ArrayList<LogEntry>();
        int blocks = 0;
        while (received.size() < written.size()) {
            long next = received.size() + 1;
            var block = log.readBlock(next, 10);
            var request = new com.namnv.rpc.model.request.AppendEntriesRequest(3, "leader", next - 1, 3, block, 0);
            assertEquals(block.count(), request.entryCount());
            assertEquals(written.subList(received.size(), received.size() + block.count()), request.entries());

            var wire = new java.io.ByteArrayOutputStream();
            com.namnv.rpc.RpcCodec.write(new java.io.DataOutputStream(wire), 1, request);
            var decoded = (com.namnv.rpc.model.request.AppendEntriesRequest) com.namnv.rpc.RpcCodec.read(
                    new java.io.DataInputStream(new java.io.ByteArrayInputStream(wire.toByteArray()))).message();
            assertEquals(next - 1, decoded.prevLogIndex);
            received.addAll(decoded.entries());
            blocks++;
        }
        assertEquals(written, received);
        assertTrue(blocks > 1);
        assertEquals(5, log.readBlock(10, 5).count());
        assertNull(log.readBlock(43, 10));
        log.close();
    }

    @Test
    void blockKeepsTheEntriesItWasTakenWithWhateverHappensToTheLog() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 60, 1));
        log.sync();
        var early = log.readBlock(1, 5);
        var late = log.readBlock(56, 5);

        // snapshot compact phần đầu, rồi leader mới ghi đè phần đuôi: block lấy từ trước vẫn mang đúng các entry cũ,
        // như một request mang danh sách entry
        log.truncatePrefix(51);
        log.cleanup();
        log.truncateSuffix(56);
        log.appendEntries(entries(56, 60, 2));
        log.sync();

        assertEquals(entries(1, 5, 1), EntryFrame.readAll(ByteBuffer.wrap(early.copy()), early.count()));
        assertEquals(entries(56, 60, 1), EntryFrame.readAll(ByteBuffer.wrap(late.copy()), late.count()));
        assertEquals(entries(56, 60, 2), EntryFrame.readAll(ByteBuffer.wrap(log.readBlock(56, 5).copy()), 5));
        log.close();
    }

    @Test
    void appendsStayInMemoryAndNeverWaitForTheDisk() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var log = new BinaryLogStorage(options().diskFaults(operation -> {
            if (operation.equals("log.sync")) {
                // đĩa treo giữa một lần ghi
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        }).build(), 0, 0);
        log.appendEntries(entries(1, 20, 1));
        // chưa sync: chưa có file nào, nhưng log đọc được đầy đủ
        assertEquals(List.of(), segmentFiles());
        assertEquals(entries(1, 20, 1), log.readFrom(1));

        var writer = new Thread(log::sync);
        writer.start();
        assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
        // trong lúc thread ghi còn kẹt với đĩa, append và đọc vẫn chạy bình thường
        log.appendEntries(entries(21, 40, 1));
        assertEquals(40, log.lastIndex());
        assertEquals(entries(15, 30, 1), log.readFrom(15, 16));
        assertEquals(0, log.durableIndex());

        release.countDown();
        writer.join();
        assertEquals(20, log.durableIndex());
        log.sync();
        assertEquals(40, log.durableIndex());
        log.close();

        var reopened = open(0, 0);
        assertEquals(entries(1, 40, 1), reopened.readFrom(1));
        reopened.close();
    }

    @Test
    void withoutLogSyncEntriesCountAsDurableButTheTailStaysDisposable() throws IOException {
        var log = new BinaryLogStorage(options().logSync(false).build(), 0, 0);
        log.appendEntries(entries(1, 30, 1));
        log.sync();
        assertEquals(30, log.durableIndex());
        log.close();
        // tiến trình bị kill: mọi thứ đã ghi vẫn còn trong file
        var reopened = new BinaryLogStorage(options().logSync(false).build(), 0, 0);
        assertEquals(entries(1, 30, 1), reopened.readFrom(1));
        reopened.close();

        // mất điện làm mất trang chứa khung 4: vì chưa từng ép xuống đĩa nên đây là đuôi ghi dở, không phải dữ liệu hỏng
        overwrite("log_1.rec", frameOffset("log_1.rec", 3), new byte[64]);
        var afterPowerLoss = new BinaryLogStorage(options().logSync(false).build(), 0, 0);
        assertEquals(3, afterPowerLoss.lastIndex());
        assertEquals(List.of("log_1.rec"), segmentFiles());
        afterPowerLoss.close();
    }

    @Test
    void oldEntriesAreReadFromTheFileWhileTheLogKeepsChanging() throws Exception {
        var log = new BinaryLogStorage(options().logCacheEntries(8).build(), 0, 0);
        log.appendEntries(entries(1, 100, 1));
        assertFalse(log.isCached(1, 10));
        assertTrue(log.isCached(93, 10));
        assertTrue(log.isCached(101, 10));
        assertEquals("cmd17", new String(log.get(17).getCommand(), StandardCharsets.UTF_8));

        // một thread đọc lại đoạn cũ liên tục trong lúc log được nối dài, compact và cắt đuôi
        var stop = new AtomicBoolean();
        var failure = new AtomicReference<Throwable>();
        var reader = new Thread(() -> {
            try {
                while (!stop.get()) {
                    var read = log.readFrom(40, 30);
                    for (int i = 1; i < read.size(); i++) {
                        assertEquals(read.get(i - 1).getIndex() + 1, read.get(i).getIndex());
                    }
                    for (LogEntry entry : read) {
                        assertEquals("cmd" + entry.getIndex(), new String(entry.getCommand(), StandardCharsets.UTF_8));
                    }
                }
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        reader.start();
        for (long next = 101; next <= 400; next += 10) {
            log.appendEntries(entries(next, next + 9, 1));
            if (next == 201) {
                log.truncatePrefix(51);
                log.cleanup();
            }
            if (next % 50 == 1) {
                log.truncateSuffix(next + 5);
                log.appendEntries(entries(next + 5, next + 9, 1));
            }
            log.sync();
        }
        stop.set(true);
        reader.join();
        assertNull(failure.get());
        assertEquals(51, log.readFrom(40, 30).get(0).getIndex());
        assertEquals(30, log.readFrom(40, 30).size());
        assertEquals(400, log.durableIndex());
        log.close();

        var reopened = open(50, 1);
        assertEquals(400, reopened.lastIndex());
        assertEquals(entries(51, 400, 1), reopened.readFrom(51));
        reopened.close();
    }
}

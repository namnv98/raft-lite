package com.namnv.storage;

import com.namnv.config.NodeOptions;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileLogStorageTest {

    @TempDir
    Path dir;

    private FileLogStorage open(long baseIndex, long baseTerm) throws IOException {
        return new FileLogStorage(NodeOptions.builder().logUri(dir.toString()).build(), baseIndex, baseTerm);
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
            return files.map(p -> p.getFileName().toString()).sorted().toList();
        }
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
    void prefixTruncationDropsWholeSegmentsAndSurvivesReopen() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 5, 1));
        log.truncatePrefix(4); // snapshot tới index 3
        log.appendEntries(entries(6, 8, 2));
        log.sync();
        assertEquals(List.of("log_1.jsonl", "log_6.jsonl"), segmentFiles());

        log.truncatePrefix(7); // snapshot tới index 6: segment đầu không còn entry nào cần giữ
        assertEquals(List.of("log_6.jsonl"), segmentFiles());
        log.close();

        var reopened = open(6, 2);
        assertEquals(8, reopened.lastIndex());
        assertNull(reopened.get(6));
        assertEquals(7, reopened.get(7).getIndex());
        assertEquals(2, reopened.readFrom(7).size());
        reopened.close();
    }

    @Test
    void suffixTruncationAcrossSegmentsSurvivesReopen() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 5, 1));
        log.truncatePrefix(4);
        log.appendEntries(entries(6, 8, 1));

        log.truncateSuffix(5); // bỏ cả segment thứ hai và entry 5 của segment đầu
        assertEquals(4, log.lastIndex());
        assertEquals(List.of("log_1.jsonl"), segmentFiles());

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
    void tornTailIsDiscardedOnReopen() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 3, 1));
        log.sync();
        log.close();
        Files.writeString(dir.resolve("log_1.jsonl"), "{\"index\":4,\"te", StandardOpenOption.APPEND);

        var reopened = open(0, 0);
        assertEquals(3, reopened.lastIndex());
        reopened.appendEntries(entries(4, 4, 1));
        reopened.sync();
        reopened.close();

        var again = open(0, 0);
        assertEquals(4, again.lastIndex());
        assertEquals("cmd4", new String(again.get(4).getCommand(), StandardCharsets.UTF_8));
        again.close();
    }

    @Test
    void failedWriteLeavesNoGarbageBehind() throws IOException {
        var failNext = new boolean[1];
        var options = NodeOptions.builder().logUri(dir.toString()).build();
        var log = new FileLogStorage(options, 0, 0) {
            @Override
            protected void writeFully(FileChannel target, ByteBuffer buffer) throws IOException {
                if (failNext[0]) {
                    // đĩa đầy giữa chừng: chỉ một nửa số byte xuống được file
                    failNext[0] = false;
                    var half = buffer.duplicate();
                    half.limit(buffer.position() + buffer.remaining() / 2);
                    super.writeFully(target, half);
                    throw new IOException("disk full");
                }
                super.writeFully(target, buffer);
            }
        };
        log.appendEntries(entries(1, 3, 1));

        failNext[0] = true;
        assertThrows(UncheckedIOException.class, () -> log.appendEntries(entries(4, 5, 1)));
        assertEquals(3, log.lastIndex());

        // các entry ghi sau đó không được nằm sau phần rác của lần ghi hỏng
        log.appendEntries(entries(4, 6, 2));
        log.sync();
        log.close();

        var reopened = open(0, 0);
        assertEquals(6, reopened.lastIndex());
        assertEquals(2, reopened.get(4).getTerm());
        reopened.close();
    }

    @Test
    void failedSyncDoesNotMarkEntriesDurable() throws IOException {
        var failNext = new boolean[1];
        var options = NodeOptions.builder().logUri(dir.toString()).diskFaults(operation -> {
            if (failNext[0] && operation.equals("log.sync")) {
                failNext[0] = false;
                throw new IOException("injected failure of " + operation);
            }
        }).build();
        var log = new FileLogStorage(options, 0, 0);
        log.appendEntries(entries(1, 3, 1));

        failNext[0] = true;
        assertThrows(UncheckedIOException.class, log::sync);
        assertEquals(0, log.durableIndex());

        log.sync();
        assertEquals(3, log.durableIndex());
        log.close();
    }

    private void flipByteOfEntry(String segment, int entryNumber) throws IOException {
        var file = dir.resolve(segment);
        var data = Files.readAllBytes(file);
        int lineStart = 0;
        for (int line = 1; line < entryNumber; line++) {
            while (data[lineStart] != '\n') {
                lineStart++;
            }
            lineStart++;
        }
        data[lineStart + 20] ^= 0x01;
        Files.write(file, data);
    }

    @Test
    void corruptedEntryInTheMiddleIsReportedNotTruncated() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 5, 1));
        log.sync();
        log.close();

        // entry 3 đã ghi xong và có thể đã được ack: không được coi nó là đuôi ghi dở rồi cắt bỏ cả 4 và 5
        flipByteOfEntry("log_1.jsonl", 3);
        assertThrows(IOException.class, () -> open(0, 0));
    }

    @Test
    void corruptedLastEntryIsTreatedAsTornTail() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 5, 1));
        log.sync();
        log.close();

        flipByteOfEntry("log_1.jsonl", 5);
        var reopened = open(0, 0);
        assertEquals(4, reopened.lastIndex());
        reopened.close();
    }

    @Test
    void corruptedEntryInAnOlderSegmentIsReported() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 5, 1));
        log.truncatePrefix(4);
        log.appendEntries(entries(6, 8, 1));
        log.sync();
        log.close();

        flipByteOfEntry("log_1.jsonl", 5);
        assertThrows(IOException.class, () -> open(3, 1));
    }

    @Test
    void segmentThatReappearsAfterPrefixTruncationIsIgnored() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 5, 1));
        log.sync();
        var oldSegment = Files.readAllBytes(dir.resolve("log_1.jsonl"));
        log.truncatePrefix(6); // snapshot tới index 5: segment bị xoá
        log.appendEntries(entries(6, 8, 2));
        log.sync();
        log.close();

        // mất điện trước khi việc xoá file kịp bền vững: segment cũ sống lại
        Files.write(dir.resolve("log_1.jsonl"), oldSegment);
        var reopened = open(5, 1);
        assertEquals(8, reopened.lastIndex());
        assertNull(reopened.get(5));
        assertEquals(2, reopened.get(6).getTerm());
        assertEquals(List.of("log_6.jsonl"), segmentFiles());
        reopened.close();
    }

    @Test
    void silentlyAlteredEntryIsDetectedByItsChecksum() throws IOException {
        var log = open(0, 0);
        log.appendEntries(entries(1, 5, 1));
        log.sync();
        log.close();

        // dòng vẫn là JSON hợp lệ, chỉ có term của entry 3 bị đổi: không có checksum thì không thể biết
        var file = dir.resolve("log_1.jsonl");
        var lines = Files.readAllLines(file);
        lines.set(2, lines.get(2).replace("\"term\":1", "\"term\":3"));
        Files.write(file, lines);
        assertThrows(IOException.class, () -> open(0, 0));
    }
}

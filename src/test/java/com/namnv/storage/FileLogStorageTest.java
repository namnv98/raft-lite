package com.namnv.storage;

import com.namnv.config.NodeOptions;
import com.namnv.entity.LogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
}

package com.namnv.storage;

import com.namnv.entity.ConfigurationEntry;
import com.namnv.statemachine.snapshot.SnapshotMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Mỗi test dựng lại trạng thái đĩa của một thời điểm crash trong lúc ghi snapshot, rồi mở lại store.
 */
class SnapshotStoreTest {

    private static final String META_FILE = "__raft_snapshot_meta.json";

    @TempDir
    Path dir;

    private static SnapshotMeta meta(long index) {
        return new SnapshotMeta(index, 1, new ConfigurationEntry(List.of("A", "B", "C")), List.of("snapshot.data"),
                Map.of("client-1", 7L));
    }

    private void commit(SnapshotStore store, long index, String content) throws IOException {
        Files.writeString(Path.of(store.prepareTemp(), "snapshot.data"), content);
        store.commit(meta(index));
    }

    private String currentData(SnapshotStore store) throws IOException {
        return Files.readString(Path.of(store.currentPath(), "snapshot.data"));
    }

    @Test
    void emptyStoreHasNoSnapshot() throws IOException {
        assertNull(new SnapshotStore(dir.toString()).getMeta());
    }

    @Test
    void commitReplacesThePreviousSnapshot() throws IOException {
        var store = new SnapshotStore(dir.toString());
        commit(store, 5, "five");
        commit(store, 9, "nine");

        assertEquals("nine", currentData(store));
        assertFalse(Files.exists(dir.resolve("snapshot_5")));

        var reopened = new SnapshotStore(dir.toString());
        assertEquals(meta(9), reopened.getMeta());
        assertEquals("nine", currentData(reopened));
    }

    @Test
    void crashWhileWritingTheNextSnapshotKeepsTheCurrentOne() throws IOException {
        var store = new SnapshotStore(dir.toString());
        commit(store, 5, "five");
        // state machine đang ghi dở vào temp thì mất điện
        Files.writeString(Path.of(store.prepareTemp(), "snapshot.data"), "half written");

        var reopened = new SnapshotStore(dir.toString());
        assertEquals(5, reopened.getMeta().getLastIncludedIndex());
        assertEquals("five", currentData(reopened));
        assertFalse(Files.exists(dir.resolve("temp")));
    }

    @Test
    void crashAfterMetaWasWrittenButBeforeRenameKeepsTheCurrentOne() throws IOException {
        var store = new SnapshotStore(dir.toString());
        commit(store, 5, "five");
        var temp = Path.of(store.prepareTemp());
        Files.writeString(temp.resolve("snapshot.data"), "nine");
        Files.writeString(temp.resolve(META_FILE), "meta written, directory not renamed yet");

        var reopened = new SnapshotStore(dir.toString());
        assertEquals(5, reopened.getMeta().getLastIncludedIndex());
        assertEquals("five", currentData(reopened));
    }

    @Test
    void crashAfterRenameButBeforeOldSnapshotWasDeletedUsesTheNewOne() throws IOException {
        var store = new SnapshotStore(dir.toString());
        commit(store, 5, "five");
        // snapshot 9 đã hoàn chỉnh và đã được rename vào chỗ, nhưng snapshot 5 chưa kịp xoá
        var elsewhere = new SnapshotStore(dir.resolve("elsewhere").toString());
        commit(elsewhere, 9, "nine");
        Files.move(dir.resolve("elsewhere").resolve("snapshot_9"), dir.resolve("snapshot_9"));

        var reopened = new SnapshotStore(dir.toString());
        assertEquals(9, reopened.getMeta().getLastIncludedIndex());
        assertEquals("nine", currentData(reopened));
        assertFalse(Files.exists(dir.resolve("snapshot_5")));
    }

    @Test
    void corruptedSnapshotIsReportedInsteadOfUsed() throws IOException {
        var store = new SnapshotStore(dir.toString());
        commit(store, 5, "five");
        var data = dir.resolve("snapshot_5").resolve("snapshot.data");

        // một bit trong file dữ liệu bị hỏng
        Files.writeString(data, "fivf");
        assertThrows(IOException.class, () -> new SnapshotStore(dir.toString()));

        // file meta bị hỏng
        Files.writeString(data, "five");
        assertEquals(5, new SnapshotStore(dir.toString()).getMeta().getLastIncludedIndex());
        var metaFile = dir.resolve("snapshot_5").resolve(META_FILE);
        var bytes = Files.readAllBytes(metaFile);
        bytes[bytes.length / 2] ^= 0x01;
        Files.write(metaFile, bytes);
        assertThrows(IOException.class, () -> new SnapshotStore(dir.toString()));

        // file dữ liệu biến mất
        bytes[bytes.length / 2] ^= 0x01;
        Files.write(metaFile, bytes);
        Files.delete(data);
        assertThrows(IOException.class, () -> new SnapshotStore(dir.toString()));
    }

    @Test
    void failedCommitKeepsThePreviousSnapshot() throws IOException {
        var failNext = new boolean[1];
        var store = new SnapshotStore(dir.toString(), operation -> {
            if (failNext[0]) {
                failNext[0] = false;
                throw new IOException("injected failure of " + operation);
            }
        });
        commit(store, 5, "five");

        failNext[0] = true;
        assertThrows(IOException.class, () -> commit(store, 9, "nine"));
        assertEquals(5, store.getMeta().getLastIncludedIndex());
        assertEquals("five", currentData(store));

        // lần sau ghi lại được bình thường
        commit(store, 9, "nine");
        assertTrue(Files.exists(dir.resolve("snapshot_9")));
        assertEquals(9, new SnapshotStore(dir.toString()).getMeta().getLastIncludedIndex());
    }
}

package com.namnv.raft;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Tạo, khôi phục và truyền snapshot. */
class SnapshotTest extends ClusterTestSupport {

    @Test
    void restartRestoresSnapshotAndLog() throws Exception {
        startCluster("A", "B", "C");
        write(awaitLeader(), "1", "2", "3");
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("1", "2", "3"));
        }
        nodes.values().forEach(this::snapshot);
        write(awaitLeader(), "4", "5");
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("1", "2", "3", "4", "5"));
        }

        for (String id : List.of("A", "B", "C")) {
            stopNode(id);
        }
        for (String id : List.of("A", "B", "C")) {
            startNode(id, List.of("A", "B", "C"));
        }

        write(awaitLeader(), "6");
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("1", "2", "3", "4", "5", "6"));
        }
    }

    @Test
    void laggingFollowerCatchesUpFromSnapshot() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = nodes.keySet().stream().filter(id -> !id.equals(leader.getNodeId())).findFirst().orElseThrow();

        isolate(lagging);
        write(leader, "1", "2", "3");
        snapshot(leader);
        write(leader, "4");

        connect(lagging);
        awaitStore(lagging, List.of("1", "2", "3", "4"));
        assertTrue(nodes.get(lagging).getPersistent().getLogStore().getBaseIndex() > 0);

        // replication tiếp tục bình thường sau snapshot
        write(awaitLeader(), "5");
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("1", "2", "3", "4", "5"));
        }
    }

    @Test
    void crashBetweenSnapshotAndLogTruncationDoesNotReapply() throws Exception {
        startCluster("A");
        write(awaitLeader(), "1", "2", "3");

        var folder = dataDir.resolve("A");
        var logFile = folder.resolve("log_1.rec");
        var logBeforeSnapshot = folder.resolve("log.backup");
        Files.copy(logFile, logBeforeSnapshot);

        snapshot(nodes.get("A"));
        stopNode("A");

        // giả lập crash ngay sau khi snapshot được commit: log chưa truncate, còn sót một snapshot ghi dở
        Files.copy(logBeforeSnapshot, logFile, StandardCopyOption.REPLACE_EXISTING);
        Files.createDirectories(folder.resolve("temp"));
        Files.writeString(folder.resolve("temp").resolve("snapshot.data"), "garbage");

        startNode("A", List.of("A"));
        write(awaitLeader(), "4");
        awaitStore("A", List.of("1", "2", "3", "4"));
        assertFalse(Files.exists(folder.resolve("temp")));
    }

    @Test
    void rejectedSnapshotLoadStopsNodeAndRestartRecovers() throws Exception {
        assertFailedLoadStopsNode(machine -> machine.rejectLoad = true);
    }

    @Test
    void brokenSnapshotLoadStopsNodeAndRestartRecovers() throws Exception {
        assertFailedLoadStopsNode(machine -> machine.breakOnLoad = true);
    }

    @Test
    void replicatesTenThousandCommandsAndCatchesUpFromSnapshot() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = others(leader.getNodeId()).get(0);
        stopNode(lagging);

        // gửi dồn dập không chờ từng lệnh: leader phải gom lại và commit kịp trong thời hạn của client
        var expected = new ArrayList<String>();
        var pending = new ArrayList<CompletableFuture<Boolean>>();
        for (int i = 0; i < 10_000; i++) {
            expected.add("k" + i);
            pending.add(leader.appendClientCommand(("k" + i).getBytes(StandardCharsets.UTF_8)));
        }
        for (var future : pending) {
            assertTrue(future.get(30, TimeUnit.SECONDS));
        }
        snapshot(leader);

        // node tụt lại 10.000 lệnh nhận snapshot rồi bắt kịp phần còn lại
        expected.add("last");
        write(leader, "last");
        startNode(lagging, List.of("A", "B", "C"));
        for (String id : List.of("A", "B", "C")) {
            await("store of " + id + " to hold " + expected.size() + " commands",
                    () -> machines.get(id).getStore().equals(expected));
        }
        assertTrue(nodes.get(lagging).getPersistent().getLogStore().getBaseIndex() >= 10_000);
    }

    @Test
    void snapshotIsTakenAutomaticallyAndBatchesAreBounded() throws Exception {
        snapshotIntervalEntries = 50;
        maxEntriesPerRequest = 7;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = others(leader.getNodeId()).get(0);
        stopNode(lagging);

        var expected = new ArrayList<String>();
        for (int i = 0; i < 230; i++) {
            expected.add("k" + i);
            write(leader, "k" + i);
        }
        // log được compact mà không ai gọi createSnapshot()
        await("automatic snapshots", () -> leader.metrics().snapshotsCreated() >= 4);
        assertTrue(leader.metrics().firstLogIndex() > 150);

        // node tụt lại bắt kịp qua snapshot rồi các request nhỏ
        startNode(lagging, List.of("A", "B", "C"));
        awaitStore(lagging, expected);
        var metrics = leader.metrics();
        assertEquals(NodeState.LEADER, metrics.state());
        assertEquals(metrics.commitIndex(), metrics.lastApplied());
        assertEquals(230, metrics.commandsAccepted());
    }

    @Test
    void snapshotIsTransferredInSmallChunks() throws Exception {
        snapshotChunkBytes = 7;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = others(leader.getNodeId()).get(0);
        stopNode(lagging);

        var expected = new ArrayList<String>();
        for (int i = 0; i < 40; i++) {
            expected.add("value-" + i);
            assertTrue(send(leader, "client-1", i + 1, "value-" + i).get(5, TimeUnit.SECONDS));
        }
        snapshot(leader);
        var snapshotFile = dataDir.resolve(leader.getNodeId())
                .resolve("snapshot_" + leader.getPersistent().getLastSnapshotIndex()).resolve("snapshot.data");
        assertTrue(Files.size(snapshotFile) > 20L * snapshotChunkBytes, "snapshot must not fit in a few chunks");

        // node tụt lại nhận file qua hàng chục mẩu 7 byte và ghép lại đúng từng byte
        startNode(lagging, List.of("A", "B", "C"));
        awaitStore(lagging, expected);
        var received = dataDir.resolve(lagging)
                .resolve("snapshot_" + nodes.get(lagging).getPersistent().getLastSnapshotIndex()).resolve("snapshot.data");
        assertEquals(Files.readString(snapshotFile), Files.readString(received));
        assertEquals(Map.of("client-1", 40L), sessions(nodes.get(lagging)));
    }

    @Test
    void interruptedSnapshotTransferRestartsCleanly() throws Exception {
        snapshotChunkBytes = 5;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = others(leader.getNodeId()).get(0);
        isolate(lagging);
        var expected = new ArrayList<String>();
        for (int i = 0; i < 60; i++) {
            expected.add("value-" + i);
            write(leader, "value-" + i);
        }
        snapshot(leader);

        // cắt mạng nhiều lần giữa lúc đang truyền: mẩu bị mất, lần truyền phải làm lại mà không ghép sai
        for (int round = 0; round < 6; round++) {
            connect(lagging);
            TimeUnit.MILLISECONDS.sleep(15);
            isolate(lagging);
            TimeUnit.MILLISECONDS.sleep(15);
        }
        connect(lagging);
        awaitStore(lagging, expected);
    }
}

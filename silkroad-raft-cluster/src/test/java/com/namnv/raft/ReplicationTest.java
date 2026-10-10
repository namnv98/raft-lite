package com.namnv.raft;

import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.TimeoutNowRequest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Replication: ghi log của follower, commit theo term, pipelining và dò lại, bắt kịp từ đĩa. */
class ReplicationTest extends ClusterTestSupport {

    @Test
    void staleAppendEntriesDoesNotTruncateNewerEntries() {
        // B không nối được tới ai nên luôn là follower, test gọi thẳng handler như một leader "L"
        var follower = startNode("B", List.of("L", "B"));

        var full = new AppendEntriesRequest(1, "L", 0, 0, List.of(entry(1, 1), entry(2, 1), entry(3, 1)), 0);
        var stale = new AppendEntriesRequest(1, "L", 0, 0, List.of(entry(1, 1)), 0);

        assertEquals(3, follower.handleAppendEntriesRequest(full).matchIndex);
        var response = follower.handleAppendEntriesRequest(stale);

        assertTrue(response.success);
        assertEquals(1, response.matchIndex);
        assertEquals(3, follower.getPersistent().getLogStore().lastIndex());
    }

    @Test
    void conflictingEntriesAreReplaced() {
        var follower = startNode("B", List.of("L", "B"));
        follower.handleAppendEntriesRequest(
                new AppendEntriesRequest(1, "L", 0, 0, List.of(entry(1, 1), entry(2, 1), entry(3, 1)), 0));

        var response = follower.handleAppendEntriesRequest(
                new AppendEntriesRequest(2, "L", 1, 1, List.of(entry(2, 2)), 0));

        assertTrue(response.success);
        var log = follower.getPersistent().getLogStore();
        assertEquals(2, log.lastIndex());
        assertEquals(2, log.get(2).getTerm());

        // prevLogTerm sai thì phải từ chối
        assertFalse(follower.handleAppendEntriesRequest(
                new AppendEntriesRequest(2, "L", 2, 1, List.of(entry(3, 2)), 0)).success);
    }

    @Test
    void oldTermEntryIsNotCommittedByCountingReplicas() throws Exception {
        // B và C là peer giả: bầu cho A nhưng không nhận log, để test tự đặt matchIndex
        for (String id : List.of("A", "B", "C")) {
            connect(id);
        }
        rpc.register("B", new StubPeer());
        rpc.register("C", new StubPeer());
        var node = startNode("A", List.of("A", "B", "C"));

        // A nhận một entry của term 1 (chưa commit), rồi thành leader của term 2
        node.handleAppendEntriesRequest(new AppendEntriesRequest(1, "B", 0, 0, List.of(entry(1, 1)), 0));
        assertTrue(node.handleTimeoutNowRequest(new TimeoutNowRequest(1, "B")).success);
        await("A to become leader", () -> node.getState() == NodeState.LEADER);
        assertEquals(2, node.getPersistent().getCurrentTerm());

        // như thể B đã có entry của term 1 nhưng chưa có entry nào của term 2: đủ quorum cho index 1
        setMatchIndex(node, "B", 1);
        node.appendClientCommand("y".getBytes(StandardCharsets.UTF_8)); // buộc leader tính lại commit index
        var log = node.getPersistent().getLogStore();
        await("leader log to be on disk", () -> log.durableIndex() == log.lastIndex());
        TimeUnit.MILLISECONDS.sleep(300);
        assertEquals(0, node.getCommitIndex());
        assertEquals(List.of(), machines.get("A").getStore());

        // khi một entry của term 2 đạt quorum thì entry cũ được commit theo
        setMatchIndex(node, "B", log.lastIndex());
        node.appendClientCommand("z".getBytes(StandardCharsets.UTF_8));
        awaitStore("A", List.of("cmd1", "y"));
    }

    @Test
    void leaderKeepsSendingWithoutWaitingForResponsesUpToTheWindow() throws Exception {
        maxInflightAppends = 4;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        int inFlight = appendsInFlightWhileResponsesAreHeld(leader, 12);
        // hai follower, mỗi follower đầy cửa sổ; không bao giờ quá cửa sổ
        assertEquals(2 * 4, inFlight);
        var expected = new ArrayList<String>();
        for (int i = 0; i <= 12; i++) {
            expected.add(String.valueOf(i));
        }
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, expected);
        }
    }

    @Test
    void windowOfOneSendsOneRequestAtATime() throws Exception {
        maxInflightAppends = 1;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        assertEquals(2, appendsInFlightWhileResponsesAreHeld(leader, 12));
    }

    @Test
    void lostRequestInThePipelineMakesTheLeaderProbeAgain() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var follower = others(leader.getNodeId()).get(0);
        write(leader, "0");

        // vài request giữa chuỗi bị mất: các request sau đó không khớp với log của follower
        rpc.dropAppendsTarget = follower;
        rpc.appendsToDrop.set(3);
        var expected = new ArrayList<String>(List.of("0"));
        var pending = new ArrayList<CompletableFuture<Boolean>>();
        for (int i = 1; i <= 40; i++) {
            expected.add(String.valueOf(i));
            pending.add(leader.appendClientCommand(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
            TimeUnit.MILLISECONDS.sleep(2);
        }
        for (var write : pending) {
            assertTrue(write.get(10, TimeUnit.SECONDS));
        }
        assertEquals(3, rpc.appendsDropped.get());
        // follower vẫn nhận đủ, đúng thứ tự, không lặp
        awaitStore(follower, expected);
        await("leader to know the follower has everything",
                () -> matchIndex(leader, follower) == leader.getPersistent().getLogStore().lastIndex());
    }

    @Test
    void requestThatVanishesWithoutAnErrorDoesNotStallTheFollowerForever() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var follower = others(leader.getNodeId()).get(0);
        write(leader, "0");
        awaitStore(follower, List.of("0"));

        // một request bị mất (có báo lỗi) đưa follower về chế độ dò, rồi chính request dò bị nuốt: cửa sổ dò (1 request)
        // đầy mà không bao giờ có câu trả lời hay lỗi nào
        rpc.dropAppendsTarget = follower;
        rpc.appendsToSwallow.set(1);
        rpc.appendsToDrop.set(1);
        var expected = new ArrayList<String>(List.of("0"));
        for (int i = 1; i <= 20; i++) {
            expected.add(String.valueOf(i));
            // follower kia vẫn đủ quorum: mọi lệnh được commit
            write(leader, String.valueOf(i));
        }
        assertEquals(1, rpc.appendsDropped.get());
        assertEquals(0, rpc.appendsToSwallow.get());
        // leader nhận ra request đang bay đã mất và dò lại: follower vẫn nhận đủ
        awaitStore(follower, expected);
    }

    @Test
    void followerBehindTheLogCacheCatchesUpFromDisk() throws Exception {
        logCacheEntries = 8;
        maxEntriesPerRequest = 5;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = others(leader.getNodeId()).get(0);

        isolate(lagging);
        var expected = new ArrayList<String>();
        for (int i = 0; i < 60; i++) {
            expected.add("w" + i);
        }
        write(leader, expected.toArray(String[]::new));
        // phần follower cần đã rời khỏi bộ nhớ của leader và không có snapshot nào thay cho nó
        var logStore = leader.getPersistent().getLogStore();
        assertEquals(0, logStore.getBaseIndex());
        assertFalse(logStore.isCached(1, maxEntriesPerRequest));

        connect(lagging);
        awaitStore(lagging, expected);
        write(awaitLeader(), "last");
        expected.add("last");
        for (String node : List.of("A", "B", "C")) {
            awaitStore(node, expected);
        }
    }

    @Test
    void logReplicatesRestartsAndCatchesUpAcrossSegments() throws Exception {
        logCacheEntries = 8;
        maxEntriesPerRequest = 5;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = others(leader.getNodeId()).get(0);

        // follower tụt xa hơn bộ nhớ của leader: phần nó cần được đọc lại từ các file segment
        isolate(lagging);
        var expected = new ArrayList<String>();
        for (int i = 0; i < 120; i++) {
            expected.add("w" + i);
        }
        write(leader, expected.toArray(String[]::new));
        connect(lagging);
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, expected);
        }
        try (var files = Files.list(dataDir.resolve(leader.getNodeId()))) {
            var names = files.map(p -> p.getFileName().toString()).filter(name -> name.startsWith("log_")).toList();
            assertTrue(names.size() > 1 && names.stream().allMatch(name -> name.endsWith(".rec")), "segments: " + names);
        }

        // khởi động lại từng node: log đọc lại từ file, rồi snapshot compact nó
        for (String id : List.of("A", "B", "C")) {
            stopNode(id);
            startNode(id, List.of("A", "B", "C"));
            awaitStore(id, expected);
        }
        snapshot(awaitLeader());
        write(awaitLeader(), "after");
        expected.add("after");
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, expected);
        }

        // thư mục còn log dạng JSON của phiên bản cũ phải bị từ chối thay vì được coi là log rỗng
        stopNode("A");
        Files.writeString(dataDir.resolve("A").resolve("log_1.jsonl"), "");
        assertThrows(IOException.class, () -> startNode("A", List.of("A", "B", "C")));
        nodes.remove("A");
    }
}

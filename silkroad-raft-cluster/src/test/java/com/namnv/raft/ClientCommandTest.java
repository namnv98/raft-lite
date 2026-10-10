package com.namnv.raft;

import com.namnv.entity.LogEntry;
import com.namnv.raft.example.ListStateMachine;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Lệnh của client: chống ghi trùng, ghi theo lô, kết quả của lệnh, giới hạn hàng chờ. */
class ClientCommandTest extends ClusterTestSupport {

    @Test
    void retriedCommandIsAppliedOnce() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();

        // gửi lại sau khi đã có kết quả, và gửi hai lần liền nhau khi lần đầu còn chưa commit
        assertTrue(send(leader, "client-1", 1, "x").get(5, TimeUnit.SECONDS));
        assertTrue(send(leader, "client-1", 1, "x").get(5, TimeUnit.SECONDS));
        var first = send(leader, "client-1", 2, "y");
        var second = send(leader, "client-1", 2, "y");
        assertTrue(first.get(5, TimeUnit.SECONDS));
        assertTrue(second.get(5, TimeUnit.SECONDS));

        // client khác dùng lại cùng sequence thì không liên quan
        assertTrue(send(leader, "client-2", 1, "z").get(5, TimeUnit.SECONDS));
        // lần gửi lại rất muộn của một lệnh cũ cũng bị bỏ qua
        assertTrue(send(leader, "client-1", 1, "x").get(5, TimeUnit.SECONDS));
        write(leader, "end");

        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("x", "y", "z", "end"));
        }
    }

    @Test
    void retryAfterLeaderChangeIsAppliedOnce() throws Exception {
        startCluster("A", "B", "C");
        var oldLeader = awaitLeader();
        assertTrue(send(oldLeader, "client-1", 1, "x").get(5, TimeUnit.SECONDS));

        // client không nhận được kết quả và gửi lại cho leader mới
        isolate(oldLeader.getNodeId());
        var newLeader = awaitLeader(oldLeader.getNodeId());
        assertTrue(send(newLeader, "client-1", 1, "x").get(5, TimeUnit.SECONDS));
        assertTrue(send(newLeader, "client-1", 2, "y").get(5, TimeUnit.SECONDS));
        connect(oldLeader.getNodeId());

        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("x", "y"));
        }
    }

    @Test
    void deduplicationSurvivesSnapshotRestartAndInstallSnapshot() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var lagging = others(leader.getNodeId()).get(0);
        isolate(lagging);
        assertTrue(send(leader, "client-1", 1, "x").get(5, TimeUnit.SECONDS));
        assertTrue(send(leader, "client-1", 2, "y").get(5, TimeUnit.SECONDS));
        snapshot(leader);
        write(leader, "after");

        // node tụt lại nhận bảng sessions qua InstallSnapshot
        connect(lagging);
        awaitStore(lagging, List.of("x", "y", "after"));
        assertEquals(Map.of("client-1", 2L), sessions(nodes.get(lagging)));

        // sau khi cả cluster restart, bảng được dựng lại từ snapshot nên lệnh gửi lại vẫn bị nhận ra
        for (String id : List.of("A", "B", "C")) {
            stopNode(id);
        }
        for (String id : List.of("A", "B", "C")) {
            startNode(id, List.of("A", "B", "C"));
        }
        var newLeader = awaitLeader();
        assertTrue(send(newLeader, "client-1", 2, "y").get(5, TimeUnit.SECONDS));
        assertTrue(send(newLeader, "client-1", 3, "w").get(5, TimeUnit.SECONDS));
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("x", "y", "after", "w"));
        }
    }

    @Test
    void batchIsAppliedInOrderAndOnlyOnceAsOneEntry() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "0");
        long before = leader.getPersistent().getLogStore().lastIndex();

        assertTrue(leader.appendClientBatch("batcher", 1, commands("a", "b", "c")).get(5, TimeUnit.SECONDS));
        // cả lô là một entry
        assertEquals(before + 1, leader.getPersistent().getLogStore().lastIndex());
        assertTrue(leader.getPersistent().getLogStore().get(before + 1).isBatch());
        // gửi lại đúng lô đó (client không biết kết quả lần trước): không apply lần hai
        assertTrue(leader.appendClientBatch("batcher", 1, commands("a", "b", "c")).get(5, TimeUnit.SECONDS));
        assertTrue(leader.appendClientBatch("batcher", 2, commands("d")).get(5, TimeUnit.SECONDS));
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("0", "a", "b", "c", "d"));
        }
    }

    @Test
    void malformedBatchIsRejectedBeforeReachingTheLog() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        long before = leader.getPersistent().getLogStore().lastIndex();
        assertFalse(leader.appendClientCommand("x", 1, new byte[]{0, 0, 0, 2, 0, 0, 0, 9}, true).get(5, TimeUnit.SECONDS));
        assertFalse(leader.appendClientCommand("x", 1, new byte[]{1}, true).get(5, TimeUnit.SECONDS));
        assertEquals(before, leader.getPersistent().getLogStore().lastIndex());
    }

    @Test
    void batchSurvivesRestartAndSnapshot() throws Exception {
        snapshotIntervalEntries = 5;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        for (int i = 0; i < 8; i++) {
            assertTrue(leader.appendClientBatch("b", i + 1, commands(i + "x", i + "y")).get(5, TimeUnit.SECONDS));
        }
        var expected = new ArrayList<String>();
        for (int i = 0; i < 8; i++) {
            expected.add(i + "x");
            expected.add(i + "y");
        }
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, expected);
        }
        // khởi động lại một follower: lô được apply lại từ snapshot và log trên đĩa, vẫn đúng thứ tự và một lần
        var follower = others(leader.getNodeId()).get(0);
        stopNode(follower);
        startNode(follower, List.of("A", "B", "C"));
        awaitStore(follower, expected);
    }

    // ---------- kết quả của lệnh ----------

    @Test
    void submitReturnsWhatTheStateMachineAnswered() throws Exception {
        // state machine trả về số lệnh nó đã có sau khi apply lệnh này
        machineFactory = () -> new ListStateMachine() {
            @Override
            public byte[] onApplyWithResult(String node, LogEntry entry) {
                onApply(node, entry);
                return ("size=" + getStore().size()).getBytes(StandardCharsets.UTF_8);
            }
        };
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        assertEquals("size=1", new String(leader.submit(null, 0, "a".getBytes(StandardCharsets.UTF_8), false)
                .get(5, TimeUnit.SECONDS), StandardCharsets.UTF_8));

        // lô: kết quả của từng lệnh, đúng thứ tự
        var results = new ArrayList<String>();
        com.namnv.entity.CommandBatch.forEach(leader.submit(null, 0,
                        com.namnv.entity.CommandBatch.encode(commands("b", "c")), true).get(5, TimeUnit.SECONDS),
                r -> results.add(new String(r, StandardCharsets.UTF_8)));
        assertEquals(List.of("size=2", "size=3"), results);

        // follower không nhận lệnh: không rõ kết quả, không phải lỗi lập trình
        var follower = nodes.get(others(leader.getNodeId()).get(0));
        var error = assertThrows(java.util.concurrent.ExecutionException.class,
                () -> follower.submit(null, 0, "x".getBytes(StandardCharsets.UTF_8), false).get(5, TimeUnit.SECONDS));
        assertTrue(error.getCause() instanceof UnknownOutcomeException);
    }

    @Test
    void nodeStopsInsteadOfDivergingWhenTheStateMachineFails() throws Exception {
        machineFactory = () -> new ListStateMachine() {
            @Override
            public void onApply(String node, LogEntry entry) {
                if ("boom".equals(new String(entry.getCommand(), StandardCharsets.UTF_8))) {
                    throw new IllegalStateException("state machine failure");
                }
                super.onApply(node, entry);
            }
        };
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1");
        leader.appendClientCommand("boom".getBytes(StandardCharsets.UTF_8));
        // Node nào apply tới lệnh đó thì tự tắt. Leader tắt có thể trước khi follower biết lệnh đã commit; leader mới
        // commit lại nó rồi cũng tắt, còn node cuối cùng không còn đa số nên không bao giờ apply tới lệnh đó.
        await("leader to stop", leader::isStopped);
        await("a second node to stop",
                () -> nodes.values().stream().filter(RaftNode::isStopped).count() >= 2);
        // không bản sao nào apply dở hay chạy tiếp với state khác các bản còn lại
        for (String id : List.of("A", "B", "C")) {
            assertEquals(List.of("1"), machines.get(id).getStore());
        }
    }

    @Test
    void concurrentCommandsOfOneClientAreEachAppliedOnce() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();

        // 200 lệnh của cùng một client được gửi dồn dập không chờ nhau, mỗi lệnh gửi hai lần
        var pending = new ArrayList<CompletableFuture<Boolean>>();
        var expected = new ArrayList<String>();
        for (int sequence = 1; sequence <= 200; sequence++) {
            expected.add("cmd" + sequence);
            pending.add(send(leader, "client-1", sequence, "cmd" + sequence));
            pending.add(send(leader, "client-1", sequence, "cmd" + sequence));
        }
        for (var future : pending) {
            assertTrue(future.get(10, TimeUnit.SECONDS));
        }
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, expected);
        }
        // không còn sequence lẻ nào phải nhớ: bảng chỉ giữ một mốc cho mỗi client
        assertEquals(Map.of("client-1", 200L), sessions(leader));
        assertTrue(leader.getSessions().get("client-1").getAbove().isEmpty());
    }

    @Test
    void closedSessionIsForgottenOnEveryNode() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        assertTrue(send(leader, "client-1", 1, "x").get(5, TimeUnit.SECONDS));
        assertTrue(send(leader, "client-2", 1, "y").get(5, TimeUnit.SECONDS));

        assertTrue(leader.closeClientSession("client-1").get(5, TimeUnit.SECONDS));
        write(leader, "end");
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("x", "y", "end"));
            await("session table of " + id, () -> sessions(nodes.get(id)).equals(Map.of("client-2", 1L)));
        }
    }

    @Test
    void leaderRejectsCommandsWhenTooManyArePending() throws Exception {
        maxPendingCommands = 5;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        // không follower nào trả lời nên không lệnh nào commit được
        for (String id : others(leader.getNodeId())) {
            isolate(id);
        }

        var pending = new ArrayList<CompletableFuture<Boolean>>();
        for (int i = 0; i < 5; i++) {
            pending.add(leader.appendClientCommand(("p" + i).getBytes(StandardCharsets.UTF_8)));
        }
        assertFalse(leader.appendClientCommand("over".getBytes(StandardCharsets.UTF_8)).get(1, TimeUnit.SECONDS));
        assertEquals(1, leader.metrics().commandsRejected());
        assertEquals(5, leader.metrics().pendingCommands());
        assertFalse(pending.get(0).isDone());
    }

    @Test
    void outOfOrderSequencesAreDeduplicated() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();

        // lệnh 3 và 2 vào log trước lệnh 1, mỗi lệnh hai lần: bảng phải nhớ cả sequence nằm trên mốc liền mạch
        var pending = new ArrayList<CompletableFuture<Boolean>>();
        for (long sequence : List.of(3L, 2L, 3L, 2L, 1L, 1L)) {
            pending.add(send(leader, "client-1", sequence, "cmd" + sequence));
        }
        for (var future : pending) {
            assertTrue(future.get(5, TimeUnit.SECONDS));
        }
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("cmd3", "cmd2", "cmd1"));
        }
        assertEquals(Map.of("client-1", 3L), sessions(leader));
    }

    @Test
    void resultCallbacksRunOutsideTheNodeLock() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var follower = nodes.get(others(leader.getNodeId()).get(0));

        // callback gắn trực tiếp vào future chạy trên thread hoàn tất nó: thread đó không được còn giữ lock của node
        var written = leader.appendClientCommand("1".getBytes(StandardCharsets.UTF_8))
                .thenApply(ok -> ok && !leader.getLock().isHeldByCurrentThread());
        assertTrue(written.get(5, TimeUnit.SECONDS));
        assertFalse(leader.read(() -> 1).thenApply(v -> leader.getLock().isHeldByCurrentThread()).get(5, TimeUnit.SECONDS));
        assertFalse(follower.read(() -> 1).thenApply(v -> follower.getLock().isHeldByCurrentThread()).get(5, TimeUnit.SECONDS));

        // và nó gọi lại node được mà không bị kẹt
        var chained = leader.appendClientCommand("2".getBytes(StandardCharsets.UTF_8))
                .thenCompose(ok -> leader.appendClientCommand("3".getBytes(StandardCharsets.UTF_8)));
        assertTrue(chained.get(5, TimeUnit.SECONDS));
        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("1", "2", "3"));
        }
    }
}

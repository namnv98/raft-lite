package com.namnv.raft;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Đọc nhất quán theo ReadIndex trên leader và follower. */
class ReadTest extends ClusterTestSupport {

    // ---------- đọc nhất quán ----------

    @Test
    void readSeesEveryAcknowledgedWrite() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var machine = machines.get(leader.getNodeId());

        for (int i = 0; i < 50; i++) {
            write(leader, "w" + i);
            // lệnh vừa được xác nhận phải có trong lần đọc ngay sau đó
            assertEquals(i + 1, leader.read(machine::getStore).get(5, TimeUnit.SECONDS).size());
        }
    }

    @Test
    void singleNodeClusterServesReads() throws Exception {
        startCluster("A");
        var leader = awaitLeader();
        write(leader, "1");
        assertEquals(List.of("1"), leader.read(machines.get("A")::getStore).get(5, TimeUnit.SECONDS));
    }

    @Test
    void followerServesConsistentReads() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var followers = others(leader.getNodeId());

        for (int i = 0; i < 30; i++) {
            write(leader, "w" + i);
            // follower chưa chắc đã apply lệnh vừa commit, nhưng lần đọc nhất quán trên nó vẫn phải thấy
            for (String id : followers) {
                assertEquals(i + 1, nodes.get(id).read(machines.get(id)::getStore).get(5, TimeUnit.SECONDS).size());
            }
        }
    }

    @Test
    void readFailsWithoutAReachableLeader() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1");
        var id = others(leader.getNodeId()).get(0);
        var follower = nodes.get(id);
        await("follower to learn the leader", () -> leader.getNodeId().equals(follower.getLeaderId()));

        // follower bị cô lập không hỏi được leader: thà báo lỗi còn hơn trả dữ liệu có thể đã cũ
        isolate(id);
        var failure = assertThrows(ExecutionException.class,
                () -> follower.read(machines.get(id)::getStore).get(5, TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof NotLeaderException);

        // node chưa từng biết leader nào
        connect("D");
        var loner = startNode("D", List.of());
        failure = assertThrows(ExecutionException.class,
                () -> loner.read(machines.get("D")::getStore).get(5, TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof NotLeaderException);
    }

    @Test
    void isolatedLeaderCannotServeStaleRead() throws Exception {
        startCluster("A", "B", "C");
        var oldLeader = awaitLeader();
        write(oldLeader, "1");

        // leader cũ vẫn tưởng mình là leader trong một lúc, nhưng không còn xác nhận được với đa số
        isolate(oldLeader.getNodeId());
        var staleRead = oldLeader.read(machines.get(oldLeader.getNodeId())::getStore);
        var newLeader = awaitLeader(oldLeader.getNodeId());
        write(newLeader, "2");

        // nếu trả lời, nó sẽ trả về dữ liệu thiếu lệnh "2" đã được xác nhận
        var failure = assertThrows(ExecutionException.class, () -> staleRead.get(10, TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof NotLeaderException);
        assertEquals(List.of("1", "2"),
                newLeader.read(machines.get(newLeader.getNodeId())::getStore).get(5, TimeUnit.SECONDS));
    }

    @Test
    void readIsNotConfirmedByResponsesToEarlierHeartbeats() throws Exception {
        startCluster("A", "B", "C");
        var oldLeader = awaitLeader();
        var id = oldLeader.getNodeId();
        write(oldLeader, "1");

        // giữ lại response của một vòng heartbeat, rồi đóng băng và cô lập leader: nó sẽ không tự step-down
        rpc.holdResponsesTo = id;
        rpc.heldResponses = new CompletableFuture<>();
        await("a heartbeat to each follower to be in flight", () -> rpc.held.get() >= 2);
        runtimes.get(id).timersFrozen = true;
        isolate(id);

        // yêu cầu đọc đến SAU khi các heartbeat đó được gửi đi
        var staleRead = oldLeader.read(machines.get(id)::getStore);
        var newLeader = awaitLeader(id);
        write(newLeader, "2");

        // response cũ về tới nơi: chúng chỉ chứng minh node này là leader trước khi có yêu cầu đọc, không phải sau
        rpc.heldResponses.complete(null);
        TimeUnit.MILLISECONDS.sleep(500);
        assertFalse(staleRead.isDone() && !staleRead.isCompletedExceptionally(),
                "an isolated leader answered a read with stale data");
    }

    @Test
    void concurrentFollowerReadsShareOneReadIndexRequest() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1", "2");
        var id = others(leader.getNodeId()).get(0);
        var follower = nodes.get(id);

        var gate = new CompletableFuture<Void>();
        rpc.heldReadIndex = gate;
        var before = rpc.readIndexCalls.get();
        var reads = new ArrayList<CompletableFuture<List<String>>>();
        for (int i = 0; i < 50; i++) {
            reads.add(follower.read(() -> List.copyOf(machines.get(id).getStore())));
        }
        // lần đọc đầu đã gửi một request; 49 lần còn lại chờ, không gửi thêm request nào
        await("the first ReadIndex request", () -> rpc.readIndexCalls.get() > before);
        TimeUnit.MILLISECONDS.sleep(100);
        assertEquals(before + 1, rpc.readIndexCalls.get());
        assertTrue(reads.stream().noneMatch(CompletableFuture::isDone));

        rpc.heldReadIndex = null;
        gate.complete(null);
        for (var read : reads) {
            assertEquals(List.of("1", "2"), read.get(5, TimeUnit.SECONDS));
        }
        // các lần đọc đến sau khi request đầu được gửi đi chung đúng một request nữa
        assertEquals(before + 2, rpc.readIndexCalls.get());
    }
}

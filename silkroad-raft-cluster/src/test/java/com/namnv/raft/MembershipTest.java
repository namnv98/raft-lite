package com.namnv.raft;

import com.namnv.entity.ConfigurationEntry;
import com.namnv.entity.LogEntry;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Đổi thành viên bằng joint consensus, gỡ node, trao quyền leader, learner. */
class MembershipTest extends ClusterTestSupport {

    @Test
    void newNodeJoinsThroughJointConsensus() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1", "2");
        snapshot(leader);

        connect("D");
        startNode("D", List.of());
        assertTrue(leader.onJoinPeerCluster("D"));

        var members = List.of("A", "B", "C", "D");
        for (String id : members) {
            await("final configuration on " + id, () -> {
                var conf = nodes.get(id).getConf();
                return !conf.isJoint() && conf.getOldNodes().equals(members);
            });
        }
        write(awaitLeader(), "3");
        for (String id : members) {
            awaitStore(id, List.of("1", "2", "3"));
        }

        // cấu hình mới phải còn sau khi restart, không quay về danh sách peers ban đầu
        stopNode("A");
        startNode("A", List.of("A", "B", "C"));
        assertEquals(members, nodes.get("A").getConf().getOldNodes());
    }

    @Test
    void followerCanBeRemoved() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1");
        var removed = nodes.keySet().stream().filter(id -> !id.equals(leader.getNodeId())).findFirst().orElseThrow();
        var remaining = nodes.keySet().stream().filter(id -> !id.equals(removed)).sorted().toList();

        assertTrue(leader.onLeavePeerCluster(removed));
        for (String id : remaining) {
            await("final configuration on " + id, () -> {
                var conf = nodes.get(id).getConf();
                return !conf.isJoint() && conf.getOldNodes().equals(remaining);
            });
        }

        // leader tiếp tục gửi log cho node bị gỡ tới khi nó nhận được cấu hình cuối
        await("removed node to learn the final configuration",
                () -> nodes.get(removed).getConf().getOldNodes().equals(remaining));

        // biết chắc mình đã bị gỡ thì node tự tắt
        await("removed node to shut itself down", () -> nodes.get(removed).isStopped());

        // node bị gỡ không còn được tính vào quorum và không thể giành quyền leader
        stopNode(removed);
        write(awaitLeader(), "2");
        for (String id : remaining) {
            awaitStore(id, List.of("1", "2"));
        }
        assertFalse(leader.onLeavePeerCluster(removed));
    }

    @Test
    void leaderCanRemoveItself() throws Exception {
        startCluster("A", "B", "C");
        var oldLeader = awaitLeader();
        write(oldLeader, "1");
        var remaining = nodes.keySet().stream().filter(id -> !id.equals(oldLeader.getNodeId())).sorted().toList();

        assertTrue(oldLeader.onLeavePeerCluster(oldLeader.getNodeId()));

        var newLeader = awaitLeader(oldLeader.getNodeId());
        assertEquals(remaining, newLeader.getConf().getOldNodes());
        write(newLeader, "2");
        for (String id : remaining) {
            awaitStore(id, List.of("1", "2"));
        }

        // node bị gỡ nhận được cấu hình cuối nên tự biết mình đã rời cluster
        await("old leader to learn the final configuration",
                () -> oldLeader.getConf().getOldNodes().equals(remaining));

        // leader cũ trao quyền xong thì tự tắt, cluster còn lại không bị gián đoạn
        await("old leader to shut itself down", oldLeader::isStopped);
        TimeUnit.MILLISECONDS.sleep(1000);
        assertNotEquals(NodeState.LEADER, oldLeader.getState());
        assertEquals(NodeState.LEADER, newLeader.getState());
    }

    @Test
    void leadershipTransferTriesNextFollowerWhenOneFails() {
        startCluster("A", "B", "C");
        var oldLeader = awaitLeader();
        var remaining = others(oldLeader.getNodeId());
        rpc.timeoutNowBlocked.addAll(remaining);

        assertTrue(oldLeader.onLeavePeerCluster(oldLeader.getNodeId()));

        // follower đầu không nhận được thì phải hỏi tiếp follower sau, mỗi node đúng một lần
        await("both followers to be asked", () -> rpc.timeoutNowCalls.size() == 2);
        assertEquals(Set.copyOf(remaining), Set.copyOf(rpc.timeoutNowCalls));

        // không ai nhận lời thì cluster vẫn tự bầu theo election timeout, và leader cũ vẫn tự tắt
        awaitLeader(oldLeader.getNodeId());
        await("old leader to shut itself down", oldLeader::isStopped);
        assertEquals(2, rpc.timeoutNowCalls.size());
    }

    @Test
    void newLeaderFinishesRemovalStartedByOldLeader() {
        startCluster("A", "B", "C", "D");
        var oldLeader = awaitLeader();
        var removed = others(oldLeader.getNodeId()).get(0);
        var remaining = others(removed);

        // node bị gỡ không nhận được gì từ leader cũ
        isolate(removed);
        assertTrue(oldLeader.onLeavePeerCluster(removed));
        awaitFinalConf(remaining, remaining);
        assertTrue(nodes.get(removed).getConf().contains(removed));

        stopNode(oldLeader.getNodeId());
        var newLeader = awaitLeader();
        assertEquals(Set.of(removed), departing(newLeader));

        connect(removed);
        await("removed node to shut itself down", () -> nodes.get(removed).isStopped());
        await("new leader to stop tracking the removed node", () -> departing(newLeader).isEmpty());
    }

    @Test
    void leaderGivesUpOnUnreachableRemovedNode() {
        departingTimeoutMs = 1500;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var removed = others(leader.getNodeId()).get(0);
        var remaining = others(removed);

        isolate(removed);
        assertTrue(leader.onLeavePeerCluster(removed));
        awaitFinalConf(remaining, remaining);
        assertEquals(Set.of(removed), departing(leader));

        await("leader to give up on the removed node", () -> departing(leader).isEmpty());
        assertEquals(NodeState.LEADER, leader.getState());
        assertFalse(nodes.get(removed).isStopped());
    }

    @Test
    void removedNodeStaysUpWhenShutdownOnRemovedIsOff() throws Exception {
        shutdownOnRemoved = false;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var removed = others(leader.getNodeId()).get(0);
        var remaining = others(removed);

        assertTrue(leader.onLeavePeerCluster(removed));
        awaitFinalConf(remaining, List.of("A", "B", "C"));
        await("leader to finish with the removed node", () -> departing(leader).isEmpty());

        TimeUnit.MILLISECONDS.sleep(700);
        assertFalse(nodes.get(removed).isStopped());
        assertEquals(NodeState.FOLLOWER, nodes.get(removed).getState());
        assertEquals(NodeState.LEADER, leader.getState());
    }

    @Test
    void configurationRollsBackWhenItsEntryIsTruncated() {
        var follower = startNode("B", List.of("L", "B", "C"));
        var joint = new ConfigurationEntry(List.of("L", "B", "C"), List.of("L", "B", "C", "D"), true);

        // cấu hình có hiệu lực ngay khi entry vào log, dù chưa commit
        follower.handleAppendEntriesRequest(new AppendEntriesRequest(1, "L", 0, 0,
                List.of(entry(1, 1), LogEntry.newConfigurationEntry(2, 1, joint)), 0));
        assertTrue(follower.getConf().isJoint());
        assertTrue(follower.getConf().contains("D"));

        // leader mới ghi đè entry đó: cấu hình phải quay về bản trước
        follower.handleAppendEntriesRequest(new AppendEntriesRequest(2, "C", 1, 1, List.of(entry(2, 2)), 0));
        assertFalse(follower.getConf().isJoint());
        assertEquals(List.of("L", "B", "C"), follower.getConf().getOldNodes());
        assertFalse(follower.getConf().contains("D"));
    }

    @Test
    void secondConfigurationChangeIsRejectedWhileOneIsInProgress() {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        var follower = others(leader.getNodeId()).get(0);

        // giữ lock để thay đổi thứ nhất chưa thể commit trước khi thử thay đổi thứ hai
        leader.getLock().lock();
        try {
            assertTrue(leader.onJoinPeerCluster("D"));
            assertFalse(leader.onLeavePeerCluster(follower));
            assertFalse(leader.onJoinPeerCluster("E"));
        } finally {
            leader.getLock().unlock();
        }
    }

    // ---------- learner ----------

    @Test
    void learnerFollowsTheLogButNeverCountsTowardsQuorumOrLeads() throws Exception {
        learners = List.of("L");
        for (String id : List.of("A", "B", "C", "L")) {
            connect(id);
        }
        for (String id : List.of("A", "B", "C")) {
            startNode(id, List.of("A", "B", "C"));
        }
        var learner = startNode("L", List.of("A", "B", "C"));
        var leader = awaitLeader();
        assertTrue(!leader.getNodeId().equals("L"));
        write(leader, "1", "2");
        awaitStore("L", List.of("1", "2"));
        // đọc nhất quán trên learner: hỏi leader readIndex như một follower
        assertEquals(List.of("1", "2"), learner.read(machines.get("L")::getStore).get(5, TimeUnit.SECONDS));

        // còn leader và learner thì không đủ đa số: lệnh không được commit dù learner vẫn nhận log
        for (String id : others(leader.getNodeId(), "L")) {
            isolate(id);
        }
        var pending = leader.appendClientCommand("3".getBytes(StandardCharsets.UTF_8));
        TimeUnit.MILLISECONDS.sleep(400);
        assertFalse(pending.isDone() && pending.getNow(false), "a learner must not complete a quorum");
        assertEquals(List.of("1", "2"), machines.get("L").getStore());

        // leader cũng bị cô lập: chỉ còn learner, nó không bao giờ tự ứng cử
        isolate(leader.getNodeId());
        TimeUnit.MILLISECONDS.sleep(1200);
        assertEquals(0, learner.metrics().electionsStarted());
        assertTrue(learner.getState() != NodeState.LEADER);
    }

    @Test
    void nodeThatNeverCatchesUpIsNotAddedAndDoesNotBlockTheCluster() throws Exception {
        catchUpTimeoutMs = 1000;
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1");

        // D chưa hề chạy: nó chỉ là learner, cấu hình và quorum không đổi
        assertTrue(leader.onJoinPeerCluster("D"));
        assertFalse(leader.onJoinPeerCluster("E"));
        write(leader, "2");
        assertFalse(leader.getConf().isJoint());
        assertEquals(List.of("A", "B", "C"), leader.getConf().getOldNodes());

        // hết hạn thì yêu cầu bị huỷ và leader nhận thay đổi khác
        connect("E");
        startNode("E", List.of());
        await("the pending change to be abandoned", () -> leader.onJoinPeerCluster("E"));
        awaitFinalConf(List.of("A", "B", "C", "E"), List.of("A", "B", "C", "E"));
        awaitStore("E", List.of("1", "2"));
    }

    @Test
    void severalNodesCanBeReplacedInOneChange() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1", "2");
        var kept = leader.getNodeId();
        var removed = others(kept);
        for (String id : List.of("D", "E")) {
            connect(id);
            startNode(id, List.of());
        }

        var target = List.of(kept, "D", "E");
        assertTrue(leader.changePeers(target));
        awaitFinalConf(target, target);
        for (String id : removed) {
            await("removed node " + id + " to shut itself down", () -> nodes.get(id).isStopped());
        }

        write(awaitLeader(), "3");
        for (String id : target) {
            awaitStore(id, List.of("1", "2", "3"));
        }
        assertFalse(leader.changePeers(target));
        assertFalse(leader.changePeers(List.of()));
    }
}

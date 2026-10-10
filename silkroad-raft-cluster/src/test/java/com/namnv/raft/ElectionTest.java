package com.namnv.raft;

import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.TimeoutNowRequest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Bầu leader: pre-vote, check quorum, TimeoutNow, leader không ghi được no-op. */
class ElectionTest extends ClusterTestSupport {

    // ---------- tests ----------

    @Test
    void electsOneLeaderAndReplicates() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();

        write(leader, "1", "2", "3", "4", "5");

        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("1", "2", "3", "4", "5"));
        }
        assertEquals(1, nodes.values().stream().filter(n -> n.getState() == NodeState.LEADER).count());
    }

    @Test
    void singleNodeClusterElectsItselfAndCommits() throws Exception {
        startCluster("A");
        write(awaitLeader(), "1");
        awaitStore("A", List.of("1"));
    }

    @Test
    void isolatedLeaderStepsDownAndRejoins() throws Exception {
        startCluster("A", "B", "C");
        var oldLeader = awaitLeader();
        write(oldLeader, "1");

        isolate(oldLeader.getNodeId());
        var newLeader = awaitLeader(oldLeader.getNodeId());
        assertNotEquals(oldLeader.getNodeId(), newLeader.getNodeId());
        await("old leader to step down", () -> oldLeader.getState() != NodeState.LEADER);

        write(newLeader, "2");
        connect(oldLeader.getNodeId());

        for (String id : List.of("A", "B", "C")) {
            awaitStore(id, List.of("1", "2"));
        }
    }

    @Test
    void twoRemainingNodesKeepElectingLeaders() throws Exception {
        // sau mỗi lần leader chết, hai node còn lại phải bầu được leader mới (kể cả khi split vote)
        startCluster("A", "B", "C");
        var expected = new ArrayList<String>();
        for (int round = 0; round < 5; round++) {
            var leader = awaitLeader();
            var id = leader.getNodeId();
            stopNode(id);

            var next = awaitLeader();
            expected.add("r" + round);
            write(next, "r" + round);

            startNode(id, List.of("A", "B", "C"));
            awaitStore(id, expected);
        }
    }

    @Test
    void preVoteIsRejectedWhileLeaderIsAlive() {
        var follower = startNode("B", List.of("L", "B", "C"));
        follower.handleAppendEntriesRequest(new AppendEntriesRequest(1, "L", 0, 0, List.of(), 0));

        var preVote = new PreVoteRequest(1, "C", 0, 0);
        assertFalse(follower.handlePreVoteRequest(preVote).voteGranted);

        // hết thời gian tối thiểu mà không nghe leader thì mới đồng ý
        await("pre-vote to be granted", () -> follower.handlePreVoteRequest(preVote).voteGranted);
    }

    @Test
    void timeoutNowStartsElectionImmediately() {
        var follower = startNode("B", List.of("L", "B", "C"));
        follower.handleAppendEntriesRequest(new AppendEntriesRequest(1, "L", 0, 0, List.of(), 0));

        // yêu cầu của term cũ bị từ chối
        assertFalse(follower.handleTimeoutNowRequest(new TimeoutNowRequest(0, "L")).success);

        assertTrue(follower.handleTimeoutNowRequest(new TimeoutNowRequest(1, "L")).success);
        assertEquals(NodeState.CANDIDATE, follower.getState());
        assertEquals(2, follower.getPersistent().getCurrentTerm());
    }

    @Test
    void leaderStepsDownWhenItCannotWriteItsNoOp() {
        var failNextAppend = new boolean[1];
        diskFaults = operation -> {
            if (failNextAppend[0] && operation.equals("log.append")) {
                failNextAppend[0] = false;
                throw new IOException("injected failure of " + operation);
            }
        };
        for (String id : List.of("A", "B", "C")) {
            connect(id);
        }
        rpc.register("B", new StubPeer());
        rpc.register("C", new StubPeer());
        var node = startNode("A", List.of("A", "B", "C"));
        node.handleAppendEntriesRequest(new AppendEntriesRequest(1, "B", 0, 0, List.of(), 0));

        // thắng cử ở term 2 nhưng không ghi được no-op: không được kẹt ở trạng thái leader mà không gửi heartbeat
        failNextAppend[0] = true;
        assertTrue(node.handleTimeoutNowRequest(new TimeoutNowRequest(1, "B")).success);
        await("the failed leader to step down", () -> !failNextAppend[0] && node.getState() == NodeState.FOLLOWER);
        assertEquals(0, node.getPersistent().getLogStore().lastIndex());

        // đĩa hoạt động lại thì bầu lại và làm leader bình thường
        await("node to be elected again", () -> node.getState() == NodeState.LEADER);
        assertTrue(node.getPersistent().getCurrentTerm() > 2);
        assertEquals(1, node.getPersistent().getLogStore().lastIndex());
    }
}

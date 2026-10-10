package com.namnv.raft;

import com.fasterxml.jackson.databind.JsonNode;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.RequestVoteRequest;
import com.namnv.rpc.model.request.TimeoutNowRequest;
import com.namnv.storage.Checksum;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Mọi thứ node hứa với node khác phải nằm trên đĩa trước khi lời hứa được gửi đi; dữ liệu hỏng thì không khởi động. */
class DurabilityTest extends ClusterTestSupport {

    @Test
    void voteIsOnDiskBeforeItIsGranted() {
        var follower = startNode("B", List.of("L", "B", "C"));

        assertTrue(follower.handleRequestVoteRequest(new RequestVoteRequest(5, "C", 0, 0)).voteGranted);

        var meta = metaOnDisk("B");
        assertEquals(5, meta.get("currentTerm").asLong());
        assertEquals("C", meta.get("votedFor").asText());
    }

    @Test
    void entriesAreOnDiskBeforeTheyAreAcknowledged() {
        var follower = startNode("B", List.of("L", "B"));

        var response = follower.handleAppendEntriesRequest(
                new AppendEntriesRequest(1, "L", 0, 0, List.of(entry(1, 1), entry(2, 1), entry(3, 1)), 0));

        assertEquals(3, response.matchIndex);
        assertEquals(3, follower.getPersistent().getLogStore().durableIndex());
        assertEquals(1, metaOnDisk("B").get("currentTerm").asLong());
    }

    @Test
    void candidateSelfVoteIsOnDiskBeforeVotesAreRequested() {
        var metaWhenAsked = new CopyOnWriteArrayList<JsonNode>();
        rpc.onRequestVote = req -> metaWhenAsked.add(metaOnDisk(req.candidateId));
        var follower = startNode("B", List.of("L", "B", "C"));
        follower.handleAppendEntriesRequest(new AppendEntriesRequest(1, "L", 0, 0, List.of(), 0));

        assertTrue(follower.handleTimeoutNowRequest(new TimeoutNowRequest(1, "L")).success);

        await("vote requests to be sent", () -> metaWhenAsked.size() == 2);
        for (JsonNode meta : metaWhenAsked) {
            assertEquals(2, meta.get("currentTerm").asLong());
            assertEquals("B", meta.get("votedFor").asText());
        }
    }

    @Test
    void leaderDoesNotCountItselfBeforeItsLogIsOnDisk() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1");
        var followers = others(leader.getNodeId());

        // chỉ còn leader và một follower: quorum buộc phải tính cả leader
        isolate(followers.get(1));
        runtimes.get(leader.getNodeId()).pauseIo();
        var before = leader.getPersistent().getLogStore().lastIndex();
        var future = leader.appendClientCommand("2".getBytes(StandardCharsets.UTF_8));
        // lệnh được ghi vào log trên thread của node, không phải trong lời gọi
        await("leader to append the entry", () -> leader.getPersistent().getLogStore().lastIndex() > before);
        var index = before + 1;
        await("follower to store the entry", () -> matchIndex(leader, followers.get(0)) >= index);

        // follower đã có entry trên đĩa, nhưng leader chưa fsync nên chưa được commit
        TimeUnit.MILLISECONDS.sleep(300);
        assertFalse(future.isDone());
        assertTrue(leader.getCommitIndex() < index);

        runtimes.get(leader.getNodeId()).resumeIo();
        assertTrue(future.get(5, TimeUnit.SECONDS));
    }

    @Test
    void nodeWithCorruptedLogRefusesToStartAndClusterCarriesOn() throws Exception {
        startCluster("A", "B", "C");
        var leader = awaitLeader();
        write(leader, "1", "2", "3");
        var broken = others(leader.getNodeId()).get(0);
        awaitStore(broken, List.of("1", "2", "3"));
        stopNode(broken);

        // một bit hỏng giữa log của node đang tắt
        var segment = dataDir.resolve(broken).resolve("log_1.rec");
        var data = Files.readAllBytes(segment);
        data[32 + 10] ^= 0x01; // trong khung đầu tiên, ngay sau header 32 byte của segment
        Files.write(segment, data);

        assertThrows(Exception.class, () -> startNode(broken, List.of("A", "B", "C")));
        runtimes.get(broken).shutdown();

        // hai node còn lại vẫn là đa số
        write(awaitLeader(), "4");
        for (String id : others(broken)) {
            awaitStore(id, List.of("1", "2", "3", "4"));
        }
    }

    @Test
    void nodeWithCorruptedMetaRefusesToStart() throws Exception {
        startCluster("A");
        write(awaitLeader(), "1");
        stopNode("A");

        // term hoặc phiếu bầu bị đổi âm thầm còn nguy hiểm hơn mất hẳn: node có thể bỏ phiếu hai lần
        var meta = dataDir.resolve("A").resolve("raft_meta.json");
        var text = Files.readString(meta);
        assertTrue(text.contains("\"currentTerm\" : 1"));
        Files.writeString(meta, text.replace("\"currentTerm\" : 1", "\"currentTerm\" : 0"));

        assertThrows(Exception.class, () -> startNode("A", List.of("A")));
        runtimes.get("A").shutdown();
    }

    @Test
    void commitIndexIsKeptInItsOwnFileAndADamagedOneIsIgnored() throws Exception {
        startCluster("A");
        write(awaitLeader(), "1", "2", "3");
        var commitIndex = nodes.get("A").getCommitIndex();
        stopNode("A");

        // file riêng, không nằm trong raft_meta.json: ghi nó không cần fsync
        var file = dataDir.resolve("A").resolve("commit_index");
        var saved = new String(Checksum.unwrap(Files.readAllBytes(file), file.toString()), StandardCharsets.US_ASCII);
        assertEquals(commitIndex, Long.parseLong(saved));
        startNode("A", List.of("A"));
        // apply lại ngay lúc khởi động, trước khi có leader nào
        assertEquals(List.of("1", "2", "3"), machines.get("A").getStore());
        stopNode("A");

        // mất điện giữa lúc ghi đè: node vẫn khởi động, chỉ phải chờ leader commit lại
        Files.writeString(file, "0000");
        startNode("A", List.of("A"));
        awaitStore("A", List.of("1", "2", "3"));
    }
}

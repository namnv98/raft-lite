package com.namnv.samples;

import com.namnv.raft.NodeState;
import com.namnv.raft.RaftNode;
import com.namnv.raft.config.NodeOptions;
import com.namnv.raft.config.RaftConfig;
import com.namnv.raft.example.ListStateMachine;
import com.namnv.transport.InMemoryRpcClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AppRaftInMem {
    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File("data"));

        List<String> nodes = Arrays.asList("A", "B", "C");
        InMemoryRpcClient rpc = new InMemoryRpcClient();

        Map<String, Set<String>> reachable = new HashMap<>();
        reachable.put("A", new HashSet<>(Arrays.asList("A", "B", "C")));
        reachable.put("B", new HashSet<>(Arrays.asList("A", "B", "C")));
        reachable.put("C", new HashSet<>(Arrays.asList("A", "B", "C")));
        rpc.setReachable(reachable);

        List<RaftNode> clusters = new ArrayList<RaftNode>();
        for (int i = 0; i < nodes.size(); i++) {
            String nodeFolder = "data/" + nodes.get(i);

            NodeOptions nodeOptions = NodeOptions.builder()
                    .raftMetaUri(nodeFolder)
                    .logUri(nodeFolder)
                    .snapshotUri(nodeFolder)
                    .electionTimeoutMinMs(300)
                    .electionTimeoutMaxMs(500)
                    .heartbeatIntervalMs(100)
                    .stateMachine(new ListStateMachine())
                    .raftConfig(RaftConfig.builder().self(nodes.get(i)).peers(nodes).build())
                    .build();

            clusters.add(new RaftNode(nodeOptions, rpc));
        }

        for (RaftNode n : clusters) {
            rpc.register(n.getNodeId(), n);
            n.start();
        }

        TimeUnit.SECONDS.sleep(3);
        var leader = getLeader(clusters);

        leader.appendClientCommand("data_1".getBytes(StandardCharsets.UTF_8));
        TimeUnit.SECONDS.sleep(3);

        log.info(leader.getNodeId() + " bị network partition");
        partitionNode(leader.getNodeId(), rpc.getReachable());
        clusters.remove(leader);
        TimeUnit.SECONDS.sleep(3);

        log.info(leader.getNodeId() + " node rejoin");
        restoreNode(leader.getNodeId(), rpc.getReachable());
        clusters.add(leader);
        TimeUnit.SECONDS.sleep(3);


        restoreNode("D", rpc.getReachable());
        NodeOptions nodeOptions = NodeOptions.builder()
                .raftMetaUri("data/D")
                .logUri("data/D")
                .snapshotUri("data/D")
                .electionTimeoutMinMs(300)
                .electionTimeoutMaxMs(500)
                .heartbeatIntervalMs(100)
                .stateMachine(new ListStateMachine())
                .raftConfig(RaftConfig.builder().self("D").build())
                .build();

        var raftNode = new RaftNode(nodeOptions, rpc);
        raftNode.start();
        clusters.add(raftNode);

        rpc.register("D", raftNode);
        getLeader(clusters).onJoinPeerCluster("D");

        TimeUnit.SECONDS.sleep(3);

        getLeader(clusters).appendClientCommand("data_3".getBytes(StandardCharsets.UTF_8)).get();

        TimeUnit.SECONDS.sleep(3);

        for (RaftNode n : clusters) {
            n.createSnapshot();
        }

        TimeUnit.SECONDS.sleep(300000);
    }

    private static RaftNode getLeader(List<RaftNode> clusters) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            for (RaftNode n : clusters) {
                if (n.getState() == NodeState.LEADER) {
                    return n;
                }
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        throw new IllegalStateException("No leader elected after 10s");
    }

    private static void partitionNode(String node, Map<String, Set<String>> network) {
        // Tất cả node khác không thể gửi tới node này
        for (var entry : network.entrySet()) {
            entry.getValue().remove(node);
        }
        // Node bị partition chỉ nhìn thấy chính nó
        var nodeSet = new HashSet<String>();
        nodeSet.add(node);
        network.put(node, nodeSet);
    }


    private static void restoreNode(String node, Map<String, Set<String>> network) {
        network.putIfAbsent(node, new HashSet<>());

        // Kết nối 2 chiều giữa node và tất cả node còn lại
        for (var entry : network.entrySet()) {
            String other = entry.getKey();
            if (!other.equals(node)) {
                entry.getValue().add(node);        // other → node
                network.get(node).add(other);      // node → other
            }
        }
    }
}



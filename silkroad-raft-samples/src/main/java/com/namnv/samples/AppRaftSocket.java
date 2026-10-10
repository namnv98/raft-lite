package com.namnv.samples;

import com.namnv.raft.NodeState;
import com.namnv.raft.RaftNode;
import com.namnv.raft.config.NodeOptions;
import com.namnv.raft.config.RaftConfig;
import com.namnv.raft.example.ListStateMachine;
import com.namnv.rpc.RpcProcessor;
import com.namnv.transport.SocketRpcClient;
import com.namnv.transport.SocketRpcServer;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class AppRaftSocket {
    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File("data"));

        List<String> nodes = Arrays.asList("localhost:8080", "localhost:8081", "localhost:8082");
        RpcProcessor rpc = new SocketRpcClient(1000);

        List<RaftNode> clusters = new ArrayList<RaftNode>();
        for (int i = 0; i < nodes.size(); i++) {
            String nodeFolder = "data/" + nodes.get(i);

            NodeOptions nodeOptions = NodeOptions.builder()
                    .raftMetaUri(nodeFolder)
                    .logUri(nodeFolder)
                    .snapshotUri(nodeFolder)
                    .electionTimeoutMinMs(200)
                    .electionTimeoutMaxMs(500)
                    .heartbeatIntervalMs(100)
                    .stateMachine(new ListStateMachine())
                    .raftConfig(RaftConfig.builder().self(nodes.get(i)).peers(nodes).build())
                    .build();

            clusters.add(new RaftNode(nodeOptions, rpc));
        }

        for (RaftNode n : clusters) {
            startRpcServer(n);
            n.start();
        }

        TimeUnit.SECONDS.sleep(3);
        var leader = getLeader(clusters);

        leader.appendClientCommand("data_1".getBytes(StandardCharsets.UTF_8));
        TimeUnit.SECONDS.sleep(3);

        NodeOptions nodeOptions = NodeOptions.builder()
                .raftMetaUri("data/D")
                .logUri("data/D")
                .snapshotUri("data/D")
                .electionTimeoutMinMs(300)
                .electionTimeoutMaxMs(500)
                .heartbeatIntervalMs(100)
                .stateMachine(new ListStateMachine())
                .raftConfig(RaftConfig.builder().self("localhost:8083").build())
                .build();

        var raftNode = new RaftNode(nodeOptions, rpc);
        startRpcServer(raftNode);
        raftNode.start();
        clusters.add(raftNode);

        getLeader(clusters).onJoinPeerCluster("localhost:8083");

        TimeUnit.SECONDS.sleep(3);

        getLeader(clusters).appendClientCommand("data_3".getBytes(StandardCharsets.UTF_8)).get();

        TimeUnit.SECONDS.sleep(3);

        for (RaftNode n : clusters) {
            n.createSnapshot();
        }

        TimeUnit.SECONDS.sleep(300000);
    }

    // transport nằm ngoài RaftNode: node chỉ cần biết RpcProcessor để gửi và được gọi qua RaftServerService
    private static void startRpcServer(RaftNode node) {
        int port = Integer.parseInt(node.getNodeId().split(":")[1]);
        new SocketRpcServer(port, node).start();
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
}



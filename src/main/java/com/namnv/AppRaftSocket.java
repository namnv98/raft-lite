package com.namnv;

import com.namnv.config.NodeOptions;
import com.namnv.config.RaftConfig;
import com.namnv.core.RaftNode;
import com.namnv.rpc.client.SocketRpcClient;
import com.namnv.rpc.client.RpcProcessor;
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
                    .stateMachine(new KeyValueStateMachine())
                    .raftConfig(RaftConfig.builder().self(nodes.get(i)).peers(nodes).build())
                    .build();

            clusters.add(new RaftNode(nodeOptions, rpc));
        }

        for (RaftNode n : clusters) {
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
                .stateMachine(new KeyValueStateMachine())
                .raftConfig(RaftConfig.builder().self("localhost:8083").build())
                .build();

        var raftNode = new RaftNode(nodeOptions, rpc);
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

    private static RaftNode getLeader(List<RaftNode> clusters) {
        while (true) {
            for (RaftNode n : clusters) {
                if (n.getState() == RaftNode.NodeState.LEADER) {
                    return n;
                }
            }
        }
    }
}



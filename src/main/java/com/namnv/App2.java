package com.namnv;


import com.namnv.config.RaftConfig;
import com.namnv.core.RaftNode;
import com.namnv.rpc.InProcessRPC;
import com.namnv.statemachine.KeyValueStateMachine;
import com.namnv.storage.FileLogStore;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class App2 {
    public static void main(String[] args) throws Exception {
//        com.alipay.sofa.jraft.core.NodeImpl;
//        FileUtils.deleteDirectory(new File("data"));

        List<String> nodes = Arrays.asList("A", "B", "C");
        InProcessRPC rpc = new InProcessRPC();
        RaftConfig cfg = new RaftConfig(300, 500, 100);

        Map<String, Set<String>> reachable = new HashMap<>();
        reachable.put("A", new HashSet<>(Arrays.asList("A", "B", "C")));
        reachable.put("B", new HashSet<>(Arrays.asList("A", "B", "C")));
        reachable.put("C", new HashSet<>(Arrays.asList("A", "B", "C")));
        rpc.setReachable(reachable);


        List<RaftNode> clusters = new ArrayList<RaftNode>();
        for (int i = 0; i < nodes.size(); i++) {
            String nodeFolder = "data/" + nodes.get(i);
            FileLogStore logStore = new FileLogStore(nodeFolder, 10, 2);
            clusters.add(new RaftNode(nodes.get(i), nodes, cfg, rpc, logStore, nodeFolder, new KeyValueStateMachine()));
        }

        for (RaftNode n : clusters) {
            rpc.register(n.getNodeId(), n);
            n.start();
        }

        TimeUnit.SECONDS.sleep(3);

        var leader = getLeader(clusters);
        leader.appendClientCommand("data_1".getBytes(StandardCharsets.UTF_8));

        TimeUnit.SECONDS.sleep(3);

        System.out.println(leader.getNodeId() + " bị network partition");
        partitionNode(leader.getNodeId(), rpc.getReachable());
        clusters.remove(leader);

        TimeUnit.SECONDS.sleep(3);

        getLeader(clusters).appendClientCommand("data_2".getBytes(StandardCharsets.UTF_8));

        TimeUnit.SECONDS.sleep(3);

        System.out.println(leader.getNodeId() + " node rejoin");
        restoreNode(leader.getNodeId(), rpc.getReachable());
        clusters.add(leader);

        TimeUnit.SECONDS.sleep(3);

        restoreNode("D", rpc.getReachable());
        FileLogStore logStore = new FileLogStore("data/D", 10, 2);
        var raftNode = new RaftNode("D", nodes, cfg, rpc, logStore, "data/D", new KeyValueStateMachine());
        raftNode.start();
        clusters.add(raftNode);

        rpc.register("D", raftNode);

        getLeader(clusters).onJoinPeerCluster("D");

        TimeUnit.SECONDS.sleep(3);

        getLeader(clusters).appendClientCommand("data_3".getBytes(StandardCharsets.UTF_8)).get();

        TimeUnit.SECONDS.sleep(10);

//        for (RaftNode n : clusters) {
//            n.createSnapshot();
//        }

        TimeUnit.SECONDS.sleep(300000);
    }

    private static RaftNode getLeader(List<RaftNode> clusters) {
        for (RaftNode n : clusters) {
            if (n.getState() == RaftNode.NodeState.LEADER) {
                return n;
            }
        }
        return null;
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
        // Nếu node chưa có trong map thì tạo
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



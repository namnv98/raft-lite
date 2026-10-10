package com.namnv.reference;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleRaftCluster {

    // StateMachine in ra lệnh được apply
    public static class MyStateMachine extends StateMachineAdapter {
        private final String nodeId;

        public MyStateMachine(String nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public void onApply(Iterator iter) {
            while (iter.hasNext()) {
                ByteBuffer task = iter.next();
                String cmd = new String(String.valueOf(task));
                System.out.println("Node " + nodeId + " applied: " + cmd);
            }
        }


        @Override
        public void onSnapshotSave(SnapshotWriter writer, Closure done) {
            try {
                // snapshot lưu vào file trong thư mục snapshot của node
                File snapshotFile = new File(writer.getPath(), "snapshot.data");
                try (FileOutputStream fos = new FileOutputStream(snapshotFile)) {
                    String snapshotData = "Snapshot node " + nodeId + " tại " + System.currentTimeMillis();
                    fos.write(snapshotData.getBytes());
                }

                    writer.addFile("snapshot.data");
                System.out.println("Node " + nodeId + " saved snapshot");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Load snapshot khi node khởi động hoặc join
        @Override
        public boolean onSnapshotLoad(SnapshotReader reader) {
            try {
                File snapshotFile = new File(reader.getPath(), "snapshot.data");
                if (snapshotFile.exists()) {
                    String content = java.nio.file.Files.readString(snapshotFile.toPath());
                    System.out.println("Node " + nodeId + " loaded snapshot: " + content);
                }
                return true;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int[] ports = {8081, 8082, 8083};
        RaftGroupService[] services = new RaftGroupService[ports.length];
        Node[] nodes = new Node[ports.length];

        // 1️⃣ Start 3 node ban đầu
        for (int i = 0; i < ports.length; i++) {
            PeerId self = new PeerId("localhost", ports[i]);
            Configuration conf = new Configuration();
            conf.parse("localhost:8081,localhost:8082,localhost:8083");

            NodeOptions nodeOptions = new NodeOptions();
            nodeOptions.setElectionTimeoutMs(1000);
            nodeOptions.setInitialConf(conf);
            nodeOptions.setLogUri("node/node" + ports[i] + "/log");
            nodeOptions.setRaftMetaUri("node/node" + ports[i] + "/meta");
            nodeOptions.setSnapshotUri("node/node" + ports[i] + "/snapshot");
            nodeOptions.setFsm(new MyStateMachine(String.valueOf(ports[i])));
            nodeOptions.setSnapshotIntervalSecs(10); // snapshot mỗi 10s nếu có log mới

            services[i] = new RaftGroupService("raft_group", self, nodeOptions);
            nodes[i] = services[i].start();
        }

        // 2️⃣ Chờ leader ổn định
        Node leader = null;
        int stableCount = 0;
        while (stableCount < 3) {
            leader = null;
            for (Node n : nodes) {
                if (n.isLeader()) {
                    leader = n;
                    break;
                }
            }
            if (leader != null) {
                stableCount++;
            } else {
                stableCount = 0;
                System.out.println("Waiting for leader...");
            }
            TimeUnit.MILLISECONDS.sleep(500);
        }
        System.out.println("Leader is: " + leader.getNodeId());

        for (int i = 0; i < 10000; i++) {
            // 5️⃣ Leader gửi lệnh mới sau khi node 4 join
            Task task2 = new Task();
            task2.setData(ByteBuffer.wrap("cmd2".getBytes()));
            leader.apply(task2);

        }

        TimeUnit.SECONDS.sleep(20);

        // 4️⃣ Thêm node4
        PeerId node4Id = new PeerId("localhost", 8084);
        NodeOptions node4Options = new NodeOptions();
        node4Options.setElectionTimeoutMs(1000);
        node4Options.setLogUri("node/node8084/log");
        node4Options.setRaftMetaUri("node/node8084/meta");
        node4Options.setSnapshotUri("node/node8084/snapshot");
        node4Options.setFsm(new MyStateMachine("8084"));
        node4Options.setSnapshotIntervalSecs(10); // snapshot mỗi 10s nếu có log mới

        Configuration conf = new Configuration();
        conf.parse("localhost:8081,localhost:8082,localhost:8083,localhost:8084");

        node4Options.setInitialConf(conf);
        RaftGroupService service4 = new RaftGroupService("raft_group", node4Id, node4Options);
        Node node4 = service4.start(); // node4 khởi tạo
        node4.join();

        AtomicBoolean added = new AtomicBoolean(false);
        leader.addPeer(node4Id, status -> {
            if (status.isOk()) {
                System.out.println("Node 8084 đã được thêm vào cluster");
                added.set(true);
            } else {
                System.out.println("Leader bận, retry add node 8084 sau 1s");
            }
        });


        // 6️⃣ Giữ node chạy
        while (true) {
            TimeUnit.SECONDS.sleep(1);
        }
    }
}

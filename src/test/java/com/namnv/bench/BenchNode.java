package com.namnv.bench;

import com.namnv.config.NodeOptions;
import com.namnv.config.RaftConfig;
import com.namnv.core.RaftClientService;
import com.namnv.core.RaftNode;
import com.namnv.rpc.client.SocketRpcClient;
import com.namnv.rpc.server.SocketRpcServer;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Một node chạy thành tiến trình riêng cho {@link ClusterBenchmark}: nhận RPC của node khác và yêu cầu của client qua TCP.
 * Tham số: id (host:port) | danh sách peers cách nhau bằng dấu phẩy | thư mục dữ liệu.
 */
public class BenchNode {
    public static void main(String[] args) throws Exception {
        String id = args[0];
        List<String> peers = List.of(args[1].split(","));
        String folder = args[2];

        var machine = new RaftBenchmark.CountingMachine();
        var node = new RaftNode(NodeOptions.builder()
                .raftMetaUri(folder).logUri(folder).snapshotUri(folder)
                .electionTimeoutMinMs(1000).electionTimeoutMaxMs(2000).heartbeatIntervalMs(100)
                .clientTimeoutMs(10_000)
                .snapshotIntervalEntries(Long.getLong("bench.snapshotInterval", 200_000))
                .commitIndexFlushIntervalMs(Integer.getInteger("bench.commitFlushMs", 1000))
                .logSync(!"false".equals(System.getProperty("bench.logSync")))
                .logPreallocate(!"false".equals(System.getProperty("bench.logPreallocate")))
                .stateMachine(machine)
                .raftConfig(RaftConfig.builder().self(id).peers(peers).build())
                .build(), new SocketRpcClient(2000));
        // câu hỏi duy nhất: đã apply bao nhiêu lệnh
        var clientService = new RaftClientService(node,
                query -> ByteBuffer.allocate(Long.BYTES).putLong(machine.applied.get()).array());
        new SocketRpcServer(Integer.parseInt(id.split(":")[1]), node, null, clientService).start();
        node.start();
        System.out.println("READY " + id);
        Thread.currentThread().join();
    }
}

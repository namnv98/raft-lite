package com.namnv.bench;

import com.namnv.config.NodeOptions;
import com.namnv.config.RaftConfig;
import com.namnv.core.RaftClientService;
import com.namnv.core.RaftNode;
import com.namnv.core.ThreadedRuntime;
import com.namnv.kv.BufferedKvStateMachine;
import com.namnv.kv.LmdbKvStateMachine;
import com.namnv.kv.RocksDbKvStateMachine;
import java.nio.file.Path;
import com.namnv.rpc.client.RpcProcessor;
import com.namnv.rpc.client.SocketRpcClient;
import com.namnv.rpc.nio.NioRpcClient;
import com.namnv.rpc.nio.NioRpcServer;
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
        boolean logSync = !"false".equals(System.getProperty("bench.logSync"));
        // -Dbench.kv=lmdb|rocksdb: kho KV thay cho state machine chỉ đếm số lệnh. Kho fsync mỗi lô theo -Dbench.kvSync,
        // mặc định theo cùng cờ với log (RocksDB: bật WAL và sync mỗi lô; false thì tắt WAL)
        var store = System.getProperty("bench.kv", "");
        boolean kvSync = Boolean.parseBoolean(System.getProperty("bench.kvSync", String.valueOf(logSync)));
        BufferedKvStateMachine kv = switch (store) {
            case "lmdb", "true" -> new LmdbKvStateMachine(Path.of(folder, "kv"), kvSync, 100, 10_000, 16L << 30);
            case "rocksdb" -> new RocksDbKvStateMachine(Path.of(folder, "kv"), kvSync, 100, 10_000);
            default -> null;
        };
        // -Dbench.transport=socket: transport cũ (thread đọc/ghi riêng); mặc định là nio, chạy trên vòng của node
        boolean nio = !"socket".equals(System.getProperty("bench.transport"));
        var runtime = new ThreadedRuntime();
        RpcProcessor rpc = nio ? new NioRpcClient(runtime.loop(), 2000) : new SocketRpcClient(2000);
        var node = new RaftNode(NodeOptions.builder()
                .runtime(runtime)
                .raftMetaUri(folder).logUri(folder).snapshotUri(folder)
                .electionTimeoutMinMs(1000).electionTimeoutMaxMs(2000).heartbeatIntervalMs(100)
                .clientTimeoutMs(10_000)
                .snapshotIntervalEntries(Long.getLong("bench.snapshotInterval", 200_000))
                .commitIndexFlushIntervalMs(Integer.getInteger("bench.commitFlushMs", 1000))
                .logSync(logSync)
                .logPreallocate(!"false".equals(System.getProperty("bench.logPreallocate")))
                .stateMachine(kv != null ? kv : machine)
                .raftConfig(RaftConfig.builder().self(id).peers(peers).build())
                .build(), rpc);
        // câu hỏi duy nhất: đã apply bao nhiêu lệnh
        var clientService = kv != null ? new RaftClientService(node, kv::query) : new RaftClientService(node,
                query -> ByteBuffer.allocate(Long.BYTES).putLong(machine.applied.get()).array());
        int port = Integer.parseInt(id.split(":")[1]);
        if (nio) {
            new NioRpcServer(port, node, clientService, runtime.loop()).start();
        } else {
            new SocketRpcServer(port, node, null, clientService).start();
        }
        node.start();
        System.out.println("READY " + id);
        Thread.currentThread().join();
    }
}

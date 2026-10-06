package bench;

import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.samples.cluster.ClusterConfig;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;
import java.util.List;

/**
 * Một node Aeron Cluster chạy thành tiến trình riêng: media driver + archive + consensus module + service container.
 * Tham số: nodeId | thư mục dữ liệu | mức fsync (0 = không, 1 = fdatasync sau mỗi lần ghi log).
 */
public class AeronNode {
    static final int PORT_BASE = 19000;
    static final List<String> HOSTS = List.of("localhost", "localhost", "localhost");
    // -Dbench.netemIps=true: đường nội bộ giữa các node ở 127.0.1.N, cổng cho client ở 127.0.2.N, để tc netem chỉ làm chậm
    // đường giữa các node (xem bench/netem/netem.sh udp)
    static final boolean NETEM_IPS = Boolean.getBoolean("bench.netemIps");
    static final List<String> CLUSTER_HOSTS = NETEM_IPS ? List.of("127.0.1.1", "127.0.1.2", "127.0.1.3") : HOSTS;
    static final List<String> INGRESS_HOSTS = NETEM_IPS ? List.of("127.0.2.1", "127.0.2.2", "127.0.2.3") : HOSTS;

    public static void main(String[] args) {
        int nodeId = Integer.parseInt(args[0]);
        File parentDir = new File(args[1]);
        int syncLevel = Integer.parseInt(args[2]);

        ClusterConfig config = ClusterConfig.create(0, nodeId, INGRESS_HOSTS, CLUSTER_HOSTS, PORT_BASE, parentDir, new EchoService());
        config.errorHandler(Throwable::printStackTrace);
        config.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true);
        config.archiveContext().deleteArchiveOnStart(true).fileSyncLevel(syncLevel).catalogFileSyncLevel(syncLevel);
        config.consensusModuleContext().ingressChannel("aeron:udp").deleteDirOnStart(true).fileSyncLevel(syncLevel);

        if (Boolean.getBoolean("bench.tuned")) {
            // Cấu hình độ trễ thấp: không thread nào ngủ giữa các vòng lặp. Mỗi node dùng 4 thread quay liên tục
            // (driver, archive, consensus module, service) thay vì cách chia mặc định thành 8 thread, để ba node và client
            // vừa với số lõi của một máy.
            config.mediaDriverContext().threadingMode(ThreadingMode.SHARED).sharedIdleStrategy(new BusySpinIdleStrategy());
            config.archiveContext().threadingMode(ArchiveThreadingMode.SHARED).idleStrategySupplier(BusySpinIdleStrategy::new);
            config.consensusModuleContext().idleStrategySupplier(BusySpinIdleStrategy::new);
            config.clusteredServiceContext().idleStrategySupplier(BusySpinIdleStrategy::new);
        }

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
             ClusteredMediaDriver driver = ClusteredMediaDriver.launch(
                     config.mediaDriverContext(), config.archiveContext(), config.consensusModuleContext());
             ClusteredServiceContainer container = ClusteredServiceContainer.launch(config.clusteredServiceContext())) {
            System.out.println("READY " + nodeId);
            barrier.await();
        }
    }
}

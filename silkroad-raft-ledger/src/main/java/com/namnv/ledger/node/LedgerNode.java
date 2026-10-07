package com.namnv.ledger.node;

import com.namnv.ledger.event.EventPublisher;
import com.namnv.ledger.event.EventSink;
import com.namnv.ledger.event.JsonLinesEventSink;
import com.namnv.ledger.view.JdbcEventSink;
import com.namnv.ledger.state.Ledger;
import com.namnv.config.NodeOptions;
import com.namnv.config.RaftConfig;
import com.namnv.core.RaftClientService;
import com.namnv.core.RaftNode;
import com.namnv.core.ThreadedRuntime;
import com.namnv.transport.nio.NioRpcClient;
import com.namnv.transport.nio.NioRpcServer;

import java.net.InetAddress;
import java.nio.file.Path;
import java.util.List;

/**
 * Một node của dịch vụ sổ cái: đồng thuận Raft, state machine {@link Ledger}, transport NIO chạy trên thread của node.
 * <pre>
 * java ... com.namnv.ledger.node.LedgerNode host:port host1:port1,host2:port2,host3:port3 thư-mục-dữ-liệu
 * </pre>
 * -Dledger.logSync=false: không fsync log. -Dledger.snapshotInterval=100000: số entry giữa hai lần snapshot.
 * -Dledger.bindLocal=true: kết nối tới node khác đi từ chính địa chỉ của node này (cho tc netem).
 * -Dledger.learners=host:port,...: learner cố định (khai báo giống nhau trên mọi node). -Dledger.eventsFile=đường-dẫn:
 * phát mọi thay đổi đã commit vào file JSON lines đó (thường chỉ bật trên learner). -Dledger.eventsJdbcUrl=jdbc:postgresql://...
 * (cùng -Dledger.eventsJdbcUser/-Dledger.eventsJdbcPassword): phát vào phía truy vấn trong cơ sở dữ liệu quan hệ
 * ({@link JdbcEventSink}): số dư, giao dịch và sao kê cho hệ thống khác truy vấn.
 * -Dledger.expectedAccounts: số tài khoản cấp phát sẵn. -Dledger.expectedTransfers: kích thước mỗi đoạn bloom filter của
 * id giao dịch. -Dledger.segmentTransfers: số giao dịch mới giữ trong bộ nhớ trước khi ghi xuống RocksDB.
 */
public final class LedgerNode implements AutoCloseable {
    private final RaftNode node;
    private final Ledger ledger;
    private final NioRpcServer server;
    private final EventPublisher events;

    private LedgerNode(RaftNode node, Ledger ledger, NioRpcServer server, EventPublisher events) {
        this.node = node;
        this.ledger = ledger;
        this.server = server;
        this.events = events;
    }

    public static LedgerNode start(String id, List<String> peers, String dataDir, boolean logSync, long snapshotInterval,
                                   boolean bindLocal) throws Exception {
        return start(id, peers, dataDir, logSync, snapshotInterval, bindLocal, new Ledger(Path.of(dataDir, "transfers")));
    }

    /** @param ledger state machine, ví dụ với dung lượng đặt riêng ({@link Ledger#Ledger(Path, int, long, int)}) */
    public static LedgerNode start(String id, List<String> peers, String dataDir, boolean logSync, long snapshotInterval,
                                   boolean bindLocal, Ledger ledger) throws Exception {
        return start(id, peers, List.of(), dataDir, logSync, snapshotInterval, bindLocal, ledger, null);
    }

    /**
     * @param voters  các node bỏ phiếu
     * @param learners learner cố định (cùng danh sách trên mọi node); {@code id} có thể là một trong số đó
     * @param events  nơi phát mọi thay đổi đã commit (thường chỉ trên learner), hoặc null
     */
    public static LedgerNode start(String id, List<String> voters, List<String> learners, String dataDir, boolean logSync,
                                   long snapshotInterval, boolean bindLocal, Ledger ledger, EventPublisher events)
            throws Exception {
        if (events != null) {
            ledger.publishTo(events);
        }
        var runtime = new ThreadedRuntime();
        var self = bindLocal ? InetAddress.getByName(id.substring(0, id.lastIndexOf(':'))) : null;
        var node = new RaftNode(NodeOptions.builder()
                .raftMetaUri(dataDir).logUri(dataDir).snapshotUri(dataDir)
                .electionTimeoutMinMs(1000).electionTimeoutMaxMs(2000).heartbeatIntervalMs(100)
                .clientTimeoutMs(10_000)
                .snapshotIntervalEntries(snapshotInterval)
                .logSync(logSync)
                .runtime(runtime)
                .stateMachine(ledger)
                .raftConfig(RaftConfig.builder().self(id).peers(voters).learners(learners).build())
                .build(), new NioRpcClient(runtime.loop(), 2000, self));
        var server = new NioRpcServer(Integer.parseInt(id.substring(id.lastIndexOf(':') + 1)), node,
                new RaftClientService(node, ledger::query), runtime.loop());
        server.start();
        node.start();
        return new LedgerNode(node, ledger, server, events);
    }

    public RaftNode node() {
        return node;
    }

    public Ledger ledger() {
        return ledger;
    }

    @Override
    public void close() {
        server.stop();
        node.shutdown();
        ledger.close();
        if (events != null) {
            try {
                events.close();
            } catch (java.io.IOException e) {
                // sink đóng không sạch: lần mở sau cắt bỏ dòng ghi dở
            }
        }
    }

    public static void main(String[] args) throws Exception {
        var learners = System.getProperty("ledger.learners", "").isBlank() ? List.<String>of()
                : List.of(System.getProperty("ledger.learners").split(","));
        // nơi phát sự kiện (thường chỉ trên learner): phía truy vấn trong cơ sở dữ liệu quan hệ, hoặc một file JSON lines
        var eventsJdbcUrl = System.getProperty("ledger.eventsJdbcUrl", "");
        var eventsFile = System.getProperty("ledger.eventsFile", "");
        EventSink sink = !eventsJdbcUrl.isBlank()
                ? new JdbcEventSink(eventsJdbcUrl, System.getProperty("ledger.eventsJdbcUser"), System.getProperty("ledger.eventsJdbcPassword"))
                : !eventsFile.isBlank() ? new JsonLinesEventSink(Path.of(eventsFile)) : null;
        var events = sink == null ? null : new EventPublisher(sink, 100_000);
        start(args[0], List.of(args[1].split(",")), learners, args[2],
                !"false".equals(System.getProperty("ledger.logSync")),
                Long.getLong("ledger.snapshotInterval", 100_000),
                Boolean.getBoolean("ledger.bindLocal"),
                new Ledger(Path.of(args[2], "transfers"), Integer.getInteger("ledger.expectedAccounts", 1024),
                        Long.getLong("ledger.expectedTransfers", 16_000_000),
                        Integer.getInteger("ledger.segmentTransfers", 1 << 20)),
                events);
        System.out.println("READY " + args[0]);
        Thread.currentThread().join();
    }
}

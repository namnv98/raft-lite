package com.namnv.config;


import com.namnv.core.RaftRuntime;
import com.namnv.statemachine.StateMachine;
import com.namnv.storage.DiskFaultInjector;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class NodeOptions {
    private StateMachine stateMachine;
    private String logUri;
    private String raftMetaUri;
    private String snapshotUri;

    private RaftConfig raftConfig;

    // null: dùng ThreadedRuntime (thread và đồng hồ thật)
    private RaftRuntime runtime;

    // chỉ dùng trong test để giả lập lỗi ghi đĩa
    @Builder.Default
    private DiskFaultInjector diskFaults = DiskFaultInjector.NONE;

    private int electionTimeoutMinMs;
    private int electionTimeoutMaxMs;
    private int heartbeatIntervalMs;

    // client chờ một lệnh hoặc một lần đọc tối đa bao lâu trước khi nhận kết quả "không rõ"
    @Builder.Default
    private int clientTimeoutMs = 5000;

    // số entry tối đa trong một AppendEntries, để một follower tụt xa không nhận một request khổng lồ
    @Builder.Default
    private int maxEntriesPerRequest = 1024;

    // số AppendEntries leader được gửi liên tiếp cho một follower mà chưa có câu trả lời (pipelining), khi log của
    // follower đã khớp. Với một request mỗi lần, mỗi lệnh chờ thêm tới một RTT và throughput bị giới hạn ở
    // (số entry mỗi request) / RTT. 1 = tắt pipelining: trên loopback trần cao hơn chừng 5-10% vì request to hơn
    @Builder.Default
    private int maxInflightAppends = 32;

    // leader từ chối lệnh mới khi số lệnh đang chờ commit vượt mức này (backpressure)
    @Builder.Default
    private int maxPendingCommands = 100_000;

    // số entry mới nhất của log được giữ trong bộ nhớ; entry cũ hơn (cho follower tụt xa, hay khi khởi động lại)
    // được đọc lại từ đĩa
    @Builder.Default
    private int logCacheEntries = 16_384;

    // kích thước mỗi file segment của log; một entry không được lớn hơn một segment
    @Builder.Default
    private int logSegmentBytes = 64 << 20;

    /**
     * true (mặc định): một entry chỉ được coi là bền vững, và node chỉ trả lời "đã lưu" cho leader hay tính mình vào
     * quorum, sau khi log đã được ép xuống đĩa (fsync). Đây là giả định của Raft: node đã hứa thì sau khi khởi động
     * lại vẫn còn entry đó.
     * <p>
     * false: làm như Aeron Cluster. Entry được coi là bền vững ngay khi đã ghi vào file (page cache của hệ điều hành);
     * hệ điều hành tự ghi xuống đĩa sau. Độ trễ không còn phụ thuộc tốc độ fsync của ổ đĩa, đổi lại:
     * <ul>
     * <li>Tiến trình bị crash hay bị kill: không mất gì, dữ liệu đã nằm trong page cache.</li>
     * <li>Máy mất điện hoặc kernel treo: node đó mất các entry cuối chưa kịp xuống đĩa dù đã trả lời "đã lưu". Chỉ một
     * node như vậy cũng có thể làm mất lệnh đã commit (nó quên entry rồi cùng một node đang tụt lại bầu ra leader mới
     * không có entry đó), và nếu đa số node cùng mất điện thì chắc chắn mất.</li>
     * </ul>
     * Chỉ nên tắt khi các node nằm trên những máy có nguồn điện độc lập và hệ thống chấp nhận rủi ro trên.
     * Term và phiếu bầu (raft_meta.json) cùng snapshot vẫn luôn được fsync, vì chúng hiếm khi được ghi.
     */
    @Builder.Default
    private boolean logSync = true;

    // Cấp phát sẵn file segment kế tiếp ở thread nền (ghi đầy byte 0 rồi fsync). fsync trên file cấp phát sẵn nhanh hơn
    // vài lần so với trên file thưa, vì filesystem không phải ghi nhận block mới cho mỗi lần fsync. Đổi lại mỗi segment
    // được ghi hai lần, nên log chỉ làm việc này khi nó lớn chậm (dưới khoảng 40 MB/giây), và chỉ khi logSync = true.
    @Builder.Default
    private boolean logPreallocate = true;

    // commit index được ghi xuống đĩa nhiều nhất mỗi khoảng này một lần. Nó chỉ giúp lần khởi động sau apply lại
    // nhanh hơn chứ không cần cho tính đúng đắn, nên không đáng tốn một lần fsync cho mỗi lệnh; 0 là ghi sau mỗi lần commit
    @Builder.Default
    private int commitIndexFlushIntervalMs = 1000;

    // snapshot được gửi cho follower theo từng mẩu không quá kích thước này
    @Builder.Default
    private int snapshotChunkBytes = 1 << 20;

    // tự tạo snapshot khi số entry đã apply mà chưa compact đạt mức này; 0 là tắt
    @Builder.Default
    private long snapshotIntervalEntries = 0;

    // node tự shutdown khi cấu hình không còn nó đã commit; false thì node chỉ đứng yên
    @Builder.Default
    private boolean shutdownOnRemoved = true;

    // leader còn cố gửi cấu hình cuối cho node vừa bị gỡ trong bao lâu trước khi bỏ cuộc
    @Builder.Default
    private int departingTimeoutMs = 10_000;

    // node mới phải bắt kịp log trong thời gian này thì mới được đưa vào cấu hình; quá hạn thì thay đổi bị huỷ
    @Builder.Default
    private int catchUpTimeoutMs = 30_000;
}

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

    // leader từ chối lệnh mới khi số lệnh đang chờ commit vượt mức này (backpressure)
    @Builder.Default
    private int maxPendingCommands = 100_000;

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

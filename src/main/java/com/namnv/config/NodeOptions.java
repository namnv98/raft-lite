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

    // node tự shutdown khi cấu hình không còn nó đã commit; false thì node chỉ đứng yên
    @Builder.Default
    private boolean shutdownOnRemoved = true;

    // leader còn cố gửi cấu hình cuối cho node vừa bị gỡ trong bao lâu trước khi bỏ cuộc
    @Builder.Default
    private int departingTimeoutMs = 10_000;
}

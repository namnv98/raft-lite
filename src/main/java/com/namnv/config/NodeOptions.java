package com.namnv.config;


import com.namnv.statemachine.StateMachine;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class NodeOptions {
    private StateMachine stateMachine;
    private String logUri;
    private String raftMetaUri;
    private String snapshotUri;
    private String snapshotTempUri;

    private RaftConfig raftConfig;

    private int electionTimeoutMinMs;
    private int electionTimeoutMaxMs;
    private int heartbeatIntervalMs;
}

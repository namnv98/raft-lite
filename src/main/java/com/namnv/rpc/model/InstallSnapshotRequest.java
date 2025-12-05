package com.namnv.rpc.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class InstallSnapshotRequest {
    private long term;                // leader term
    private String leaderId;          // leader nodeId
    private long lastIncludedIndex;   // snapshot lastIncludedIndex
    private long lastIncludedTerm;    // snapshot lastIncludedTerm
    private String path;      // serialized snapshot
}

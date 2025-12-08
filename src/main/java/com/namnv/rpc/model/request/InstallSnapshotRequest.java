package com.namnv.rpc.model.request;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

@Data
@AllArgsConstructor
public class InstallSnapshotRequest implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private long term;                // leader term
    private String leaderId;          // leader nodeId
    private long lastIncludedIndex;   // snapshot lastIncludedIndex
    private long lastIncludedTerm;    // snapshot lastIncludedTerm
    private String path;      // serialized snapshot
}

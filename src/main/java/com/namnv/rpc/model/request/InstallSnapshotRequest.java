package com.namnv.rpc.model.request;

import com.namnv.entity.ConfigurationEntry;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

@Data
@AllArgsConstructor
public class InstallSnapshotRequest implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private long term;                // leader term
    private String leaderId;          // leader nodeId
    private long lastIncludedIndex;   // snapshot lastIncludedIndex
    private long lastIncludedTerm;    // snapshot lastIncludedTerm
    private ConfigurationEntry conf;  // cluster config tại lastIncludedIndex
    private Map<String, byte[]> files; // tên file snapshot -> nội dung
}

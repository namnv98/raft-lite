package com.namnv.statemachine.snapshot;

import com.namnv.entity.ConfigurationEntry;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SnapshotMeta {
    private long lastIncludedIndex;
    private long lastIncludedTerm;
    // cấu hình cluster tại lastIncludedIndex, vì config entry có thể đã bị compact khỏi log
    private ConfigurationEntry conf;
    private List<String> files;
}

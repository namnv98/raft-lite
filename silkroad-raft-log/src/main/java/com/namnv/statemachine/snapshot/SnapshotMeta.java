package com.namnv.statemachine.snapshot;

import com.namnv.entity.ClientSession;
import com.namnv.entity.ConfigurationEntry;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
public class SnapshotMeta {
    private long lastIncludedIndex;
    private long lastIncludedTerm;
    // cấu hình cluster tại lastIncludedIndex, vì config entry có thể đã bị compact khỏi log
    private ConfigurationEntry conf;
    private List<String> files;
    // clientId -> các sequence đã apply tính tới lastIncludedIndex, để việc chống ghi trùng sống qua snapshot
    private Map<String, ClientSession> sessions;
    // thời điểm của entry tại lastIncludedIndex (xem LogEntry#getTimestamp): leader mới không gắn thời điểm nhỏ hơn mốc này
    // kể cả khi log của nó đã bị compact hết vào snapshot; 0 với snapshot cũ
    private long lastIncludedTimestamp;

    public SnapshotMeta(long lastIncludedIndex, long lastIncludedTerm, ConfigurationEntry conf, List<String> files,
                        Map<String, ClientSession> sessions) {
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.conf = conf;
        this.files = files;
        this.sessions = sessions;
    }
}

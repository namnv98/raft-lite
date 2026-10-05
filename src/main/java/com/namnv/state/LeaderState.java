package com.namnv.state;

import lombok.Getter;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


@Getter
public class LeaderState {
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    // thời điểm (nanoTime) gần nhất peer trả lời, dùng để leader tự kiểm tra còn quorum hay không
    private final Map<String, Long> lastAck = new HashMap<>();
    // peer đang có RPC replicate chưa trả lời
    private final Set<String> inflight = new HashSet<>();
    // node vừa bị gỡ -> index của config entry cuối mà nó cần nhận để biết mình đã rời cluster
    private final Map<String, Long> departing = new HashMap<>();
    // thời hạn (nanoTime) ngừng gửi cho node đang rời đi nếu nó không bao giờ trả lời
    private final Map<String, Long> departingDeadline = new HashMap<>();

    // index của no-op mà leader ghi khi nhậm chức: khi nó commit thì leader biết chắc commit index của mình là mới nhất
    private final long termStartIndex;
    // mỗi AppendEntries gửi đi và mỗi yêu cầu đọc nhận một số thứ tự tăng dần
    private long stamp;
    // số thứ tự lớn nhất của request mà mỗi peer đã trả lời: peer đó vẫn coi node này là leader ở thời điểm đó
    private final Map<String, Long> ackedStamp = new HashMap<>();

    public long nextStamp() {
        return ++stamp;
    }

    public LeaderState(Collection<String> peers, long nextLogIndex, long nowNanos) {
        this.termStartIndex = nextLogIndex;
        for (String p : peers) {
            addPeer(p, nextLogIndex, nowNanos);
        }
    }

    public void addPeer(String peer, long nextLogIndex, long nowNanos) {
        nextIndex.putIfAbsent(peer, nextLogIndex);
        matchIndex.putIfAbsent(peer, 0L);
        lastAck.putIfAbsent(peer, nowNanos);
    }
}

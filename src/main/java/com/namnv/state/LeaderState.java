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

    public LeaderState(Collection<String> peers, long nextLogIndex) {
        for (String p : peers) {
            addPeer(p, nextLogIndex);
        }
    }

    public void addPeer(String peer, long nextLogIndex) {
        nextIndex.putIfAbsent(peer, nextLogIndex);
        matchIndex.putIfAbsent(peer, 0L);
        lastAck.putIfAbsent(peer, System.nanoTime());
    }
}

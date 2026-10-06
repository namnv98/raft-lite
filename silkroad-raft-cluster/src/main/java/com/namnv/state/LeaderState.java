package com.namnv.state;

import lombok.Getter;
import lombok.Setter;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


@Getter
public class LeaderState {
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    // thời điểm (nanoTime) gần nhất peer trả lời, dùng để leader tự kiểm tra còn quorum hay không
    private final Map<String, Long> lastAck = new HashMap<>();
    // tiến độ gửi cho từng peer: đang dò hay đang pipeline, và bao nhiêu request chưa trả lời
    private final Map<String, Progress> progress = new HashMap<>();
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

    // thay đổi cấu hình đang chờ: các node mới trong catchingUp phải bắt kịp log (chưa được tính vào quorum)
    // rồi leader mới ghi cấu hình joint dẫn tới pendingConf
    @Setter
    private List<String> pendingConf;
    private final Set<String> catchingUp = new HashSet<>();
    @Setter
    private long catchUpDeadline;

    // tiến độ gửi snapshot cho từng peer
    private final Map<String, SnapshotTransfer> snapshotTransfers = new HashMap<>();

    public static final class SnapshotTransfer {
        public final long index;
        public final String path;
        // mẩu kế tiếp cần gửi
        public int fileIndex;
        public long offset;

        public SnapshotTransfer(long index, String path) {
            this.index = index;
            this.path = path;
        }
    }

    /**
     * Cách leader gửi log cho một peer, như Progress của etcd/raft.
     * <ul>
     * <li>Dò ({@code pipelining = false}): chưa biết log của peer khớp tới đâu. Mỗi lúc một request; nextIndex chỉ đổi
     * khi có câu trả lời.</li>
     * <li>Pipeline: lần gửi trước đã khớp. Leader gửi liên tiếp nhiều request mà không chờ, và đẩy nextIndex lên ngay khi
     * gửi. Transport giữ thứ tự trên một kết nối, nên các request tới peer theo đúng thứ tự gửi.</li>
     * </ul>
     * Peer từ chối hoặc một request bị mất (lỗ hổng trong chuỗi) thì quay về dò. Response của các request gửi trước lúc
     * đó vẫn cập nhật được matchIndex (một lần ack thành công luôn đúng), nhưng không còn được tính vào {@link #inflight}.
     */
    public static final class Progress {
        public boolean pipelining;
        // request (AppendEntries hoặc mẩu snapshot) của lần dò/pipeline hiện tại chưa được trả lời
        public int inflight;
        // tăng mỗi lần quay về dò; request mang số cũ là của lần trước
        public long generation;

        public void restartProbe() {
            pipelining = false;
            inflight = 0;
            generation++;
        }
    }

    public Progress progress(String peer) {
        return progress.computeIfAbsent(peer, p -> new Progress());
    }

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

package bench;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * State machine tương đương CountingMachine của Raft Lite: đếm số lệnh, và xác nhận mỗi lệnh bằng 8 byte đầu của nó
 * (dấu thời gian mà client đã ghi vào).
 */
public class EchoService implements ClusteredService {
    private Cluster cluster;
    private long applied;

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        this.cluster = cluster;
    }

    @Override
    public void onSessionMessage(ClientSession session, long timestamp, DirectBuffer buffer, int offset, int length,
                                 Header header) {
        applied++;
        if (session != null) {
            // trên follower offer() chỉ là giả, trả về ngay
            while (session.offer(buffer, offset, Long.BYTES) < 0) {
                cluster.idleStrategy().idle();
            }
        }
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
    }

    @Override
    public void onTerminate(Cluster cluster) {
    }
}

package com.namnv.raft;

import com.namnv.entity.ConfigurationEntry;
import com.namnv.entity.LogEntry;
import com.namnv.raft.state.LeaderState;
import com.namnv.rpc.model.request.TimeoutNowRequest;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Thành viên của cluster: cấu hình đang có hiệu lực, thay đổi cấu hình bằng joint consensus (node mới bắt kịp log trước
 * như một learner), node đang rời đi, learner cố định, và việc leader tự gỡ mình rồi trao quyền.
 * Mọi method chạy trên thread của node, khi đang giữ lock của node.
 */
@Slf4j
final class Membership {
    private final RaftNode node;

    // cấu hình ban đầu, chỉ dùng khi log và snapshot chưa có config entry nào
    private final ConfigurationEntry initialConf;
    // cấu hình đang có hiệu lực = config entry mới nhất trong log (kể cả chưa commit)
    volatile ConfigurationEntry conf;
    long confIndex;
    // peers() của cấu hình peersOf
    private ConfigurationEntry peersOf;
    private List<String> peers = List.of();

    // node từng là thành viên; khi cấu hình không còn nó được commit thì node tự tắt
    private boolean wasMember;
    // learner cố định (RaftConfig.learners), trừ chính node này: leader gửi log cho họ nhưng không tính họ vào quorum
    private final List<String> learners;
    private boolean transferring;
    boolean removalHandled;

    Membership(RaftNode node) {
        this.node = node;
        var raftConfig = node.nodeOptions.getRaftConfig();
        this.initialConf = new ConfigurationEntry(raftConfig.getPeers());
        var learnerIds = new ArrayList<>(raftConfig.getLearners());
        learnerIds.remove(node.nodeId);
        learnerIds.removeAll(raftConfig.getPeers()); // đã là thành viên thì không còn là learner
        this.learners = List.copyOf(learnerIds);
        refreshConf();
    }

    boolean join(String newNodeId) {
        if (conf.contains(newNodeId)) {
            return false;
        }
        var newNodes = new ArrayList<>(conf.getOldNodes());
        newNodes.add(newNodeId);
        return changePeers(newNodes);
    }

    boolean leave(String removedNodeId) {
        var newNodes = new ArrayList<>(conf.getOldNodes());
        if (!newNodes.remove(removedNodeId)) {
            return false;
        }
        return changePeers(newNodes);
    }

    // xem RaftNode.changePeers
    boolean changePeers(Collection<String> newNodes) {
        var target = new ArrayList<>(new LinkedHashSet<>(newNodes));
        var current = conf.getOldNodes();
        var leaderState = node.leaderState;
        if (node.stopped || node.state != NodeState.LEADER || target.isEmpty()
                || (target.size() == current.size() && target.containsAll(current))) {
            return false;
        }
        // mỗi lần chỉ một thay đổi cấu hình, và cấu hình hiện tại phải commit xong
        if (conf.isJoint() || confIndex > node.commitIndex || leaderState.getPendingConf() != null) {
            log.warn("Node {} rejects change to {}: configuration change in progress <{}>.", node.nodeId, target, conf);
            return false;
        }
        var added = target.stream().filter(id -> !current.contains(id)).toList();
        if (added.isEmpty()) {
            return appendJointConf(target);
        }
        // node mới còn trống: đưa nó vào cấu hình ngay sẽ bắt cluster chờ nó mới commit được
        var now = node.runtime.nanoTime();
        leaderState.setPendingConf(target);
        leaderState.getCatchingUp().addAll(added);
        leaderState.setCatchUpDeadline(now + TimeUnit.MILLISECONDS.toNanos(node.nodeOptions.getCatchUpTimeoutMs()));
        log.info("Leader {} waits for {} to catch up before changing configuration to {}.", node.nodeId, added, target);
        for (var id : added) {
            leaderState.addPeer(id, node.persistent.getLogStore().lastIndex() + 1, now);
            node.replicator.replicateTo(id, true);
        }
        return true;
    }

    private boolean appendJointConf(List<String> target) {
        try {
            appendConfiguration(new ConfigurationEntry(conf.getOldNodes(), target, true));
            return true;
        } catch (Exception e) {
            log.error("Leader {} failed to append configuration change", node.nodeId, e);
            return false;
        }
    }

    // một node mới vừa nhận thêm log: khi mọi node mới đều chỉ còn cách leader không quá một request thì bắt đầu đổi cấu hình
    void onLearnerProgress(LeaderState ls, String peer, long match) {
        if (!ls.getCatchingUp().contains(peer)
                || node.persistent.getLogStore().lastIndex() - match > node.nodeOptions.getMaxEntriesPerRequest()) {
            return;
        }
        ls.getCatchingUp().remove(peer);
        if (ls.getCatchingUp().isEmpty()) {
            var target = ls.getPendingConf();
            ls.setPendingConf(null);
            appendJointConf(target);
        }
    }

    void expirePendingConf() {
        var leaderState = node.leaderState;
        if (leaderState.getPendingConf() != null && node.runtime.nanoTime() - leaderState.getCatchUpDeadline() > 0) {
            log.warn("Leader {} gives up changing configuration to {}: {} did not catch up in time.", node.nodeId,
                    leaderState.getPendingConf(), leaderState.getCatchingUp());
            leaderState.setPendingConf(null);
            leaderState.getCatchingUp().clear();
        }
    }

    private void appendConfiguration(ConfigurationEntry newConf) {
        var logStore = node.persistent.getLogStore();
        var index = logStore.lastIndex() + 1;
        logStore.appendEntry(node.stamped(LogEntry.newConfigurationEntry(index, node.persistent.getCurrentTerm(), newConf)));
        refreshConf();
        for (var peer : peers()) {
            node.leaderState.addPeer(peer, index, node.runtime.nanoTime());
        }
        log.info("Leader {} appended configuration <{}> at index {}.", node.nodeId, newConf, index);
        node.replicator.broadcast();
    }

    // gọi sau mỗi lần commit: đẩy thay đổi cấu hình sang bước kế tiếp
    void onConfCommitted() {
        if (node.state != NodeState.LEADER || confIndex > node.commitIndex) {
            return;
        }
        if (conf.isJoint()) {
            finalizeJointConf();
        } else if (!conf.contains(node.nodeId)) {
            handOverLeadership();
        }
    }

    // C(old,new) đã commit: ghi tiếp C(new)
    private void finalizeJointConf() {
        // node bị gỡ vẫn được gửi log tới khi nhận xong C(new), để nó biết mình đã rời cluster
        var finalIndex = node.persistent.getLogStore().lastIndex() + 1;
        for (var id : conf.getOldNodes()) {
            if (!conf.getNewNodes().contains(id) && !id.equals(node.nodeId)) {
                addDeparting(id, finalIndex);
            }
        }
        appendConfiguration(new ConfigurationEntry(conf.getNewNodes()));
    }

    // leader tự gỡ mình: trao quyền cho follower có log đầy đủ nhất thay vì để cluster chờ election timeout
    private void handOverLeadership() {
        var matchIndex = node.leaderState.getMatchIndex();
        var candidates = new ArrayList<>(peers());
        candidates.sort(Comparator.comparingLong((String peer) -> matchIndex.getOrDefault(peer, 0L)).reversed());
        var term = node.persistent.getCurrentTerm();
        log.info("Leader {} is no longer in configuration <{}>, step down.", node.nodeId, conf);
        node.election.becomeFollower(term);
        transferring = true;
        transferLeadership(candidates, 0, term);
    }

    // thử lần lượt từng follower tới khi một node nhận lời bầu cử ngay, hoặc cluster đã sang term mới
    private void transferLeadership(List<String> candidates, int position, long term) {
        if (position >= candidates.size()) {
            transferring = false;
            maybeShutdownRemoved();
            return;
        }
        var target = candidates.get(position);
        log.info("Node {} asks {} to take over leadership.", node.nodeId, target);
        node.rpcProcessor.timeoutNow(target, new TimeoutNowRequest(term, node.nodeId)).whenComplete((resp, error) -> {
            node.onNode(() -> {
                var accepted = resp != null && resp.success;
                if (accepted || node.stopped || node.persistent.getCurrentTerm() != term) {
                    transferring = false;
                    maybeShutdownRemoved();
                } else {
                    transferLeadership(candidates, position + 1, term);
                }
            });
        });
    }

    private void addDeparting(String id, long finalConfIndex) {
        // node đã chết hẳn thì không gửi mãi: bỏ sau một khoảng thời gian
        var deadline = node.runtime.nanoTime() + TimeUnit.MILLISECONDS.toNanos(node.nodeOptions.getDepartingTimeoutMs());
        node.leaderState.getDeparting().put(id, finalConfIndex);
        node.leaderState.getDepartingDeadline().put(id, deadline);
    }

    void removeDeparting(LeaderState ls, String id) {
        ls.getDeparting().remove(id);
        ls.getDepartingDeadline().remove(id);
    }

    // leader trước có thể chết khi node bị gỡ chưa kịp nhận C(new): leader mới suy lại từ joint config ngay trước đó trong log
    void restoreDepartingNodes() {
        var previousConfIndex = confIndexAt(confIndex - 1);
        if (conf.isJoint() || previousConfIndex == 0) {
            return;
        }
        var previous = node.persistent.getLogStore().get(previousConfIndex).getConfiguration();
        if (!previous.isJoint()) {
            return;
        }
        for (var id : previous.getOldNodes()) {
            if (!conf.contains(id) && !id.equals(node.nodeId)) {
                addDeparting(id, confIndex);
            }
        }
    }

    void expireDepartingNodes() {
        var now = node.runtime.nanoTime();
        var deadlines = node.leaderState.getDepartingDeadline();
        for (var id : new ArrayList<>(deadlines.keySet())) {
            if (now - deadlines.get(id) > 0) {
                removeDeparting(node.leaderState, id);
            }
        }
    }

    /**
     * Node đã bị gỡ và cấu hình đó đã commit thì không còn việc gì để làm: tự shutdown.
     * Node mới chưa từng là thành viên (đang chờ được thêm vào) thì không tính.
     */
    void maybeShutdownRemoved() {
        if (removalHandled || transferring || node.stopped || !wasMember || node.state == NodeState.LEADER) {
            return;
        }
        if (conf.isJoint() || conf.contains(node.nodeId) || confIndex > node.commitIndex) {
            return;
        }
        removalHandled = true;
        if (node.nodeOptions.isShutdownOnRemoved()) {
            log.info("Node {} was removed from the cluster <{}>, shutting down.", node.nodeId, conf);
            node.runIo(node::shutdown);
        } else {
            // vẫn chạy nhưng đứng yên: onElectionTimeout bỏ qua node không thuộc cấu hình
            log.info("Node {} was removed from the cluster <{}>.", node.nodeId, conf);
        }
    }

    // config entry mới nhất có index <= upTo; nếu đã bị compact thì lấy config lưu kèm snapshot
    ConfigurationEntry confAt(long upTo) {
        var index = confIndexAt(upTo);
        if (index > 0) {
            return node.persistent.getLogStore().get(index).getConfiguration();
        }
        var snapshotConf = node.persistent.getSnapshotConf();
        return snapshotConf != null ? snapshotConf : initialConf;
    }

    // 0 nếu trong log không còn config entry nào <= upTo
    private long confIndexAt(long upTo) {
        return node.persistent.getLogStore().lastConfigurationIndex(upTo);
    }

    void refreshConf() {
        var persistent = node.persistent;
        var lastIndex = persistent.getLogStore().lastIndex();
        var index = confIndexAt(lastIndex);
        this.conf = confAt(lastIndex);
        this.confIndex = index > 0 ? index : (persistent.getSnapshotConf() != null ? persistent.getLastSnapshotIndex() : 0);
        if (conf.contains(node.nodeId)) {
            wasMember = true;
            removalHandled = false; // được thêm lại sau khi bị gỡ
        }
    }

    // danh sách chỉ đọc, được dựng lại khi cấu hình đổi: nó được hỏi cho từng lệnh của client
    List<String> peers() {
        var current = conf;
        if (peersOf != current) {
            var others = new ArrayList<>(current.allNodes());
            others.remove(node.nodeId);
            peers = List.copyOf(others);
            peersOf = current;
        }
        return peers;
    }

    // mọi node leader cần gửi log: thành viên hiện tại, node đang rời đi, node mới đang bắt kịp và learner cố định
    Collection<String> replicationTargets() {
        var leaderState = node.leaderState;
        if (leaderState.getDeparting().isEmpty() && leaderState.getCatchingUp().isEmpty() && learners.isEmpty()) {
            return peers();
        }
        Set<String> targets = new LinkedHashSet<>(peers());
        targets.addAll(leaderState.getDeparting().keySet());
        targets.addAll(leaderState.getCatchingUp());
        for (String learner : learners) {
            // learner được thêm vào cấu hình thì đã nằm trong peers
            targets.add(learner);
        }
        return targets;
    }

    // tập phiếu ban đầu của một lần đếm quorum: node luôn tính chính nó
    Set<String> selfOnly() {
        Set<String> nodes = new HashSet<>();
        nodes.add(node.nodeId);
        return nodes;
    }
}

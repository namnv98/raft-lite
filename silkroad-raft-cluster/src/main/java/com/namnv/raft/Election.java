package com.namnv.raft;

import com.namnv.entity.LogEntry;
import com.namnv.raft.state.LeaderState;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.RequestVoteRequest;
import com.namnv.rpc.model.request.TimeoutNowRequest;
import com.namnv.rpc.model.response.PreVoteResponse;
import com.namnv.rpc.model.response.RequestVoteResponse;
import com.namnv.rpc.model.response.TimeoutNowResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Bầu leader: pre-vote, bỏ phiếu, TimeoutNow, và các lần chuyển vai trò (follower, candidate, leader).
 * Phiếu bầu và term luôn nằm trên đĩa trước khi được gửi đi hay được tính.
 */
@Slf4j
final class Election {
    private final RaftNode node;

    // tăng mỗi lần election timeout hoặc nghe được leader, để bỏ qua pre-vote response của vòng cũ
    private long electionEpoch;
    // lần gần nhất nghe được leader hoặc vừa bỏ phiếu, dùng để từ chối pre-vote khi leader còn sống
    private long lastContactNanos;
    private boolean hadContact;

    // bộ đếm cho metrics()
    long electionsStarted;
    long timesElectedLeader;

    Election(RaftNode node) {
        this.node = node;
    }

    // ---------- phía nhận RPC ----------

    PreVoteResponse handlePreVoteRequest(PreVoteRequest request) {
        var response = preVote(request);
        // term mà node để lộ trong câu trả lời phải nằm trên đĩa trước
        try {
            node.persistent.syncVote();
        } catch (Exception e) {
            log.error("Node {} failed to persist term", node.nodeId, e);
            return new PreVoteResponse(response.term, false);
        }
        return response;
    }

    private PreVoteResponse preVote(PreVoteRequest request) {
        node.lock.lock();
        try {
            node.ensureRunning();
            var term = node.persistent.getCurrentTerm();
            var conf = node.membership.conf;
            if (!conf.contains(request.candidateId)) {
                log.warn("Node {} ignore PreVoteRequest from {} as it is not in conf <{}>.", node.nodeId, request.candidateId, conf);
                return new PreVoteResponse(term, false);
            }
            if (request.term < term) {
                return new PreVoteResponse(term, false);
            }
            // leader còn sống (hoặc vừa bỏ phiếu cho ai đó) thì không tiếp tay cho một cuộc bầu cử mới
            if (node.state == NodeState.LEADER || hasRecentContact()) {
                return new PreVoteResponse(term, false);
            }
            return new PreVoteResponse(term, isLogUpToDate(request.lastLogTerm, request.lastLogIndex));
        } finally {
            node.unlock();
        }
    }

    RequestVoteResponse handleRequestVoteRequest(RequestVoteRequest req) {
        var response = vote(req);
        // term và phiếu bầu phải nằm trên đĩa trước khi trả lời, ghi ngoài lock
        try {
            node.persistent.syncVote();
        } catch (Exception e) {
            log.error("Node {} failed to persist vote", node.nodeId, e);
            return new RequestVoteResponse(response.term, false);
        }
        if (!response.voteGranted) {
            return response;
        }
        node.lock.lock();
        try {
            if (node.stopped || node.persistent.getCurrentTerm() != response.term) {
                return new RequestVoteResponse(node.persistent.getCurrentTerm(), false);
            }
            return response;
        } finally {
            node.unlock();
        }
    }

    // quyết định phiếu bầu trong bộ nhớ, chưa ghi đĩa
    private RequestVoteResponse vote(RequestVoteRequest req) {
        node.lock.lock();
        try {
            node.ensureRunning();
            var persistent = node.persistent;
            if (!node.membership.conf.contains(req.candidateId) || req.term < persistent.getCurrentTerm()) {
                return new RequestVoteResponse(persistent.getCurrentTerm(), false);
            }
            if (req.term > persistent.getCurrentTerm()) {
                becomeFollower(req.term);
            }

            var votedFor = persistent.getVotedFor();
            var voteGranted = (votedFor == null || votedFor.equals(req.candidateId))
                    && isLogUpToDate(req.lastLogTerm, req.lastLogIndex);
            if (voteGranted) {
                persistent.setVotedFor(req.candidateId);
                markContact();
                node.electionTimer.reset();
            }
            return new RequestVoteResponse(persistent.getCurrentTerm(), voteGranted);
        } finally {
            node.unlock();
        }
    }

    // log của candidate có mới ít nhất bằng log của node này không
    private boolean isLogUpToDate(long candidateLastTerm, long candidateLastIndex) {
        var lastTerm = node.persistent.getLogStore().lastTerm();
        var lastIndex = node.persistent.getLogStore().lastIndex();
        return candidateLastTerm > lastTerm || (candidateLastTerm == lastTerm && candidateLastIndex >= lastIndex);
    }

    TimeoutNowResponse handleTimeoutNowRequest(TimeoutNowRequest req) {
        node.lock.lock();
        try {
            node.ensureRunning();
            var term = node.persistent.getCurrentTerm();
            if (req.term != term || node.state == NodeState.LEADER || !node.membership.conf.contains(node.nodeId)) {
                return new TimeoutNowResponse(term, false);
            }
            log.info("Node {} starts election on request of leader {}.", node.nodeId, req.leaderId);
            becomeCandidate(); // bỏ qua pre-vote: leader hiện tại đã chủ động nhường
            return new TimeoutNowResponse(req.term, true);
        } finally {
            node.unlock();
        }
    }

    // mọi RPC hợp lệ từ leader: chấp nhận leader đó và hoãn bầu cử
    void handleHeartbeat(long term, String leaderId) {
        if (term > node.persistent.getCurrentTerm() || node.state != NodeState.FOLLOWER) {
            becomeFollower(term); // luôn step-down khi nhận leader heartbeat
        }
        node.leaderId = leaderId;
        electionEpoch++;
        markContact();
        node.electionTimer.reset();
    }

    private void markContact() {
        hadContact = true;
        lastContactNanos = node.runtime.nanoTime();
    }

    private boolean hasRecentContact() {
        return hadContact && node.runtime.nanoTime() - lastContactNanos
                < TimeUnit.MILLISECONDS.toNanos(node.nodeOptions.getElectionTimeoutMinMs());
    }

    // ---------- phía ứng cử ----------

    void onElectionTimeout() {
        node.lock.lock();
        try {
            if (node.stopped || node.applier.failed || node.state == NodeState.LEADER) {
                return;
            }
            // luôn hẹn lại timer, nếu không một vòng bầu cử thất bại sẽ không bao giờ được thử lại
            node.electionTimer.reset();
            if (!node.membership.conf.contains(node.nodeId)) {
                return; // chưa thuộc cluster (đang chờ được thêm) hoặc đã bị gỡ
            }
            startPreVote();
        } finally {
            node.unlock();
        }
    }

    // hỏi trước xem có thắng được không, để một node bị cô lập không làm tăng term của cả cluster
    private void startPreVote() {
        var epoch = ++electionEpoch;
        var term = node.persistent.getCurrentTerm();
        var granted = node.membership.selfOnly();
        if (node.membership.conf.hasQuorum(granted)) {
            becomeCandidate(); // cluster một node
            return;
        }
        var logStore = node.persistent.getLogStore();
        var req = new PreVoteRequest(term, node.nodeId, logStore.lastIndex(), logStore.lastTerm());
        for (var peer : node.membership.peers()) {
            node.rpcProcessor.preVote(peer, req).whenComplete((response, error) -> {
                if (response == null) {
                    return;
                }
                node.onNode(() -> onPreVoteResponse(peer, response, epoch, term, granted));
            });
        }
    }

    private void onPreVoteResponse(String peer, PreVoteResponse response, long epoch, long term, Set<String> granted) {
        if (node.stopped || node.state == NodeState.LEADER) {
            return;
        }
        if (response.term > node.persistent.getCurrentTerm()) {
            becomeFollower(response.term);
            return;
        }
        // response của vòng pre-vote cũ
        if (electionEpoch != epoch || node.persistent.getCurrentTerm() != term) {
            return;
        }
        log.info("Node {} received PreVoteResponse from {}, term={}, granted={}.", node.nodeId, peer, response.term, response.voteGranted);
        // candidate thua vòng trước cũng phải được bầu lại, không chỉ follower
        if (response.voteGranted && granted.add(peer) && node.membership.conf.hasQuorum(granted)) {
            becomeCandidate();
        }
    }

    void becomeFollower(long term) {
        var persistent = node.persistent;
        var changedTerm = false;
        if (term > persistent.getCurrentTerm()) {
            persistent.setTermAndVote(term, null);
            node.runIo(persistent::syncVote);
            changedTerm = true;
        }
        if (node.state != NodeState.FOLLOWER || changedTerm) {
            log.info(node.nodeId + " current state " + node.state + " becomes FOLLOWER at term " + persistent.getCurrentTerm());
            node.state = NodeState.FOLLOWER;
            node.leaderState = null;
            node.leaderId = null;
            node.heartbeatTimer.stop();
            node.electionTimer.reset();
            node.failPending();
        }
    }

    private void becomeCandidate() {
        if (node.state == NodeState.LEADER) {
            return; // tránh race khi heartbeat tới đúng lúc
        }
        var persistent = node.persistent;
        node.state = NodeState.CANDIDATE;
        electionsStarted++;
        node.leaderId = null;
        persistent.setTermAndVote(persistent.getCurrentTerm() + 1, node.nodeId);
        // đang tự ứng cử thì không ủng hộ pre-vote của node khác
        markContact();
        node.electionTimer.reset();

        var termStarted = persistent.getCurrentTerm();
        // phiếu tự bầu phải nằm trên đĩa trước khi được tính hay gửi đi
        node.runIo(() -> {
            persistent.syncVote();
            node.onNode(() -> {
                if (!node.stopped && node.state == NodeState.CANDIDATE && persistent.getCurrentTerm() == termStarted) {
                    requestVotes(termStarted);
                }
            });
        });
    }

    private void requestVotes(long termStarted) {
        var granted = node.membership.selfOnly();
        if (node.membership.conf.hasQuorum(granted)) {
            becomeLeader(); // cluster một node
            return;
        }
        var logStore = node.persistent.getLogStore();
        var req = new RequestVoteRequest(termStarted, node.nodeId, logStore.lastIndex(), logStore.lastTerm());
        for (String peer : node.membership.peers()) {
            node.rpcProcessor.requestVote(peer, req).whenComplete((resp, error) -> {
                if (resp == null) {
                    return;
                }
                node.onNode(() -> onVoteResponse(peer, resp, termStarted, granted));
            });
        }
    }

    private void onVoteResponse(String peer, RequestVoteResponse resp, long termStarted, Set<String> granted) {
        if (node.stopped) {
            return;
        }
        if (resp.term > node.persistent.getCurrentTerm()) {
            becomeFollower(resp.term);
            return;
        }
        if (node.state != NodeState.CANDIDATE || node.persistent.getCurrentTerm() != termStarted) {
            return;
        }
        if (resp.voteGranted && granted.add(peer) && node.membership.conf.hasQuorum(granted)) {
            becomeLeader();
        }
    }

    private void becomeLeader() {
        var persistent = node.persistent;
        var logStore = persistent.getLogStore();
        node.state = NodeState.LEADER;
        node.leaderId = node.nodeId;
        node.leaderState = new LeaderState(node.membership.peers(), logStore.lastIndex() + 1, node.runtime.nanoTime());
        node.electionTimer.stop();
        node.membership.restoreDepartingNodes();
        // thời gian của entry mới không nhỏ hơn entry cuối trong log (kể cả entry do leader cũ tạo), hay của snapshot nếu
        // log đã bị compact hết vào đó
        node.lastTimestamp = Math.max(node.lastTimestamp, node.timestampAt(logStore.lastIndex()));
        // no-op của term mới: entry của term cũ chỉ được commit gián tiếp qua entry của term hiện tại
        try {
            logStore.appendEntry(node.stamped(new LogEntry(logStore.lastIndex() + 1, persistent.getCurrentTerm(), null)));
        } catch (Exception e) {
            // leader không ghi được log thì không làm leader được: nhường để node khác bầu lại
            log.error("Leader {} failed to append its no-op entry, step down", node.nodeId, e);
            becomeFollower(persistent.getCurrentTerm());
            return;
        }
        timesElectedLeader++;
        log.info("Leader " + node.nodeId + " elected at term " + persistent.getCurrentTerm());
        node.heartbeatTimer.start();
        node.replicator.broadcast();
    }
}

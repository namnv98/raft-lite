package com.namnv.raft;

import com.namnv.entity.ClientSession;
import com.namnv.entity.CommandBatch;
import com.namnv.entity.ConfigurationEntry;
import com.namnv.entity.LogEntry;
import com.namnv.raft.config.NodeOptions;
import com.namnv.raft.runtime.ElectionTimer;
import com.namnv.raft.runtime.HeartbeatTimer;
import com.namnv.raft.runtime.RaftRuntime;
import com.namnv.raft.runtime.ThreadedRuntime;
import com.namnv.raft.state.LeaderState;
import com.namnv.raft.state.PersistentState;
import com.namnv.rpc.RaftServerService;
import com.namnv.rpc.RpcProcessor;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.InstallSnapshotRequest;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.ReadIndexRequest;
import com.namnv.rpc.model.request.RequestVoteRequest;
import com.namnv.rpc.model.request.TimeoutNowRequest;
import com.namnv.rpc.model.response.AppendEntriesResponse;
import com.namnv.rpc.model.response.InstallSnapshotResponse;
import com.namnv.rpc.model.response.PreVoteResponse;
import com.namnv.rpc.model.response.ReadIndexResponse;
import com.namnv.rpc.model.response.RequestVoteResponse;
import com.namnv.rpc.model.response.TimeoutNowResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Một node Raft. Lớp này giữ state dùng chung (term, vai trò, commit index...), vòng xử lý sự kiện một thread và API
 * công khai; từng phần của thuật toán nằm ở các thành phần cùng package:
 * <ul>
 * <li>{@link Election}: pre-vote, bỏ phiếu, chuyển vai trò</li>
 * <li>{@link Replicator}: phía leader gửi log và snapshot, heartbeat, commit</li>
 * <li>{@link FollowerLog}: phía follower nhận và ghi log</li>
 * <li>{@link ClientCommands} và {@link Applier}: lệnh của client, apply vào state machine, chống trùng</li>
 * <li>{@link LinearizableReads}: đọc nhất quán theo ReadIndex</li>
 * <li>{@link Membership}: cấu hình cluster, joint consensus, learner</li>
 * <li>{@link Snapshots}: tạo, nhận và khôi phục snapshot</li>
 * </ul>
 * Mọi thay đổi state chạy trên thread của node (xem {@link #onNode}) khi đang giữ {@link #lock}; các thành phần gọi
 * nhau trực tiếp, không có khoá riêng.
 */
@Slf4j
public class RaftNode implements RaftServerService {

    final NodeOptions nodeOptions;
    final RpcProcessor rpcProcessor;
    final PersistentState persistent;
    final String nodeId;
    final StateMachine stateMachine;
    // đồng hồ, hẹn giờ và thread ghi đĩa; mọi thao tác ghi đĩa chạy qua runtime.executeIo, ngoài lock của node
    final RaftRuntime runtime;

    // null cho tới khi start()
    volatile NodeState state;
    volatile boolean stopped;
    volatile String leaderId;
    LeaderState leaderState;
    // chỉ thread của node ghi; volatile để metrics và test đọc được từ thread khác
    volatile long commitIndex;
    volatile long lastApplied;
    // thời điểm của entry gần nhất leader này tạo hoặc thấy trong log (xem stamped)
    long lastTimestamp;

    final ElectionTimer electionTimer;
    final HeartbeatTimer heartbeatTimer;

    final ReentrantLock lock = new ReentrantLock();
    // Kết quả của các future trả cho người gọi. Chúng được quyết định khi node đang giữ lock nhưng chỉ được báo sau khi
    // nhả lock, theo đúng thứ tự: callback người gọi gắn vào future (ghi mạng, tính toán) không được chặn cả node.
    private final ConcurrentLinkedQueue<Runnable> completions = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean completing = new AtomicBoolean();
    // Hàng sự kiện của node: lệnh client, response RPC, timer, việc đĩa đã xong. Thread của node (runtime.executeNode)
    // xử lý cả hàng trong một lần giữ lock, nên mọi thay đổi state chạy lần lượt trên một thread; lock chỉ còn để các
    // lời gọi hiếm (đổi thành viên, snapshot, RPC bầu cử) và test chen vào an toàn.
    private final ConcurrentLinkedQueue<Runnable> inbox = new ConcurrentLinkedQueue<>();
    // số sự kiện đã xếp hàng mà thread của node chưa ghi nhận; 0 nghĩa là không có lượt xử lý nào đang chạy
    private final AtomicInteger inboxWaiting = new AtomicInteger();
    // đang xử lý một lô sự kiện: các lệnh mới chỉ đánh dấu broadcastDeferred, cuối lô gửi follower một lần cho cả lô
    private boolean draining;
    private boolean broadcastDeferred;

    final Membership membership;
    final Election election;
    final Replicator replicator;
    final FollowerLog followerLog;
    final ClientCommands commands;
    final Applier applier;
    final LinearizableReads reads;
    final Snapshots snapshots;

    public RaftNode(NodeOptions nodeOptions, RpcProcessor rpcProcessor) {
        this.rpcProcessor = rpcProcessor;
        this.nodeOptions = nodeOptions;
        this.nodeId = nodeOptions.getRaftConfig().getSelf();
        this.persistent = new PersistentState(nodeOptions);
        this.stateMachine = nodeOptions.getStateMachine();
        this.runtime = nodeOptions.getRuntime() != null ? nodeOptions.getRuntime() : new ThreadedRuntime();
        this.membership = new Membership(this);
        this.election = new Election(this);
        this.replicator = new Replicator(this);
        this.followerLog = new FollowerLog(this);
        this.commands = new ClientCommands(this);
        this.applier = new Applier(this);
        this.reads = new LinearizableReads(this);
        this.snapshots = new Snapshots(this);
        this.electionTimer = new ElectionTimer(runtime, nodeOptions.getElectionTimeoutMinMs(), nodeOptions.getElectionTimeoutMaxMs(),
                () -> onNode(election::onElectionTimeout));
        this.heartbeatTimer = new HeartbeatTimer(runtime, nodeOptions.getHeartbeatIntervalMs(),
                () -> onNode(replicator::onHeartbeatTick));
    }

    // ---------- LIFECYCLE ----------

    public void start() {
        lock.lock();
        try {
            if (state != null || stopped) {
                return;
            }
            snapshots.restoreStateMachine();
            this.state = NodeState.FOLLOWER;
            electionTimer.start();
            runtime.schedule(() -> onNode(this::housekeeping), housekeepingIntervalMs());
        } finally {
            unlock();
        }
    }

    public void shutdown() {
        lock.lock();
        try {
            if (stopped) {
                return;
            }
            stopped = true;
            state = NodeState.FOLLOWER;
            leaderState = null;
            electionTimer.stop();
            heartbeatTimer.stop();
            runtime.shutdown();
            commands.failPending();
            reads.failAll();
            Exception flushFailure = null;
            try {
                persistent.flush();
                persistent.getLogStore().flush(); // với logSync = false: tắt bình thường thì không để lại gì chưa xuống đĩa
            } catch (Exception e) {
                log.error("Node {} failed to flush state on shutdown", nodeId, e);
                flushFailure = e;
            }
            followerLog.answerUnsynced(flushFailure);
            persistent.getLogStore().close();
        } finally {
            unlock();
        }
    }

    // nhả lock; lần nhả ngoài cùng báo các kết quả đã được quyết định trong lúc giữ lock
    void unlock() {
        lock.unlock();
        if (!lock.isHeldByCurrentThread()) {
            runCompletions();
        }
    }

    // chuyển một sự kiện cho thread của node
    void onNode(Runnable task) {
        inbox.add(task);
        if (inboxWaiting.getAndIncrement() == 0) {
            try {
                runtime.executeNode(this::drainInbox);
            } catch (RejectedExecutionException e) {
                // runtime đã tắt: xử lý ngay trên thread này, các sự kiện tự thấy node đã dừng và báo lỗi cho người chờ
                drainInbox();
            }
        }
    }

    private void drainInbox() {
        int seen = 1;
        while (true) {
            if (!inbox.isEmpty()) {
                lock.lock();
                try {
                    draining = true;
                    Runnable task;
                    while ((task = inbox.poll()) != null) {
                        try {
                            task.run();
                        } catch (Throwable t) {
                            log.error("Node {} failed to handle an event", nodeId, t);
                        }
                    }
                } finally {
                    draining = false;
                    if (broadcastDeferred) {
                        broadcastDeferred = false;
                        replicator.broadcast();
                    }
                    unlock();
                }
            }
            // sự kiện xếp hàng trong lúc đang xử lý không khởi động lượt mới, nên phải quay lại lấy chúng
            seen = inboxWaiting.addAndGet(-seen);
            if (seen == 0) {
                return;
            }
        }
    }

    // leader vừa append: gửi cho follower ngay, hoặc một lần ở cuối lô sự kiện đang xử lý
    void broadcastSoon() {
        if (draining) {
            broadcastDeferred = true;
        } else {
            replicator.broadcast();
        }
    }

    // mỗi lúc một thread báo kết quả, để các future hoàn tất theo đúng thứ tự được quyết định
    private void runCompletions() {
        while (!completions.isEmpty() && completing.compareAndSet(false, true)) {
            try {
                Runnable completion;
                while ((completion = completions.poll()) != null) {
                    try {
                        completion.run();
                    } catch (Throwable t) {
                        log.error("Node {} failed to report a result", nodeId, t);
                    }
                }
            } finally {
                completing.set(false);
            }
        }
    }

    // gọi khi đang giữ lock: future được hoàn tất ngay sau khi lock được nhả
    <T> void complete(CompletableFuture<T> future, T value) {
        completions.add(() -> future.complete(value));
    }

    void fail(CompletableFuture<?> future, Throwable cause) {
        completions.add(() -> future.completeExceptionally(cause));
    }

    // RPC tới node chưa start hoặc đã tắt bị coi như lỗi mạng
    void ensureRunning() {
        if (state == null || stopped) {
            throw new IllegalStateException("Node " + nodeId + " is not running");
        }
    }

    void runIo(Runnable task) {
        if (stopped) {
            return;
        }
        runtime.executeIo(() -> {
            try {
                task.run();
            } catch (Exception e) {
                log.error("Node {} disk operation failed", nodeId, e);
            }
        });
    }

    // node mất quyền leader: báo cho các lệnh và lần đọc đang chờ quyền đó
    void failPending() {
        commands.failPending();
        reads.failPendingConfirmations();
    }

    /**
     * Chạy định kỳ để báo "không rõ kết quả" cho các lệnh và lần đọc đã chờ quá clientTimeoutMs.
     * Một nhịp quét chung thay cho một timer riêng cho từng thao tác: timer riêng giữ mỗi thao tác sống trong bộ nhớ
     * tới hết thời hạn dù nó đã xong từ lâu, và ở tải cao chính lượng rác sống lâu đó làm GC dừng lâu.
     */
    private void housekeeping() {
        lock.lock();
        try {
            if (stopped) {
                return;
            }
            var now = runtime.nanoTime();
            commands.expire(now);
            reads.expire(now);
        } finally {
            unlock();
        }
        runtime.schedule(() -> onNode(this::housekeeping), housekeepingIntervalMs());
    }

    private long housekeepingIntervalMs() {
        return Math.max(10, Math.min(100, nodeOptions.getClientTimeoutMs() / 10));
    }

    /**
     * Gắn thời điểm leader tạo entry: giờ thực của runtime, nhưng không bao giờ nhỏ hơn entry trước đó trong log (đồng hồ
     * lùi, hoặc leader mới có đồng hồ chậm hơn leader cũ), nên thời gian không giảm dọc theo log.
     */
    LogEntry stamped(LogEntry entry) {
        long now = runtime.currentTimeMillis();
        if (now > lastTimestamp) {
            lastTimestamp = now;
        }
        entry.setTimestamp(lastTimestamp);
        return entry;
    }

    // thời điểm của entry tại index: từ log, hoặc từ snapshot nếu entry đã bị compact vào đó; 0 nếu không biết
    long timestampAt(long index) {
        var logStore = persistent.getLogStore();
        if (index > logStore.getBaseIndex()) {
            try {
                LogEntry entry = logStore.get(index);
                if (entry != null) {
                    return entry.getTimestamp();
                }
            } catch (RuntimeException e) {
                log.warn("Node {} could not read log entry {} for its timestamp", nodeId, index, e);
            }
        }
        var meta = persistent.getSnapshotStore().getMeta();
        return meta != null && meta.getLastIncludedIndex() >= index ? meta.getLastIncludedTimestamp() : 0;
    }

    // -1 nếu index không còn trong log (đã compact, hoặc chưa tới)
    long termAt(long index) {
        var logStore = persistent.getLogStore();
        if (index == logStore.getBaseIndex()) {
            return logStore.getBaseTerm();
        }
        var entry = logStore.get(index);
        return entry == null ? -1 : entry.getTerm();
    }

    // ---------- CLIENT ----------

    /**
     * Ghi một lệnh, không chống ghi trùng: nếu client gửi lại sau khi không nhận được kết quả, lệnh có thể được apply hai lần.
     */
    public CompletableFuture<Boolean> appendClientCommand(byte[] command) {
        return appendClientCommand(null, 0, command);
    }

    /**
     * Ghi một lệnh có định danh. Mỗi client dùng một clientId cố định, sequence bắt đầu từ 1 và tăng liền nhau;
     * khi không biết kết quả (false, timeout) thì gửi lại đúng (clientId, sequence) đó, ở bất kỳ leader nào.
     * Lệnh được apply nhiều nhất một lần dù được gửi bao nhiêu lần.
     * Client có thể gửi nhiều lệnh cùng lúc mà không cần chờ lệnh trước.
     * <p>
     * Future được hoàn tất bởi một thread của Raft sau khi nó đã nhả lock của node, nên callback gắn vào bằng
     * {@code thenApply}/{@code whenComplete} không chặn node. Các future được hoàn tất lần lượt theo thứ tự commit,
     * vì vậy một callback chậm vẫn làm kết quả của các lệnh sau nó đến trễ; việc nặng (ghi mạng, ghi đĩa) nên dùng
     * các biến thể {@code ...Async}.
     */
    public CompletableFuture<Boolean> appendClientCommand(String clientId, long sequence, byte[] command) {
        return appendClientCommand(clientId, sequence, command, false);
    }

    /**
     * Ghi nhiều lệnh trong một entry của log. Cả lô được commit và apply cùng nhau, lần lượt từng lệnh qua
     * {@link StateMachine#onApply} (các lệnh của một lô có chung index), và được chống ghi trùng
     * như một lệnh theo (clientId, sequence). Chi phí của Raft cho mỗi entry được chia cho mọi lệnh trong lô, nên khi
     * client có nhiều lệnh cùng lúc thì gom lại thế này nhanh hơn nhiều so với gửi từng lệnh.
     */
    public CompletableFuture<Boolean> appendClientBatch(String clientId, long sequence, List<byte[]> commands) {
        return appendClientCommand(clientId, sequence, CommandBatch.encode(commands), true);
    }

    /** @param batch command là một lô đã mã hoá bằng {@link CommandBatch#encode} */
    public CompletableFuture<Boolean> appendClientCommand(String clientId, long sequence, byte[] command, boolean batch) {
        if (batch && !CommandBatch.isValid(command)) {
            return CompletableFuture.completedFuture(false);
        }
        var future = new CompletableFuture<Boolean>();
        onNode(() -> commands.accept(clientId, sequence, command, batch, future, null));
        return future;
    }

    /**
     * Ghi một lệnh (hoặc một lô, với {@code batch}) và nhận về kết quả mà state machine trả cho nó
     * ({@link StateMachine#onApplyWithResult}); với lô là các kết quả của từng lệnh gói bằng
     * {@link CommandBatch#encode}, lệnh không có kết quả thì là mảng rỗng. Future thất bại với
     * {@link UnknownOutcomeException} khi không biết lệnh đã được apply hay chưa.
     * <p>
     * Lệnh bị coi là trùng theo (clientId, sequence) thì không được apply lại và kết quả của lần đầu không còn: khi cần
     * kết quả, nên chống trùng bằng định danh nghiệp vụ của chính lệnh (ví dụ id của giao dịch) và để clientId null.
     */
    public CompletableFuture<byte[]> submit(String clientId, long sequence, byte[] command, boolean batch) {
        if (batch && !CommandBatch.isValid(command)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("malformed command batch"));
        }
        var future = new CompletableFuture<byte[]>();
        onNode(() -> commands.accept(clientId, sequence, command, batch, null, future));
        return future;
    }

    /**
     * Kết thúc phiên của một client: bảng chống trùng quên client đó trên mọi node. Gọi khi client sẽ không gửi lại
     * lệnh nào nữa; một lệnh cũ gửi lại sau thời điểm này sẽ được apply như lệnh mới.
     */
    public CompletableFuture<Boolean> closeClientSession(String clientId) {
        lock.lock();
        try {
            return commands.closeSession(clientId);
        } finally {
            unlock();
        }
    }

    /**
     * Đọc nhất quán (linearizable) theo cơ chế ReadIndex, không ghi gì vào log và không phụ thuộc đồng hồ.
     * Gọi được trên bất kỳ node nào:
     * <ul>
     * <li>Trên leader: ghi nhận commit index hiện tại (readIndex), xác nhận lại với đa số rằng mình vẫn là leader,
     * chờ state machine apply tới readIndex rồi chạy {@code query}.</li>
     * <li>Trên follower: hỏi leader readIndex (leader làm đúng bước xác nhận trên), chờ state machine của chính mình
     * apply tới đó rồi chạy {@code query} trên dữ liệu của follower.</li>
     * </ul>
     * Kết quả vì thế chứa mọi lệnh đã được xác nhận trước khi read() được gọi.
     * <p>
     * {@code query} chạy khi node đang giữ lock (không có lệnh nào được apply xen vào) nên cần nhanh.
     * Future thất bại với {@link NotLeaderException} nếu node không biết leader, không liên lạc được với leader,
     * hoặc leader mất quyền trong lúc chờ.
     */
    public <T> CompletableFuture<T> read(Supplier<T> query) {
        var future = new CompletableFuture<T>();
        onNode(() -> reads.read(query, future));
        return future;
    }

    public RaftMetrics metrics() {
        lock.lock();
        try {
            var logStore = persistent.getLogStore();
            return new RaftMetrics(nodeId, state, persistent.getCurrentTerm(), leaderId, commitIndex, lastApplied,
                    logStore.getBaseIndex() + 1, logStore.lastIndex(), commands.pendingCount(), reads.pendingCount(),
                    election.electionsStarted, election.timesElectedLeader, commands.commandsAccepted,
                    commands.commandsRejected, commands.duplicateCommands, reads.readsServed,
                    snapshots.snapshotsCreated, snapshots.snapshotsInstalled);
        } finally {
            unlock();
        }
    }

    // ---------- MEMBERSHIP ----------

    /**
     * Thêm một node. Node mới trước hết nhận log như một learner (chưa được tính vào quorum); khi nó bắt kịp,
     * leader mới bắt đầu thay đổi cấu hình bằng joint consensus.
     */
    public boolean onJoinPeerCluster(String newNodeId) {
        lock.lock();
        try {
            return membership.join(newNodeId);
        } finally {
            unlock();
        }
    }

    /**
     * Gỡ node khỏi cluster, cũng qua joint consensus. Leader có thể tự gỡ chính mình:
     * nó điều phối tới khi C(new) commit rồi trao quyền và step-down.
     */
    public boolean onLeavePeerCluster(String removedNodeId) {
        lock.lock();
        try {
            return membership.leave(removedNodeId);
        } finally {
            unlock();
        }
    }

    /**
     * Đổi danh sách thành viên sang {@code newNodes} trong một lần, thêm và gỡ bao nhiêu node cũng được.
     * Trả về true nếu leader nhận yêu cầu; thay đổi hoàn tất khi {@link #getConf()} hết joint và bằng danh sách mới.
     * Trả về false nếu node không phải leader, danh sách rỗng hoặc không đổi, hay một thay đổi khác đang dang dở.
     * <p>
     * Các node mới phải bắt kịp log trong {@code catchUpTimeoutMs}; quá hạn thì yêu cầu bị huỷ và cấu hình giữ nguyên.
     */
    public boolean changePeers(Collection<String> newNodes) {
        lock.lock();
        try {
            return membership.changePeers(newNodes);
        } finally {
            unlock();
        }
    }

    // ---------- SNAPSHOT ----------

    /**
     * Bắt đầu tạo snapshot tại lastApplied. Hàm trả về ngay; snapshot được commit và log được compact
     * sau đó ở thread IO của runtime.
     */
    public void createSnapshot() {
        snapshots.create();
    }

    // ---------- RPC HANDLERS ----------

    @Override
    public PreVoteResponse handlePreVoteRequest(PreVoteRequest request) {
        return election.handlePreVoteRequest(request);
    }

    @Override
    public RequestVoteResponse handleRequestVoteRequest(RequestVoteRequest req) {
        return election.handleRequestVoteRequest(req);
    }

    @Override
    public TimeoutNowResponse handleTimeoutNowRequest(TimeoutNowRequest req) {
        return election.handleTimeoutNowRequest(req);
    }

    @Override
    public AppendEntriesResponse handleAppendEntriesRequest(AppendEntriesRequest req) {
        return followerLog.handleAppendEntriesRequest(req);
    }

    /**
     * Bản bất đồng bộ dùng bởi transport: ghi vào log trên thread của node rồi gom mọi AppendEntries đến trong lúc đĩa
     * đang bận vào một lần fsync chung ở thread IO; không thread nào phải đứng chờ đĩa.
     */
    @Override
    public CompletableFuture<AppendEntriesResponse> handleAppendEntriesAsync(AppendEntriesRequest req) {
        var future = new CompletableFuture<AppendEntriesResponse>();
        onNode(() -> followerLog.appendAsync(req, future));
        return future;
    }

    @Override
    public InstallSnapshotResponse handleInstallSnapshotRequest(InstallSnapshotRequest req) {
        return snapshots.handleInstallSnapshotRequest(req);
    }

    @Override
    public CompletableFuture<ReadIndexResponse> handleReadIndexRequest(ReadIndexRequest req) {
        var future = new CompletableFuture<ReadIndexResponse>();
        onNode(() -> reads.handleReadIndexRequest(future));
        return future;
    }

    // ---------- STATE ----------

    public String getNodeId() {
        return nodeId;
    }

    public NodeState getState() {
        return state;
    }

    public boolean isStopped() {
        return stopped;
    }

    public String getLeaderId() {
        return leaderId;
    }

    /** cấu hình đang có hiệu lực: config entry mới nhất trong log, kể cả khi chưa commit */
    public ConfigurationEntry getConf() {
        return membership.conf;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    // cho test cùng package

    PersistentState getPersistent() {
        return persistent;
    }

    LeaderState getLeaderState() {
        return leaderState;
    }

    ReentrantLock getLock() {
        return lock;
    }

    Map<String, ClientSession> getSessions() {
        return applier.sessions;
    }
}

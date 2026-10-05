package com.namnv.core;

import com.namnv.config.NodeOptions;
import com.namnv.entity.ClientSession;
import com.namnv.entity.ConfigurationEntry;
import com.namnv.entity.LogEntry;
import com.namnv.rpc.RaftServerService;
import com.namnv.rpc.client.RpcProcessor;
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
import com.namnv.state.LeaderState;
import com.namnv.state.PersistentState;
import com.namnv.state.VolatileState;
import com.namnv.statemachine.StateMachine;
import com.namnv.statemachine.snapshot.SnapshotMeta;
import com.namnv.statemachine.snapshot.SnapshotReader;
import com.namnv.statemachine.snapshot.SnapshotWriter;
import com.namnv.timer.ElectionTimer;
import com.namnv.timer.HeartbeatTimer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Getter
public class RaftNode implements RaftServerService {

    private final NodeOptions nodeOptions;
    private final RpcProcessor rpcProcessor;

    private final PersistentState persistent;
    private final VolatileState volatileState = new VolatileState();

    private final String nodeId;
    // null cho tới khi start()
    private volatile NodeState state;
    private volatile boolean stopped;

    private volatile String leaderId;
    private LeaderState leaderState;

    private final ElectionTimer electionTimer;
    private final HeartbeatTimer heartbeatTimer;

    private final StateMachine stateMachine;

    private final ReentrantLock lock = new ReentrantLock();
    // Kết quả của các future trả cho người gọi. Chúng được quyết định khi node đang giữ lock nhưng chỉ được báo sau khi
    // nhả lock, theo đúng thứ tự: callback người gọi gắn vào future (ghi mạng, tính toán) không được chặn cả node.
    private final ConcurrentLinkedQueue<Runnable> completions = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean completing = new AtomicBoolean();

    // cấu hình ban đầu, chỉ dùng khi log và snapshot chưa có config entry nào
    private final ConfigurationEntry initialConf;
    // cấu hình đang có hiệu lực = config entry mới nhất trong log (kể cả chưa commit)
    private volatile ConfigurationEntry conf;
    private long confIndex;
    // peers() của cấu hình peersOf
    private ConfigurationEntry peersOf;
    private List<String> peers = List.of();

    // tăng mỗi lần election timeout hoặc nghe được leader, để bỏ qua pre-vote response của vòng cũ
    private long electionEpoch;
    // lần gần nhất nghe được leader hoặc vừa bỏ phiếu, dùng để từ chối pre-vote khi leader còn sống
    private long lastContactNanos;
    private boolean hadContact;

    // lệnh đang chờ commit, theo thứ tự index (cũng là thứ tự hết hạn)
    private final ArrayDeque<PendingCommand> pendingFutures = new ArrayDeque<>();
    // các yêu cầu đọc đang chờ leader xác nhận lại quyền và apply tới readIndex
    private final ArrayDeque<PendingRead> pendingReads = new ArrayDeque<>();
    // các yêu cầu đọc đã có readIndex, đang chờ state machine của node này apply tới đó
    private final List<AppliedWaiter> appliedWaiters = new ArrayList<>();
    // clientId -> sequence lớn nhất đã apply; là một phần của state được replicate nên đi kèm snapshot
    private final Map<String, ClientSession> sessions = new HashMap<>();

    // bộ đếm cho metrics(), tính từ lúc node khởi động
    private long electionsStarted;
    private long timesElectedLeader;
    private long commandsAccepted;
    private long commandsRejected;
    private long duplicateCommands;
    private long readsServed;
    private long snapshotsCreated;
    private long snapshotsInstalled;
    // đồng hồ, hẹn giờ và thread ghi đĩa; mọi thao tác ghi đĩa chạy qua runtime.executeIo, ngoài lock của node
    private final RaftRuntime runtime;

    // đang tạo hoặc cài snapshot: thư mục temp của snapshot store chỉ dùng được cho một việc
    private boolean snapshotting;
    // state machine đang load snapshot ngoài lock, tạm hoãn apply
    private boolean loadingSnapshot;
    private boolean commitIndexFlushScheduled;
    private boolean logSyncQueued;
    // follower: các lần đọc chưa được hỏi readIndex, và nhóm đang chờ leader trả lời (null nếu không có request nào đang bay)
    private List<QueuedRead> queuedReads = new ArrayList<>();
    private ReadIndexBatch readIndexBatch;
    // snapshot đang nhận dần từ leader; trong lúc đó cờ snapshotting cũng được bật
    private IncomingSnapshot incomingSnapshot;

    // node từng là thành viên; khi cấu hình không còn nó được commit thì node tự tắt
    private boolean wasMember;
    private boolean transferring;
    private boolean removalHandled;

    public RaftNode(NodeOptions nodeOptions, RpcProcessor rpcProcessor) {
        this.rpcProcessor = rpcProcessor;
        this.nodeOptions = nodeOptions;
        this.nodeId = nodeOptions.getRaftConfig().getSelf();
        this.persistent = new PersistentState(nodeOptions);
        this.stateMachine = nodeOptions.getStateMachine();
        this.runtime = nodeOptions.getRuntime() != null ? nodeOptions.getRuntime() : new ThreadedRuntime();
        this.electionTimer = new ElectionTimer(runtime, nodeOptions.getElectionTimeoutMinMs(), nodeOptions.getElectionTimeoutMaxMs(), this::onElectionTimeout);
        this.heartbeatTimer = new HeartbeatTimer(runtime, nodeOptions.getHeartbeatIntervalMs(), this::onHeartbeatTick);
        this.initialConf = new ConfigurationEntry(nodeOptions.getRaftConfig().getPeers());
        refreshConf();
    }

    // ---------- LIFECYCLE ----------

    public void start() {
        lock.lock();
        try {
            if (state != null || stopped) {
                return;
            }
            restoreStateMachine();
            this.state = NodeState.FOLLOWER;
            electionTimer.start();
            runtime.schedule(this::housekeeping, housekeepingIntervalMs());
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
            failPendingFutures();
            for (AppliedWaiter waiter : appliedWaiters) {
                waiter.onFailure().accept(new NotLeaderException(nodeId, null));
            }
            appliedWaiters.clear();
            sendReadIndex(); // node đã dừng: báo lỗi cho các lần đọc còn xếp hàng
            if (readIndexBatch != null) {
                readIndexBatch.fail(new NotLeaderException(nodeId, null));
                readIndexBatch = null;
            }
            try {
                persistent.flush();
                persistent.getLogStore().flush(); // với logSync = false: tắt bình thường thì không để lại gì chưa xuống đĩa
            } catch (Exception e) {
                log.error("Node {} failed to flush state on shutdown", nodeId, e);
            }
            persistent.getLogStore().close();
        } finally {
            unlock();
        }
    }

    private void unlock() {
        lock.unlock();
        if (!lock.isHeldByCurrentThread()) {
            runCompletions();
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
    private <T> void complete(CompletableFuture<T> future, T value) {
        completions.add(() -> future.complete(value));
    }

    private void fail(CompletableFuture<?> future, Throwable cause) {
        completions.add(() -> future.completeExceptionally(cause));
    }

    // RPC tới node chưa start hoặc đã tắt bị coi như lỗi mạng
    private void ensureRunning() {
        if (state == null || stopped) {
            throw new IllegalStateException("Node " + nodeId + " is not running");
        }
    }

    private void runIo(Runnable task) {
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

    private void failPendingFutures() {
        for (PendingCommand pending : pendingFutures) {
            complete(pending.future(), false);
        }
        pendingFutures.clear();
        var notLeader = new NotLeaderException(nodeId, leaderId);
        for (PendingRead read : pendingReads) {
            read.fail(notLeader);
        }
        pendingReads.clear();
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
        lock.lock();
        try {
            if (stopped || state != NodeState.LEADER) {
                return CompletableFuture.completedFuture(false);
            }
            if (isDuplicate(clientId, sequence)) {
                duplicateCommands++;
                return CompletableFuture.completedFuture(true); // lần gửi trước đã được apply
            }
            if (pendingFutures.size() >= nodeOptions.getMaxPendingCommands()) {
                // follower không theo kịp: từ chối sớm thay vì để hàng chờ lớn mãi
                commandsRejected++;
                return CompletableFuture.completedFuture(false);
            }
            long nextIndex = persistent.getLogStore().lastIndex() + 1;
            return appendAndTrack(new LogEntry(nextIndex, persistent.getCurrentTerm(), command, clientId, sequence));
        } finally {
            unlock();
        }
    }

    // leader ghi entry vào log và trả về future hoàn tất khi entry được apply (true) hoặc không rõ kết quả (false)
    private CompletableFuture<Boolean> appendAndTrack(LogEntry entry) {
        try {
            persistent.getLogStore().appendEntry(entry);
        } catch (Exception error) {
            log.error("Leader {} failed to append client command", nodeId, error);
            commandsRejected++;
            return CompletableFuture.completedFuture(false);
        }
        commandsAccepted++;
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        pendingFutures.addLast(new PendingCommand(entry.getIndex(), future, deadline()));
        // replicate log đến followers
        broadcast();
        return future;
    }

    private record PendingCommand(long index, CompletableFuture<Boolean> future, long deadlineNanos) {
    }

    private long deadline() {
        return runtime.nanoTime() + TimeUnit.MILLISECONDS.toNanos(nodeOptions.getClientTimeoutMs());
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
            // cả hai hàng đều xếp theo thứ tự đến, nên phần tử đầu luôn hết hạn sớm nhất
            while (!pendingFutures.isEmpty() && pendingFutures.peekFirst().deadlineNanos() - now <= 0) {
                complete(pendingFutures.pollFirst().future(), false); // timeout → fail client
            }
            while (!pendingReads.isEmpty() && pendingReads.peekFirst().deadlineNanos() - now <= 0) {
                pendingReads.pollFirst().fail(new TimeoutException("Read was not confirmed by a quorum in time"));
            }
            if (readIndexBatch != null && readIndexBatch.deadlineNanos() - now <= 0) {
                // leader không trả lời: không để các lần đọc đến sau chờ mãi sau request này
                var expired = readIndexBatch;
                readIndexBatch = null;
                expired.fail(new TimeoutException("Leader did not answer ReadIndex in time"));
                sendReadIndex();
            }
            var waiters = appliedWaiters.iterator();
            while (waiters.hasNext()) {
                var waiter = waiters.next();
                if (waiter.deadlineNanos() - now <= 0) {
                    waiters.remove();
                    waiter.onFailure().accept(new TimeoutException(
                            "State machine did not reach index " + waiter.readIndex() + " in time"));
                }
            }
        } finally {
            unlock();
        }
        runtime.schedule(this::housekeeping, housekeepingIntervalMs());
    }

    private long housekeepingIntervalMs() {
        return Math.max(10, Math.min(100, nodeOptions.getClientTimeoutMs() / 10));
    }

    private boolean isDuplicate(String clientId, long sequence) {
        var session = clientId == null ? null : sessions.get(clientId);
        return session != null && session.applied(sequence);
    }

    /**
     * Kết thúc phiên của một client: bảng chống trùng quên client đó trên mọi node. Gọi khi client sẽ không gửi lại
     * lệnh nào nữa; một lệnh cũ gửi lại sau thời điểm này sẽ được apply như lệnh mới.
     */
    public CompletableFuture<Boolean> closeClientSession(String clientId) {
        lock.lock();
        try {
            if (stopped || state != NodeState.LEADER) {
                return CompletableFuture.completedFuture(false);
            }
            var index = persistent.getLogStore().lastIndex() + 1;
            return appendAndTrack(LogEntry.newSessionClose(index, persistent.getCurrentTerm(), clientId));
        } finally {
            unlock();
        }
    }

    public RaftMetrics metrics() {
        lock.lock();
        try {
            var logStore = persistent.getLogStore();
            return new RaftMetrics(nodeId, state, persistent.getCurrentTerm(), leaderId,
                    volatileState.getCommitIndex(), volatileState.getLastApplied(),
                    logStore.getBaseIndex() + 1, logStore.lastIndex(), pendingFutures.size(), pendingReads.size(),
                    electionsStarted, timesElectedLeader, commandsAccepted, commandsRejected, duplicateCommands,
                    readsServed, snapshotsCreated, snapshotsInstalled);
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
        // query chạy trong lock, kết quả của nó được báo sau khi nhả lock
        Runnable runQuery = () -> {
            try {
                readsServed++;
                complete(future, query.get());
            } catch (Throwable t) {
                fail(future, t);
            }
        };
        Consumer<Throwable> onFailure = cause -> fail(future, cause);
        lock.lock();
        try {
            if (stopped || state == null) {
                onFailure.accept(new NotLeaderException(nodeId, leaderId));
            } else if (state == NodeState.LEADER) {
                confirmLeadership(readIndex -> awaitApplied(readIndex, runQuery, onFailure), onFailure);
            } else if (leaderId == null || leaderId.equals(nodeId)) {
                onFailure.accept(new NotLeaderException(nodeId, null));
            } else {
                queuedReads.add(new QueuedRead(runQuery, onFailure));
                if (readIndexBatch == null) {
                    sendReadIndex();
                }
            }
            return future;
        } finally {
            unlock();
        }
    }

    // một lần đọc trên follower đang chờ được hỏi readIndex
    private record QueuedRead(Runnable run, Consumer<Throwable> onFailure) {
    }

    // các lần đọc đi chung một ReadIndex đang chờ leader trả lời
    private record ReadIndexBatch(List<QueuedRead> reads, long deadlineNanos) {
        void fail(Throwable cause) {
            for (QueuedRead read : reads) {
                read.onFailure().accept(cause);
            }
        }
    }

    /**
     * Follower hỏi leader readIndex cho mọi lần đọc đang chờ bằng một request duy nhất. Lần đọc đến trong lúc request
     * đang bay phải chờ request kế tiếp: readIndex chỉ đúng cho những lần đọc bắt đầu trước khi request được gửi.
     * Nhờ gom lại, số request tới leader không tăng theo số lần đọc đồng thời.
     */
    private void sendReadIndex() {
        if (queuedReads.isEmpty()) {
            return;
        }
        var batch = new ReadIndexBatch(queuedReads, deadline());
        queuedReads = new ArrayList<>();
        if (stopped) {
            batch.fail(new NotLeaderException(nodeId, null));
        } else if (state == NodeState.LEADER) {
            // vừa được bầu trong lúc các lần đọc này còn chờ
            confirmLeadership(readIndex -> awaitApplied(readIndex, batch), batch::fail);
        } else if (leaderId == null || leaderId.equals(nodeId)) {
            batch.fail(new NotLeaderException(nodeId, null));
        } else {
            readIndexBatch = batch;
            rpcProcessor.readIndex(leaderId, new ReadIndexRequest(nodeId)).whenComplete((response, error) -> {
                lock.lock();
                try {
                    if (readIndexBatch != batch) {
                        return; // đã quá hạn và được báo lỗi
                    }
                    readIndexBatch = null;
                    if (stopped || response == null || !response.success) {
                        var hint = response != null && !nodeId.equals(response.leaderId) ? response.leaderId : null;
                        batch.fail(new NotLeaderException(nodeId, hint));
                    } else {
                        awaitApplied(response.readIndex, batch);
                    }
                    sendReadIndex();
                } finally {
                    unlock();
                }
            });
        }
    }

    private void awaitApplied(long readIndex, ReadIndexBatch batch) {
        for (QueuedRead read : batch.reads()) {
            awaitApplied(readIndex, read.run(), read.onFailure());
        }
    }

    @Override
    public CompletableFuture<ReadIndexResponse> handleReadIndexRequest(ReadIndexRequest req) {
        var future = new CompletableFuture<ReadIndexResponse>();
        lock.lock();
        try {
            ensureRunning();
            if (state != NodeState.LEADER) {
                future.complete(new ReadIndexResponse(false, 0, leaderId));
            } else {
                confirmLeadership(readIndex -> complete(future, new ReadIndexResponse(true, readIndex, nodeId)),
                        error -> complete(future, new ReadIndexResponse(false, 0, null)));
            }
            return future;
        } finally {
            unlock();
        }
    }

    // một yêu cầu đọc đang chờ leader xác nhận quyền: được đa số trả lời một request gửi sau thời điểm stamp
    private record PendingRead(long stamp, long readIndex, long deadlineNanos, LongConsumer onConfirmed,
                               Consumer<Throwable> onFailure) {
        void fail(Throwable cause) {
            onFailure.accept(cause);
        }
    }

    // việc cần làm khi state machine của node này apply tới readIndex
    private record AppliedWaiter(long readIndex, long deadlineNanos, Runnable run, Consumer<Throwable> onFailure) {
    }

    // leader: ghi nhận readIndex và bắt đầu một vòng heartbeat mới để chứng minh mình vẫn là leader sau thời điểm này
    private void confirmLeadership(LongConsumer onConfirmed, Consumer<Throwable> onFailure) {
        // leader mới chưa biết commit index của mình có đủ mới không cho tới khi no-op của nó commit,
        // nên readIndex không bao giờ nhỏ hơn index của no-op đó
        var readIndex = Math.max(volatileState.getCommitIndex(), leaderState.getTermStartIndex());
        pendingReads.addLast(new PendingRead(leaderState.nextStamp(), readIndex, deadline(), onConfirmed, onFailure));
        // chỉ response của request gửi sau thời điểm này mới xác nhận được quyền leader
        for (String peer : peers()) {
            replicateTo(peer);
        }
        completeReads(); // cluster một node không cần chờ ai
    }

    // báo cho các yêu cầu đọc đã được đa số xác nhận
    private void completeReads() {
        if (state != NodeState.LEADER || pendingReads.isEmpty()) {
            return;
        }
        var ackedStamp = leaderState.getAckedStamp();
        var confirmed = conf.quorumIndex(id -> id.equals(nodeId) ? Long.MAX_VALUE : ackedStamp.getOrDefault(id, 0L));
        // stamp tăng dần theo thứ tự đến, nên chỉ cần lấy dần từ đầu hàng
        while (!pendingReads.isEmpty() && pendingReads.peekFirst().stamp() <= confirmed) {
            var read = pendingReads.pollFirst();
            read.onConfirmed().accept(read.readIndex());
        }
    }

    // chạy run ngay nếu state machine đã apply tới readIndex, nếu không thì chờ
    private void awaitApplied(long readIndex, Runnable run, Consumer<Throwable> onFailure) {
        if (volatileState.getLastApplied() >= readIndex && !loadingSnapshot) {
            run.run();
            return;
        }
        appliedWaiters.add(new AppliedWaiter(readIndex, deadline(), run, onFailure));
    }

    private void runAppliedWaiters() {
        if (appliedWaiters.isEmpty()) {
            return;
        }
        var applied = volatileState.getLastApplied();
        var ready = new ArrayList<AppliedWaiter>();
        appliedWaiters.removeIf(waiter -> waiter.readIndex() <= applied && ready.add(waiter));
        for (AppliedWaiter waiter : ready) {
            waiter.run().run();
        }
    }

    // còn yêu cầu đọc nào mà peer này chưa xác nhận không
    private boolean awaitsReadConfirmation(LeaderState ls, String peer) {
        // yêu cầu đến sau cùng có stamp lớn nhất
        return !pendingReads.isEmpty() && pendingReads.peekLast().stamp() > ls.getAckedStamp().getOrDefault(peer, 0L);
    }

    // ---------- MEMBERSHIP ----------

    /**
     * Thêm một node. Node mới trước hết nhận log như một learner (chưa được tính vào quorum); khi nó bắt kịp,
     * leader mới bắt đầu thay đổi cấu hình bằng joint consensus.
     */
    public boolean onJoinPeerCluster(String newNodeId) {
        lock.lock();
        try {
            if (conf.contains(newNodeId)) {
                return false;
            }
            var newNodes = new ArrayList<>(conf.getOldNodes());
            newNodes.add(newNodeId);
            return changePeers(newNodes);
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
            var newNodes = new ArrayList<>(conf.getOldNodes());
            if (!newNodes.remove(removedNodeId)) {
                return false;
            }
            return changePeers(newNodes);
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
            var target = new ArrayList<>(new LinkedHashSet<>(newNodes));
            var current = conf.getOldNodes();
            if (stopped || state != NodeState.LEADER || target.isEmpty()
                    || (target.size() == current.size() && target.containsAll(current))) {
                return false;
            }
            // mỗi lần chỉ một thay đổi cấu hình, và cấu hình hiện tại phải commit xong
            if (conf.isJoint() || confIndex > volatileState.getCommitIndex() || leaderState.getPendingConf() != null) {
                log.warn("Node {} rejects change to {}: configuration change in progress <{}>.", nodeId, target, conf);
                return false;
            }
            var added = target.stream().filter(node -> !current.contains(node)).toList();
            if (added.isEmpty()) {
                return appendJointConf(target);
            }
            // node mới còn trống: đưa nó vào cấu hình ngay sẽ bắt cluster chờ nó mới commit được
            var now = runtime.nanoTime();
            leaderState.setPendingConf(target);
            leaderState.getCatchingUp().addAll(added);
            leaderState.setCatchUpDeadline(now + TimeUnit.MILLISECONDS.toNanos(nodeOptions.getCatchUpTimeoutMs()));
            log.info("Leader {} waits for {} to catch up before changing configuration to {}.", nodeId, added, target);
            for (var node : added) {
                leaderState.addPeer(node, persistent.getLogStore().lastIndex() + 1, now);
                replicateTo(node);
            }
            return true;
        } finally {
            unlock();
        }
    }

    private boolean appendJointConf(List<String> target) {
        try {
            appendConfiguration(new ConfigurationEntry(conf.getOldNodes(), target, true));
            return true;
        } catch (Exception e) {
            log.error("Leader {} failed to append configuration change", nodeId, e);
            return false;
        }
    }

    // một node mới vừa nhận thêm log: khi mọi node mới đều chỉ còn cách leader không quá một request thì bắt đầu đổi cấu hình
    private void onLearnerProgress(LeaderState ls, String peer, long match) {
        if (!ls.getCatchingUp().contains(peer)
                || persistent.getLogStore().lastIndex() - match > nodeOptions.getMaxEntriesPerRequest()) {
            return;
        }
        ls.getCatchingUp().remove(peer);
        if (ls.getCatchingUp().isEmpty()) {
            var target = ls.getPendingConf();
            ls.setPendingConf(null);
            appendJointConf(target);
        }
    }

    private void expirePendingConf() {
        if (leaderState.getPendingConf() != null && runtime.nanoTime() - leaderState.getCatchUpDeadline() > 0) {
            log.warn("Leader {} gives up changing configuration to {}: {} did not catch up in time.", nodeId,
                    leaderState.getPendingConf(), leaderState.getCatchingUp());
            leaderState.setPendingConf(null);
            leaderState.getCatchingUp().clear();
        }
    }

    private void appendConfiguration(ConfigurationEntry newConf) {
        var logStore = persistent.getLogStore();
        var index = logStore.lastIndex() + 1;
        logStore.appendEntry(LogEntry.newConfigurationEntry(index, persistent.getCurrentTerm(), newConf));
        refreshConf();
        for (var peer : peers()) {
            leaderState.addPeer(peer, index, runtime.nanoTime());
        }
        log.info("Leader {} appended configuration <{}> at index {}.", nodeId, newConf, index);
        broadcast();
    }

    // gọi sau mỗi lần commit: đẩy thay đổi cấu hình sang bước kế tiếp
    private void onConfCommitted() {
        if (state != NodeState.LEADER || confIndex > volatileState.getCommitIndex()) {
            return;
        }
        if (conf.isJoint()) {
            finalizeJointConf();
        } else if (!conf.contains(nodeId)) {
            handOverLeadership();
        }
    }

    // C(old,new) đã commit: ghi tiếp C(new)
    private void finalizeJointConf() {
        // node bị gỡ vẫn được gửi log tới khi nhận xong C(new), để nó biết mình đã rời cluster
        var finalIndex = persistent.getLogStore().lastIndex() + 1;
        for (var node : conf.getOldNodes()) {
            if (!conf.getNewNodes().contains(node) && !node.equals(nodeId)) {
                addDeparting(node, finalIndex);
            }
        }
        appendConfiguration(new ConfigurationEntry(conf.getNewNodes()));
    }

    // leader tự gỡ mình: trao quyền cho follower có log đầy đủ nhất thay vì để cluster chờ election timeout
    private void handOverLeadership() {
        var matchIndex = leaderState.getMatchIndex();
        var candidates = new ArrayList<>(peers());
        candidates.sort(Comparator.comparingLong((String peer) -> matchIndex.getOrDefault(peer, 0L)).reversed());
        var term = persistent.getCurrentTerm();
        log.info("Leader {} is no longer in configuration <{}>, step down.", nodeId, conf);
        becomeFollower(term);
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
        log.info("Node {} asks {} to take over leadership.", nodeId, target);
        rpcProcessor.timeoutNow(target, new TimeoutNowRequest(term, nodeId)).whenComplete((resp, error) -> {
            lock.lock();
            try {
                var accepted = resp != null && resp.success;
                if (accepted || stopped || persistent.getCurrentTerm() != term) {
                    transferring = false;
                    maybeShutdownRemoved();
                } else {
                    transferLeadership(candidates, position + 1, term);
                }
            } finally {
                unlock();
            }
        });
    }

    private void addDeparting(String node, long finalConfIndex) {
        // node đã chết hẳn thì không gửi mãi: bỏ sau một khoảng thời gian
        var deadline = runtime.nanoTime() + TimeUnit.MILLISECONDS.toNanos(nodeOptions.getDepartingTimeoutMs());
        leaderState.getDeparting().put(node, finalConfIndex);
        leaderState.getDepartingDeadline().put(node, deadline);
    }

    private void removeDeparting(LeaderState ls, String node) {
        ls.getDeparting().remove(node);
        ls.getDepartingDeadline().remove(node);
    }

    // leader trước có thể chết khi node bị gỡ chưa kịp nhận C(new): leader mới suy lại từ joint config ngay trước đó trong log
    private void restoreDepartingNodes() {
        var previousConfIndex = confIndexAt(confIndex - 1);
        if (conf.isJoint() || previousConfIndex == 0) {
            return;
        }
        var previous = persistent.getLogStore().get(previousConfIndex).getConfiguration();
        if (!previous.isJoint()) {
            return;
        }
        for (var node : previous.getOldNodes()) {
            if (!conf.contains(node) && !node.equals(nodeId)) {
                addDeparting(node, confIndex);
            }
        }
    }

    private void expireDepartingNodes() {
        var now = runtime.nanoTime();
        var deadlines = leaderState.getDepartingDeadline();
        for (var node : new ArrayList<>(deadlines.keySet())) {
            if (now - deadlines.get(node) > 0) {
                removeDeparting(leaderState, node);
            }
        }
    }

    /**
     * Node đã bị gỡ và cấu hình đó đã commit thì không còn việc gì để làm: tự shutdown.
     * Node mới chưa từng là thành viên (đang chờ được thêm vào) thì không tính.
     */
    private void maybeShutdownRemoved() {
        if (removalHandled || transferring || stopped || !wasMember || state == NodeState.LEADER) {
            return;
        }
        if (conf.isJoint() || conf.contains(nodeId) || confIndex > volatileState.getCommitIndex()) {
            return;
        }
        removalHandled = true;
        if (nodeOptions.isShutdownOnRemoved()) {
            log.info("Node {} was removed from the cluster <{}>, shutting down.", nodeId, conf);
            runIo(this::shutdown);
        } else {
            // vẫn chạy nhưng đứng yên: onElectionTimeout bỏ qua node không thuộc cấu hình
            log.info("Node {} was removed from the cluster <{}>.", nodeId, conf);
        }
    }

    // config entry mới nhất có index <= upTo; nếu đã bị compact thì lấy config lưu kèm snapshot
    private ConfigurationEntry confAt(long upTo) {
        var index = confIndexAt(upTo);
        if (index > 0) {
            return persistent.getLogStore().get(index).getConfiguration();
        }
        var snapshotConf = persistent.getSnapshotConf();
        return snapshotConf != null ? snapshotConf : initialConf;
    }

    // 0 nếu trong log không còn config entry nào <= upTo
    private long confIndexAt(long upTo) {
        return persistent.getLogStore().lastConfigurationIndex(upTo);
    }

    private void refreshConf() {
        var lastIndex = persistent.getLogStore().lastIndex();
        var index = confIndexAt(lastIndex);
        this.conf = confAt(lastIndex);
        this.confIndex = index > 0 ? index : (persistent.getSnapshotConf() != null ? persistent.getLastSnapshotIndex() : 0);
        if (conf.contains(nodeId)) {
            wasMember = true;
            removalHandled = false; // được thêm lại sau khi bị gỡ
        }
    }

    // danh sách chỉ đọc, được dựng lại khi cấu hình đổi: nó được hỏi cho từng lệnh của client
    private List<String> peers() {
        var current = conf;
        if (peersOf != current) {
            var others = new ArrayList<>(current.allNodes());
            others.remove(nodeId);
            peers = List.copyOf(others);
            peersOf = current;
        }
        return peers;
    }

    // mọi node leader cần gửi log: thành viên hiện tại, node đang rời đi và node mới đang bắt kịp
    private Collection<String> replicationTargets() {
        if (leaderState.getDeparting().isEmpty() && leaderState.getCatchingUp().isEmpty()) {
            return peers();
        }
        Set<String> targets = new LinkedHashSet<>(peers());
        targets.addAll(leaderState.getDeparting().keySet());
        targets.addAll(leaderState.getCatchingUp());
        return targets;
    }

    // tập phiếu ban đầu của một lần đếm quorum: node luôn tính chính nó
    private Set<String> selfOnly() {
        Set<String> nodes = new HashSet<>();
        nodes.add(nodeId);
        return nodes;
    }

    // ---------- SNAPSHOT ----------

    private void restoreStateMachine() {
        var snapshotStore = persistent.getSnapshotStore();
        var meta = snapshotStore.getMeta();
        if (meta != null) {
            if (!stateMachine.onSnapshotLoad(new SnapshotReader(snapshotStore.currentPath()))) {
                throw new IllegalStateException("Node " + nodeId + " failed to load snapshot at index " + meta.getLastIncludedIndex());
            }
            volatileState.setLastApplied(meta.getLastIncludedIndex());
            volatileState.setCommitIndex(meta.getLastIncludedIndex());
            restoreSessions(meta.getSessions());
        }

        // Apply các log sau snapshot
        var commitIndex = Math.min(persistent.getLastCommitIndex(), persistent.getLogStore().lastIndex());
        if (commitIndex > volatileState.getCommitIndex()) {
            volatileState.setCommitIndex(commitIndex);
            applyCommitted();
        }
    }

    private void restoreSessions(Map<String, ClientSession> snapshotSessions) {
        sessions.clear();
        if (snapshotSessions != null) {
            sessions.putAll(copySessions(snapshotSessions));
        }
    }

    private static Map<String, ClientSession> copySessions(Map<String, ClientSession> source) {
        Map<String, ClientSession> copy = new HashMap<>();
        for (var session : source.entrySet()) {
            copy.put(session.getKey(), new ClientSession(session.getValue()));
        }
        return copy;
    }

    /**
     * Bắt đầu tạo snapshot tại lastApplied. Hàm trả về ngay; snapshot được commit và log được compact
     * sau đó ở thread IO của runtime.
     */
    public void createSnapshot() {
        lock.lock();
        try {
            abortStaleIncomingSnapshot();
            if (stopped || snapshotting) {
                return;
            }
            var snapshotStore = persistent.getSnapshotStore();
            var snapshotIndex = volatileState.getLastApplied();
            if (snapshotIndex <= persistent.getLogStore().getBaseIndex()) {
                return;
            }
            var meta = new SnapshotMeta(snapshotIndex, termAt(snapshotIndex), confAt(snapshotIndex),
                    new ArrayList<>(), copySessions(sessions));
            snapshotting = true;
            try {
                // state machine ghi vào thư mục temp, chỉ khi commit() mới thay thế snapshot hiện tại
                var writer = new SnapshotWriter(snapshotStore.prepareTemp(), meta.getFiles());
                stateMachine.onSnapshotSave(writer, status -> {
                    if (status.isOk()) {
                        runIo(() -> finishSnapshot(meta));
                    } else {
                        log.error("Snapshot failed: {}", status.getMsg());
                        endSnapshotting();
                    }
                });
            } catch (Exception e) {
                snapshotting = false;
                log.error("Node {} failed to create snapshot", nodeId, e);
            }
        } finally {
            unlock();
        }
    }

    // leader chết giữa lúc gửi snapshot thì lần nhận dở không bao giờ xong: bỏ nó để node tự tạo snapshot được
    private void abortStaleIncomingSnapshot() {
        var stale = TimeUnit.MILLISECONDS.toNanos(10L * nodeOptions.getElectionTimeoutMaxMs());
        if (incomingSnapshot != null && !incomingSnapshot.busy
                && runtime.nanoTime() - incomingSnapshot.lastChunkNanos > stale) {
            abortIncomingSnapshot();
        }
    }

    // chạy ở thread IO: ghi meta + rename ngoài lock, sau đó mới bỏ phần log đã nằm trong snapshot
    private void finishSnapshot(SnapshotMeta meta) {
        try {
            persistent.getSnapshotStore().commit(meta);
        } catch (IOException e) {
            endSnapshotting();
            throw new UncheckedIOException(e);
        }
        lock.lock();
        try {
            snapshotting = false;
            snapshotsCreated++;
            if (!stopped) {
                persistent.getLogStore().truncatePrefix(meta.getLastIncludedIndex() + 1);
                cleanupLog();
            }
        } finally {
            unlock();
        }
    }

    // Xoá file của phần log vừa được compact ở thread nền. Làm ngay trong lock thì cả node đứng hàng chục mili giây mỗi lần
    // snapshot, và mọi node của cluster snapshot ở cùng một index nên chúng đứng cùng lúc.
    private void cleanupLog() {
        runtime.executeRead(() -> {
            try {
                persistent.getLogStore().cleanup();
            } catch (Exception e) {
                log.error("Node {} failed to delete compacted log segments", nodeId, e);
            }
        });
    }

    private void endSnapshotting() {
        lock.lock();
        try {
            snapshotting = false;
        } finally {
            unlock();
        }
    }

    @Override
    public InstallSnapshotResponse handleInstallSnapshotRequest(InstallSnapshotRequest req) {
        var response = installSnapshot(req);
        try {
            persistent.syncVote();
        } catch (Exception e) {
            log.error("Node {} failed to persist term", nodeId, e);
            return snapshotResponse(false);
        }
        return response;
    }

    // success=true ở đây nghĩa là follower đã có đủ state của snapshot
    private InstallSnapshotResponse snapshotResponse(boolean success) {
        return new InstallSnapshotResponse(persistent.getCurrentTerm(), success, success);
    }

    // snapshot mà node đang nhận dần từ leader
    private static final class IncomingSnapshot {
        final String leaderId;
        final long index;
        final List<String> files;
        String tempPath;
        // mẩu kế tiếp phải thuộc file này, tại vị trí này (hoặc là mẩu đầu của file kế tiếp)
        int fileIndex;
        long offset;
        // một mẩu đang được ghi xuống đĩa ngoài lock
        boolean busy;
        long lastChunkNanos;

        IncomingSnapshot(String leaderId, long index, List<String> files, long nowNanos) {
            this.leaderId = leaderId;
            this.index = index;
            this.files = files;
            this.lastChunkNanos = nowNanos;
        }

        // nhận mẩu nếu nó nối tiếp đúng chỗ; chuyển sang file kế tiếp khi cần
        boolean accept(InstallSnapshotRequest req) {
            if (req.getLastIncludedIndex() != index || !leaderId.equals(req.getLeaderId()) || fileIndex >= files.size()) {
                return false;
            }
            if (files.get(fileIndex).equals(req.getFileName()) && req.getOffset() == offset) {
                return true;
            }
            if (fileIndex + 1 < files.size() && files.get(fileIndex + 1).equals(req.getFileName()) && req.getOffset() == 0) {
                fileIndex++;
                offset = 0;
                return true;
            }
            return false;
        }
    }

    private void abortIncomingSnapshot() {
        incomingSnapshot = null;
        snapshotting = false;
    }

    /**
     * Nhận một mẩu snapshot, chỉ giữ lock ở các bước đổi state trong bộ nhớ. Khi nhận mẩu cuối:
     * state machine load từ temp → commit snapshot → cập nhật log và index.
     * Load trước rồi mới commit: nếu load hỏng thì snapshot và log cũ trên đĩa còn nguyên để restart dựng lại.
     */
    private InstallSnapshotResponse installSnapshot(InstallSnapshotRequest req) {
        var rejected = acceptSnapshotChunk(req);
        if (rejected != null) {
            return rejected;
        }

        var incoming = incomingSnapshot;
        var index = req.getLastIncludedIndex();
        var snapshotStore = persistent.getSnapshotStore();
        try {
            if (incoming.tempPath == null) {
                incoming.tempPath = snapshotStore.prepareTemp();
            }
            writeSnapshotChunk(incoming.tempPath, req);
        } catch (Exception e) {
            log.error("Node {} failed to store snapshot chunk", nodeId, e);
            lock.lock();
            try {
                abortIncomingSnapshot();
            } finally {
                unlock();
            }
            return snapshotResponse(false);
        }

        lock.lock();
        try {
            incoming.busy = false;
            incoming.offset += req.getData().length;
            incoming.lastChunkNanos = runtime.nanoTime();
            if (stopped || incomingSnapshot != incoming) {
                return snapshotResponse(false);
            }
            if (!req.isDone()) {
                return new InstallSnapshotResponse(persistent.getCurrentTerm(), true, false); // chờ mẩu kế tiếp
            }
            // đã đủ mọi mẩu: phần còn lại vẫn giữ cờ snapshotting cho tới khi cài xong
            incomingSnapshot = null;
        } finally {
            unlock();
        }

        var meta = new SnapshotMeta(index, req.getLastIncludedTerm(), req.getConf(), req.getFiles(), req.getSessions());
        var needLoad = beginSnapshotLoad(index);
        if (needLoad && !loadSnapshot(incoming.tempPath)) {
            // không kiểm chứng được state machine còn nguyên hay không, nên false cũng dừng như exception
            return failStop("state machine could not load snapshot at index " + index);
        }
        try {
            // snapshot chỉ chứa dữ liệu đã commit nên lưu lại luôn an toàn, kể cả khi term đổi trong lúc ghi
            snapshotStore.commit(meta);
        } catch (Exception e) {
            log.error("Node {} failed to commit snapshot", nodeId, e);
            if (needLoad) {
                return failStop("snapshot at index " + index + " was loaded but could not be saved");
            }
            endSnapshotting();
            return snapshotResponse(false);
        }
        return finishInstallSnapshot(req, needLoad);
    }

    // trả về response nếu request kết thúc ngay tại đây, null nếu mẩu này cần được ghi xuống đĩa
    private InstallSnapshotResponse acceptSnapshotChunk(InstallSnapshotRequest req) {
        lock.lock();
        try {
            ensureRunning();
            if (req.getTerm() < persistent.getCurrentTerm()) {
                return snapshotResponse(false);
            }
            handleHeartbeat(req.getTerm(), req.getLeaderId());

            if (req.getLastIncludedIndex() <= volatileState.getCommitIndex()) {
                // đã có sẵn toàn bộ dữ liệu của snapshot này
                return snapshotResponse(true);
            }
            var files = req.getFiles();
            var firstChunk = req.getOffset() == 0 && (files.isEmpty() || files.get(0).equals(req.getFileName()));
            if (firstChunk) {
                // bắt đầu (lại) một lần truyền; lần truyền dở trước đó của leader cũ bị bỏ
                var busy = incomingSnapshot != null ? incomingSnapshot.busy : snapshotting;
                if (busy) {
                    return snapshotResponse(false); // thư mục temp đang được dùng
                }
                incomingSnapshot = new IncomingSnapshot(req.getLeaderId(), req.getLastIncludedIndex(), files, runtime.nanoTime());
                snapshotting = true;
            } else if (incomingSnapshot == null || incomingSnapshot.busy || !incomingSnapshot.accept(req)) {
                // mẩu không nối tiếp đúng chỗ (mất mẩu, gửi trùng, đổi leader): leader sẽ gửi lại từ đầu
                if (incomingSnapshot != null && !incomingSnapshot.busy) {
                    abortIncomingSnapshot();
                }
                return snapshotResponse(false);
            }
            incomingSnapshot.busy = true;
            return null;
        } finally {
            unlock();
        }
    }

    private void writeSnapshotChunk(String folder, InstallSnapshotRequest req) throws IOException {
        var name = req.getFileName();
        if (name == null) {
            return; // snapshot không có file nào
        }
        if (name.contains("/") || name.contains("\\") || name.equals("..")) {
            throw new IOException("Illegal snapshot file name: " + name);
        }
        var options = req.getOffset() == 0
                ? new StandardOpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING}
                : new StandardOpenOption[]{StandardOpenOption.WRITE};
        try (FileChannel channel = FileChannel.open(Path.of(folder, name), options)) {
            var buffer = ByteBuffer.wrap(req.getData());
            long position = req.getOffset();
            while (buffer.hasRemaining()) {
                position += channel.write(buffer, position);
            }
            // từng mẩu đều được fsync, nên khi mẩu cuối về tới thì cả file đã nằm trên đĩa
            channel.force(true);
        }
    }

    // hoãn applyCommitted để không có onApply nào chen vào lúc state machine load ngoài lock.
    // Trả về false nếu trong lúc ghi file node đã tự commit qua điểm này, khi đó không cần load nữa.
    private boolean beginSnapshotLoad(long snapshotIndex) {
        lock.lock();
        try {
            loadingSnapshot = !stopped && snapshotIndex > volatileState.getCommitIndex();
            return loadingSnapshot;
        } finally {
            unlock();
        }
    }

    private boolean loadSnapshot(String path) {
        try {
            return stateMachine.onSnapshotLoad(new SnapshotReader(path));
        } catch (Exception e) {
            log.error("Node {} failed to load snapshot", nodeId, e);
            return false;
        }
    }

    private InstallSnapshotResponse finishInstallSnapshot(InstallSnapshotRequest req, boolean loaded) {
        var index = req.getLastIncludedIndex();
        lock.lock();
        try {
            snapshotting = false;
            loadingSnapshot = false;
            if (stopped) {
                return snapshotResponse(false);
            }
            var logStore = persistent.getLogStore();
            if (loaded) {
                var local = logStore.get(index);
                if (local != null && local.getTerm() == req.getLastIncludedTerm()) {
                    // log khớp với snapshot thì giữ lại phần phía sau
                    logStore.truncatePrefix(index + 1);
                    cleanupLog();
                } else {
                    logStore.reset(index, req.getLastIncludedTerm());
                }
                volatileState.setLastApplied(index);
                volatileState.setCommitIndex(Math.max(volatileState.getCommitIndex(), index));
                restoreSessions(req.getSessions());
            } else {
                // state machine đã đi qua điểm này: chỉ cần bỏ phần log mà snapshot đã bao phủ
                logStore.truncatePrefix(index + 1);
                cleanupLog();
            }
            refreshConf();
            if (loaded) {
                snapshotsInstalled++;
                log.info("Node {} installed snapshot at index {} from {}.", nodeId, index, req.getLeaderId());
                // apply các entry được commit trong lúc load, đồng thời lưu commit index
                applyCommitted();
            }
            return snapshotResponse(true);
        } catch (Exception e) {
            log.error("Node {} failed to install snapshot", nodeId, e);
            // state machine đã mang state mới mà log/index chưa theo kịp thì không chạy tiếp được
            return loaded ? failStop("snapshot at index " + index + " was loaded but could not be installed")
                    : snapshotResponse(false);
        } finally {
            unlock();
        }
    }

    // state machine trong bộ nhớ không còn đáng tin: dừng hẳn, restart sẽ dựng lại từ snapshot và log trên đĩa
    private InstallSnapshotResponse failStop(String reason) {
        log.error("Node {} stops: {}.", nodeId, reason);
        shutdown();
        return snapshotResponse(false);
    }

    // ---------- RPC HANDLERS ----------

    @Override
    public PreVoteResponse handlePreVoteRequest(PreVoteRequest request) {
        var response = preVote(request);
        // term mà node để lộ trong câu trả lời phải nằm trên đĩa trước
        try {
            persistent.syncVote();
        } catch (Exception e) {
            log.error("Node {} failed to persist term", nodeId, e);
            return new PreVoteResponse(response.term, false);
        }
        return response;
    }

    private PreVoteResponse preVote(PreVoteRequest request) {
        lock.lock();
        try {
            ensureRunning();
            var term = persistent.getCurrentTerm();
            if (!conf.contains(request.candidateId)) {
                log.warn("Node {} ignore PreVoteRequest from {} as it is not in conf <{}>.", getNodeId(), request.candidateId, this.conf);
                return new PreVoteResponse(term, false);
            }
            if (request.term < term) {
                return new PreVoteResponse(term, false);
            }
            // leader còn sống (hoặc vừa bỏ phiếu cho ai đó) thì không tiếp tay cho một cuộc bầu cử mới
            if (state == NodeState.LEADER || hasRecentContact()) {
                return new PreVoteResponse(term, false);
            }
            return new PreVoteResponse(term, isLogUpToDate(request.lastLogTerm, request.lastLogIndex));
        } finally {
            unlock();
        }
    }

    @Override
    public RequestVoteResponse handleRequestVoteRequest(RequestVoteRequest req) {
        var response = vote(req);
        // term và phiếu bầu phải nằm trên đĩa trước khi trả lời, ghi ngoài lock
        try {
            persistent.syncVote();
        } catch (Exception e) {
            log.error("Node {} failed to persist vote", nodeId, e);
            return new RequestVoteResponse(response.term, false);
        }
        if (!response.voteGranted) {
            return response;
        }
        lock.lock();
        try {
            if (stopped || persistent.getCurrentTerm() != response.term) {
                return new RequestVoteResponse(persistent.getCurrentTerm(), false);
            }
            return response;
        } finally {
            unlock();
        }
    }

    // quyết định phiếu bầu trong bộ nhớ, chưa ghi đĩa
    private RequestVoteResponse vote(RequestVoteRequest req) {
        lock.lock();
        try {
            ensureRunning();
            if (!conf.contains(req.candidateId) || req.term < persistent.getCurrentTerm()) {
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
                electionTimer.reset();
            }
            return new RequestVoteResponse(persistent.getCurrentTerm(), voteGranted);
        } finally {
            unlock();
        }
    }

    // log của candidate có mới ít nhất bằng log của node này không
    private boolean isLogUpToDate(long candidateLastTerm, long candidateLastIndex) {
        var lastTerm = persistent.getLogStore().lastTerm();
        var lastIndex = persistent.getLogStore().lastIndex();
        return candidateLastTerm > lastTerm || (candidateLastTerm == lastTerm && candidateLastIndex >= lastIndex);
    }

    @Override
    public TimeoutNowResponse handleTimeoutNowRequest(TimeoutNowRequest req) {
        lock.lock();
        try {
            ensureRunning();
            if (req.term != persistent.getCurrentTerm() || state == NodeState.LEADER || !conf.contains(nodeId)) {
                return new TimeoutNowResponse(persistent.getCurrentTerm(), false);
            }
            log.info("Node {} starts election on request of leader {}.", nodeId, req.leaderId);
            becomeCandidate(); // bỏ qua pre-vote: leader hiện tại đã chủ động nhường
            return new TimeoutNowResponse(req.term, true);
        } finally {
            unlock();
        }
    }

    @Override
    public AppendEntriesResponse handleAppendEntriesRequest(AppendEntriesRequest req) {
        var response = appendToLog(req);
        if (!response.success) {
            return response;
        }
        // fsync ngoài lock: node vẫn xử lý RPC khác trong lúc chờ đĩa, và chỉ ack khi entry đã bền vững
        try {
            persistent.getLogStore().sync();
            persistent.syncVote();
        } catch (Exception e) {
            log.error("Node {} failed to sync log", nodeId, e);
            return new AppendEntriesResponse(response.term, false, 0);
        }
        lock.lock();
        try {
            // trong lúc chờ đĩa node có thể đã sang term khác và log bị leader mới ghi đè.
            // Node vừa tự tắt vì bị gỡ vẫn ack lần cuối để leader biết nó đã nhận cấu hình và thôi gửi;
            // ack đó không được tính vào quorum nào vì node không còn trong cấu hình.
            var removedAndStopped = stopped && removalHandled && !conf.contains(nodeId);
            if ((stopped && !removedAndStopped) || persistent.getCurrentTerm() != response.term) {
                return new AppendEntriesResponse(persistent.getCurrentTerm(), false, 0);
            }
            return response;
        } finally {
            unlock();
        }
    }

    // kiểm tra và append vào log nhưng chưa fsync
    private AppendEntriesResponse appendToLog(AppendEntriesRequest req) {
        lock.lock();
        try {
            ensureRunning();
            if (req.term < persistent.getCurrentTerm()) {
                return new AppendEntriesResponse(persistent.getCurrentTerm(), false, 0);
            }

            handleHeartbeat(req.term, req.leaderId);

            var term = persistent.getCurrentTerm();
            var logStore = persistent.getLogStore();
            try {
                // matchIndex khi thất bại là gợi ý để leader lùi nextIndex nhanh
                if (req.prevLogIndex > logStore.lastIndex()) {
                    return new AppendEntriesResponse(term, false, logStore.lastIndex());
                }
                // prevLogIndex < baseIndex: phần đó nằm trong snapshot, đã commit nên chắc chắn khớp
                if (req.prevLogIndex >= logStore.getBaseIndex() && termAt(req.prevLogIndex) != req.prevLogTerm) {
                    return new AppendEntriesResponse(term, false, req.prevLogIndex - 1);
                }

                List<LogEntry> entries = req.entries();
                storeEntries(entries);

                var lastNewIndex = req.prevLogIndex + entries.size();
                var newCommit = Math.min(req.leaderCommit, lastNewIndex);
                if (newCommit > volatileState.getCommitIndex()) {
                    volatileState.setCommitIndex(newCommit);
                    applyCommitted();
                }
                return new AppendEntriesResponse(term, true, lastNewIndex);
            } catch (Exception e) {
                log.error("Node {} failed to append entries", nodeId, e);
                return new AppendEntriesResponse(term, false, volatileState.getCommitIndex());
            }
        } finally {
            unlock();
        }
    }

    // ghi các entry leader gửi vào log: bỏ qua entry đã có, chỉ truncate khi thực sự conflict term
    private void storeEntries(List<LogEntry> entries) {
        var logStore = persistent.getLogStore();
        var toAppend = new ArrayList<LogEntry>();
        var confChanged = false;
        for (LogEntry e : entries) {
            if (e.getIndex() <= logStore.getBaseIndex()) {
                continue;
            }
            if (toAppend.isEmpty()) {
                var existing = logStore.get(e.getIndex());
                if (existing != null) {
                    if (existing.getTerm() == e.getTerm()) {
                        // đã có sẵn (RPC cũ hoặc gửi trùng), không được truncate
                        continue;
                    }
                    if (e.getIndex() <= volatileState.getCommitIndex()) {
                        throw new IllegalStateException("Conflict at committed index " + e.getIndex());
                    }
                    confChanged |= confIndex >= e.getIndex();
                    logStore.truncateSuffix(e.getIndex());
                }
            }
            toAppend.add(e);
            confChanged |= e.isConfigurationEntry();
        }
        logStore.appendEntries(toAppend);
        if (confChanged) {
            refreshConf();
        }
    }

    // mọi RPC hợp lệ từ leader: chấp nhận leader đó và hoãn bầu cử
    private void handleHeartbeat(long term, String leaderId) {
        if (term > persistent.getCurrentTerm() || state != NodeState.FOLLOWER) {
            becomeFollower(term); // luôn step-down khi nhận leader heartbeat
        }
        this.leaderId = leaderId;
        electionEpoch++;
        markContact();
        electionTimer.reset();
    }

    private void markContact() {
        hadContact = true;
        lastContactNanos = runtime.nanoTime();
    }

    private boolean hasRecentContact() {
        return hadContact && runtime.nanoTime() - lastContactNanos
                < TimeUnit.MILLISECONDS.toNanos(nodeOptions.getElectionTimeoutMinMs());
    }

    // ---------- ELECTION ----------

    private void onElectionTimeout() {
        lock.lock();
        try {
            if (stopped || state == NodeState.LEADER) {
                return;
            }
            // luôn hẹn lại timer, nếu không một vòng bầu cử thất bại sẽ không bao giờ được thử lại
            electionTimer.reset();
            if (!conf.contains(nodeId)) {
                return; // chưa thuộc cluster (đang chờ được thêm) hoặc đã bị gỡ
            }
            startPreVote();
        } finally {
            unlock();
        }
    }

    // hỏi trước xem có thắng được không, để một node bị cô lập không làm tăng term của cả cluster
    private void startPreVote() {
        var epoch = ++electionEpoch;
        var term = persistent.getCurrentTerm();
        var granted = selfOnly();
        if (conf.hasQuorum(granted)) {
            becomeCandidate(); // cluster một node
            return;
        }
        var logStore = persistent.getLogStore();
        var req = new PreVoteRequest(term, nodeId, logStore.lastIndex(), logStore.lastTerm());
        for (var peer : peers()) {
            rpcProcessor.preVote(peer, req).whenComplete((response, error) -> {
                if (response == null) {
                    return;
                }
                lock.lock();
                try {
                    onPreVoteResponse(peer, response, epoch, term, granted);
                } finally {
                    unlock();
                }
            });
        }
    }

    private void onPreVoteResponse(String peer, PreVoteResponse response, long epoch, long term, Set<String> granted) {
        if (stopped || state == NodeState.LEADER) {
            return;
        }
        if (response.term > persistent.getCurrentTerm()) {
            becomeFollower(response.term);
            return;
        }
        // response của vòng pre-vote cũ
        if (electionEpoch != epoch || persistent.getCurrentTerm() != term) {
            return;
        }
        log.info("Node {} received PreVoteResponse from {}, term={}, granted={}.", getNodeId(), peer, response.term, response.voteGranted);
        // candidate thua vòng trước cũng phải được bầu lại, không chỉ follower
        if (response.voteGranted && granted.add(peer) && conf.hasQuorum(granted)) {
            becomeCandidate();
        }
    }

    private void becomeFollower(long term) {
        var changedTerm = false;
        // Cập nhật term nếu term mới lớn hơn
        if (term > persistent.getCurrentTerm()) {
            persistent.setTermAndVote(term, null);
            runIo(persistent::syncVote);
            changedTerm = true;
        }
        // Dù term bằng currentTerm, nếu node không phải follower thì vẫn step-down
        if (state != NodeState.FOLLOWER || changedTerm) {
            log.info(nodeId + " current state " + state + " becomes FOLLOWER at term " + persistent.getCurrentTerm());
            state = NodeState.FOLLOWER;
            leaderState = null;
            leaderId = null;
            heartbeatTimer.stop();
            electionTimer.reset();
            failPendingFutures();
        }
    }

    private void becomeCandidate() {
        if (state == NodeState.LEADER) {
            return; // tránh race khi heartbeat tới đúng lúc
        }
        state = NodeState.CANDIDATE;
        electionsStarted++;
        leaderId = null;
        persistent.setTermAndVote(persistent.getCurrentTerm() + 1, nodeId);
        // đang tự ứng cử thì không ủng hộ pre-vote của node khác
        markContact();
        electionTimer.reset();

        var termStarted = persistent.getCurrentTerm();
        // phiếu tự bầu phải nằm trên đĩa trước khi được tính hay gửi đi
        runIo(() -> {
            persistent.syncVote();
            lock.lock();
            try {
                if (!stopped && state == NodeState.CANDIDATE && persistent.getCurrentTerm() == termStarted) {
                    requestVotes(termStarted);
                }
            } finally {
                unlock();
            }
        });
    }

    private void requestVotes(long termStarted) {
        var granted = selfOnly();
        if (conf.hasQuorum(granted)) {
            becomeLeader(); // cluster một node
            return;
        }
        var logStore = persistent.getLogStore();
        var req = new RequestVoteRequest(termStarted, nodeId, logStore.lastIndex(), logStore.lastTerm());
        for (String peer : peers()) {
            rpcProcessor.requestVote(peer, req).whenComplete((resp, error) -> {
                if (resp == null) {
                    return;
                }
                lock.lock();
                try {
                    onVoteResponse(peer, resp, termStarted, granted);
                } finally {
                    unlock();
                }
            });
        }
    }

    private void onVoteResponse(String peer, RequestVoteResponse resp, long termStarted, Set<String> granted) {
        if (stopped) {
            return;
        }
        if (resp.term > persistent.getCurrentTerm()) {
            becomeFollower(resp.term);
            return;
        }
        if (state != NodeState.CANDIDATE || persistent.getCurrentTerm() != termStarted) {
            return;
        }
        if (resp.voteGranted && granted.add(peer) && conf.hasQuorum(granted)) {
            becomeLeader();
        }
    }

    private void becomeLeader() {
        var logStore = persistent.getLogStore();
        state = NodeState.LEADER;
        leaderId = nodeId;
        leaderState = new LeaderState(peers(), logStore.lastIndex() + 1, runtime.nanoTime());
        electionTimer.stop();
        restoreDepartingNodes();
        // no-op của term mới: entry của term cũ chỉ được commit gián tiếp qua entry của term hiện tại
        try {
            logStore.appendEntry(new LogEntry(logStore.lastIndex() + 1, persistent.getCurrentTerm(), null));
        } catch (Exception e) {
            // leader không ghi được log thì không làm leader được: nhường để node khác bầu lại
            log.error("Leader {} failed to append its no-op entry, step down", nodeId, e);
            becomeFollower(persistent.getCurrentTerm());
            return;
        }
        timesElectedLeader++;
        log.info("Leader " + nodeId + " elected at term " + persistent.getCurrentTerm());
        heartbeatTimer.start();
        broadcast();
    }

    // ---------- REPLICATION ----------

    private void onHeartbeatTick() {
        lock.lock();
        try {
            if (stopped || state != NodeState.LEADER) {
                return;
            }
            // check quorum: leader bị cô lập phải tự step-down thay vì giữ vai trò mãi
            if (!hasContactWithQuorum()) {
                log.warn("Leader {} lost contact with quorum, step down.", nodeId);
                becomeFollower(persistent.getCurrentTerm());
                return;
            }
            expireDepartingNodes();
            expirePendingConf();
            var logStore = persistent.getLogStore();
            if (logStore.durableIndex() < logStore.lastIndex()) {
                requestLogSync(); // lần fsync trước thất bại, hoặc chưa kịp chạy
            }
            for (String peer : replicationTargets()) {
                replicateTo(peer);
            }
        } finally {
            unlock();
        }
    }

    private boolean hasContactWithQuorum() {
        var now = runtime.nanoTime();
        var timeoutNanos = TimeUnit.MILLISECONDS.toNanos(nodeOptions.getElectionTimeoutMaxMs());
        var alive = selfOnly();
        for (var ack : leaderState.getLastAck().entrySet()) {
            if (now - ack.getValue() < timeoutNanos) {
                alive.add(ack.getKey());
            }
        }
        return conf.hasQuorum(alive);
    }

    // gọi sau khi leader append: gửi cho follower song song với việc fsync log của chính mình
    private void broadcast() {
        for (String peer : replicationTargets()) {
            replicateTo(peer);
        }
        requestLogSync();
    }

    /**
     * Yêu cầu thread IO fsync log của leader. Mỗi lúc chỉ có nhiều nhất một yêu cầu đang xếp hàng: một lần fsync
     * bao trọn mọi entry đã append tới lúc nó chạy, nên xếp thêm yêu cầu cho từng lệnh chỉ làm hàng đợi dài ra
     * (ở tải cao, hàng đợi đó lớn dần không giới hạn và làm GC dừng hàng trăm mili giây).
     */
    private void requestLogSync() {
        if (logSyncQueued) {
            return;
        }
        logSyncQueued = true;
        runIo(() -> {
            try {
                persistent.getLogStore().sync();
            } finally {
                lock.lock();
                try {
                    logSyncQueued = false;
                    if (!stopped && state == NodeState.LEADER) {
                        maybeAdvanceCommitIndex(); // leader chỉ tự tính mình vào quorum khi entry đã nằm trên đĩa
                        var logStore = persistent.getLogStore();
                        if (logStore.durableIndex() < logStore.lastIndex()) {
                            requestLogSync(); // có entry mới đến trong lúc đang fsync
                        }
                    }
                } finally {
                    unlock();
                }
            }
        });
    }

    private void replicateTo(String peer) {
        if (state != NodeState.LEADER) {
            return;
        }
        var ls = leaderState;
        if (!ls.getInflight().add(peer)) {
            return; // RPC trước chưa trả lời, khi nó xong sẽ gửi tiếp phần còn thiếu
        }
        try {
            var logStore = persistent.getLogStore();
            var lastIndex = logStore.lastIndex();
            var nextIdx = Math.min(ls.getNextIndex().getOrDefault(peer, lastIndex + 1), lastIndex + 1);
            if (nextIdx <= logStore.getBaseIndex()) {
                // phần follower cần đã bị compact vào snapshot
                sendSnapshot(ls, peer);
            } else {
                sendEntries(ls, peer, nextIdx);
            }
        } catch (Exception e) {
            log.error("Leader {} failed to replicate to {}", nodeId, peer, e);
            ls.getInflight().remove(peer);
        }
    }

    // gửi mọi entry từ nextIdx, hoặc heartbeat rỗng nếu follower đã đủ log
    private void sendEntries(LeaderState ls, String peer, long nextIdx) {
        var logStore = persistent.getLogStore();
        var prevIndex = nextIdx - 1;
        // phần còn lại (nếu có) được gửi tiếp ngay khi request này được trả lời
        var maxEntries = nodeOptions.getMaxEntriesPerRequest();
        // term của prevIndex cũng nằm trong log, trừ khi prevIndex chính là mốc của snapshot
        var prevInLog = prevIndex > logStore.getBaseIndex();
        var readStart = prevInLog ? prevIndex : nextIdx;
        var readCount = prevInLog ? maxEntries + 1 : maxEntries;
        if (logStore.isCached(readStart, readCount)) {
            // log lưu sẵn entry dưới dạng khung thì gửi nguyên vùng byte đó, không dựng lại từng entry cho từng follower
            var block = logStore.readBlock(nextIdx, maxEntries);
            if (block != null) {
                send(ls, peer, new AppendEntriesRequest(persistent.getCurrentTerm(), nodeId, prevIndex, termAt(prevIndex),
                        block, volatileState.getCommitIndex()));
            } else {
                sendEntries(ls, peer, prevIndex, termAt(prevIndex), logStore.readFrom(nextIdx, maxEntries));
            }
            return;
        }
        // follower tụt xa hơn phần log còn trong bộ nhớ: đọc từ đĩa ở thread khác, node không phải chờ
        var baseTerm = logStore.getBaseTerm();
        runtime.executeRead(() -> {
            List<LogEntry> read = null;
            try {
                read = logStore.readFrom(readStart, readCount);
            } catch (Exception e) {
                log.error("Leader {} failed to read log from index {} for {}", nodeId, readStart, peer, e);
            }
            lock.lock();
            try {
                // Chừng nào node còn là leader của đúng nhiệm kỳ đó thì log của nó chỉ dài thêm, nên phần vừa đọc vẫn đúng.
                // Nếu snapshot vừa compact mất đoạn này thì bỏ, nhịp sau sẽ gửi snapshot.
                if (read == null || read.isEmpty() || read.get(0).getIndex() != readStart || stopped || leaderState != ls) {
                    ls.getInflight().remove(peer);
                    return;
                }
                var prevTerm = prevInLog ? read.get(0).getTerm() : baseTerm;
                sendEntries(ls, peer, prevIndex, prevTerm, prevInLog ? new ArrayList<>(read.subList(1, read.size())) : read);
            } catch (Exception e) {
                log.error("Leader {} failed to replicate to {}", nodeId, peer, e);
                ls.getInflight().remove(peer);
            } finally {
                unlock();
            }
        });
    }

    private void sendEntries(LeaderState ls, String peer, long prevIndex, long prevTerm, List<LogEntry> entries) {
        send(ls, peer, new AppendEntriesRequest(persistent.getCurrentTerm(), nodeId, prevIndex, prevTerm, entries,
                volatileState.getCommitIndex()));
    }

    private void send(LeaderState ls, String peer, AppendEntriesRequest req) {
        var sentStamp = ls.nextStamp();
        rpcProcessor.appendEntries(peer, req).whenComplete((resp, error) -> {
            lock.lock();
            try {
                onAppendEntriesResponse(ls, peer, req, resp, sentStamp);
            } finally {
                unlock();
            }
        });
    }

    private void onAppendEntriesResponse(LeaderState ls, String peer, AppendEntriesRequest req,
                                         AppendEntriesResponse resp, long sentStamp) {
        ls.getInflight().remove(peer);
        if (resp == null || !isCurrentLeader(ls, resp.term)) {
            return;
        }
        ls.getLastAck().put(peer, runtime.nanoTime());
        // dù thành công hay bị từ chối vì log chưa khớp, peer trả lời ở đúng term này tức là nó vẫn coi node này là leader
        ls.getAckedStamp().merge(peer, sentStamp, Math::max);
        completeReads();
        advanceReplication(ls, peer, req, resp);
        // yêu cầu đọc đến sau khi request này được gửi cần một vòng mới, không chờ tới nhịp heartbeat kế tiếp
        if (leaderState == ls && awaitsReadConfirmation(ls, peer)) {
            replicateTo(peer);
        }
    }

    private void advanceReplication(LeaderState ls, String peer, AppendEntriesRequest req, AppendEntriesResponse resp) {
        var match = ls.getMatchIndex().getOrDefault(peer, 0L);
        if (!resp.success) {
            // lùi theo gợi ý của follower, không bao giờ lùi quá phần đã match
            var sentNext = req.prevLogIndex + 1;
            var next = Math.max(match + 1, Math.min(sentNext - 1, resp.matchIndex + 1));
            ls.getNextIndex().put(peer, next);
            if (next < sentNext) {
                replicateTo(peer);
            }
            return;
        }

        match = Math.max(match, req.prevLogIndex + req.entryCount());
        ls.getMatchIndex().put(peer, match);
        ls.getNextIndex().put(peer, match + 1);
        onLearnerProgress(ls, peer, match);
        // node bị gỡ cần cả C(new) lẫn commit index phủ tới nó thì mới biết chắc mình đã rời cluster
        var needed = ls.getDeparting().get(peer);
        if (needed != null && match >= needed && req.leaderCommit >= needed) {
            removeDeparting(ls, peer);
        }
        maybeAdvanceCommitIndex();
        // commit có thể vừa khiến node step-down, hoặc leader đã append thêm trong lúc chờ
        if (leaderState == ls && match < persistent.getLogStore().lastIndex() && replicationTargets().contains(peer)) {
            replicateTo(peer);
        }
    }

    // gửi mẩu kế tiếp của snapshot cho peer; mỗi lần chỉ một mẩu nằm trong bộ nhớ
    private void sendSnapshot(LeaderState ls, String peer) {
        var snapshotStore = persistent.getSnapshotStore();
        var meta = snapshotStore.getMeta();
        var existing = ls.getSnapshotTransfers().get(peer);
        // snapshot mới hơn đã thay snapshot đang gửi dở: bắt đầu lại với bản mới
        var transfer = existing != null && existing.index == meta.getLastIncludedIndex()
                ? existing : new LeaderState.SnapshotTransfer(meta.getLastIncludedIndex(), snapshotStore.currentPath());
        ls.getSnapshotTransfers().put(peer, transfer);
        var term = persistent.getCurrentTerm();
        var fileIndex = transfer.fileIndex;
        var offset = transfer.offset;
        // đọc file snapshot ở thread IO, ngoài lock
        runIo(() -> {
            try {
                var files = meta.getFiles();
                String fileName = files.isEmpty() ? null : files.get(fileIndex);
                byte[] data = new byte[0];
                var endOfFile = true;
                if (fileName != null) {
                    try (FileChannel channel = FileChannel.open(Path.of(transfer.path, fileName), StandardOpenOption.READ)) {
                        var buffer = ByteBuffer.allocate((int) Math.min(nodeOptions.getSnapshotChunkBytes(), channel.size() - offset));
                        while (buffer.hasRemaining() && channel.read(buffer, offset + buffer.position()) >= 0) {
                            // đọc cho đầy mẩu
                        }
                        data = Arrays.copyOf(buffer.array(), buffer.position());
                        endOfFile = offset + data.length >= channel.size();
                    }
                }
                var lastChunkOfFile = endOfFile;
                var done = fileName == null || (lastChunkOfFile && fileIndex == files.size() - 1);
                var req = new InstallSnapshotRequest(term, nodeId, meta.getLastIncludedIndex(), meta.getLastIncludedTerm(),
                        meta.getConf(), meta.getSessions(), files, fileName, offset, data, done);
                rpcProcessor.installSnapshot(peer, req).whenComplete((resp, error) -> {
                    lock.lock();
                    try {
                        onInstallSnapshotResponse(ls, peer, req, resp, transfer, lastChunkOfFile);
                    } finally {
                        unlock();
                    }
                });
            } catch (Exception e) {
                // thường là snapshot vừa bị thay bằng bản mới hơn: lần sau gửi lại từ đầu
                log.error("Leader {} failed to send snapshot to {}", nodeId, peer, e);
                lock.lock();
                try {
                    ls.getInflight().remove(peer);
                    ls.getSnapshotTransfers().remove(peer);
                } finally {
                    unlock();
                }
            }
        });
    }

    private void onInstallSnapshotResponse(LeaderState ls, String peer, InstallSnapshotRequest req, InstallSnapshotResponse resp,
                                           LeaderState.SnapshotTransfer transfer, boolean lastChunkOfFile) {
        ls.getInflight().remove(peer);
        if (resp == null || !isCurrentLeader(ls, resp.getTerm())) {
            return; // không rõ mẩu đã tới chưa: nhịp sau gửi lại đúng mẩu này
        }
        ls.getLastAck().put(peer, runtime.nanoTime());
        if (!resp.isSuccess()) {
            ls.getSnapshotTransfers().remove(peer); // follower không nối được mẩu này: gửi lại từ đầu
            return;
        }
        if (!resp.isComplete()) {
            if (lastChunkOfFile) {
                transfer.fileIndex++;
                transfer.offset = 0;
            } else {
                transfer.offset += req.getData().length;
            }
            if (leaderState == ls) {
                replicateTo(peer); // mẩu kế tiếp
            }
            return;
        }
        ls.getSnapshotTransfers().remove(peer);
        var match = Math.max(ls.getMatchIndex().getOrDefault(peer, 0L), req.getLastIncludedIndex());
        ls.getMatchIndex().put(peer, match);
        ls.getNextIndex().put(peer, match + 1);
        onLearnerProgress(ls, peer, match);
        maybeAdvanceCommitIndex();
        if (leaderState == ls) {
            replicateTo(peer); // gửi tiếp phần log sau snapshot
        }
    }

    // response chỉ còn giá trị nếu node vẫn là leader của đúng nhiệm kỳ đã gửi request.
    // Thấy term cao hơn thì step-down luôn tại đây.
    private boolean isCurrentLeader(LeaderState ls, long responseTerm) {
        if (stopped) {
            return false;
        }
        if (responseTerm > persistent.getCurrentTerm()) {
            becomeFollower(responseTerm);
            return false;
        }
        return leaderState == ls;
    }

    // ---------- COMMIT & APPLY ----------

    // -1 nếu index không còn trong log (đã compact, hoặc chưa tới)
    private long termAt(long index) {
        var logStore = persistent.getLogStore();
        if (index == logStore.getBaseIndex()) {
            return logStore.getBaseTerm();
        }
        var entry = logStore.get(index);
        return entry == null ? -1 : entry.getTerm();
    }

    private void maybeAdvanceCommitIndex() {
        if (state != NodeState.LEADER) {
            return;
        }
        var logStore = persistent.getLogStore();
        var matchIndex = leaderState.getMatchIndex();
        // leader chỉ tính chính nó tới phần log đã nằm trên đĩa
        var durableIndex = logStore.durableIndex();
        var candidate = conf.quorumIndex(id -> id.equals(nodeId) ? durableIndex : matchIndex.getOrDefault(id, 0L));
        // chỉ commit trực tiếp entry của term hiện tại; entry của term cũ đứng trước nó được commit theo.
        // Term trong log không giảm, nên nếu candidate thuộc term cũ thì mọi index nhỏ hơn cũng vậy.
        if (candidate > volatileState.getCommitIndex() && termAt(candidate) == persistent.getCurrentTerm()) {
            volatileState.setCommitIndex(candidate);
            applyCommitted();
        }
    }

    private void applyCommitted() {
        if (loadingSnapshot) {
            return; // sẽ apply sau khi snapshot load xong
        }
        var lastApplied = volatileState.getLastApplied();
        var commitIndex = volatileState.getCommitIndex();
        var logStore = persistent.getLogStore();
        for (long idx = lastApplied + 1; idx <= commitIndex; idx++) {
            LogEntry logEntry = logStore.get(idx);
            // no-op và config entry không thuộc về state machine
            if (logEntry != null && logEntry.isSessionClose()) {
                sessions.remove(logEntry.getClientId());
            } else if (logEntry != null && !logEntry.isConfigurationEntry() && logEntry.getCommand() != null) {
                applyCommand(logEntry);
            }
            volatileState.setLastApplied(idx);
            // hàng chờ xếp theo index, và các entry được apply theo đúng thứ tự đó
            while (!pendingFutures.isEmpty() && pendingFutures.peekFirst().index() <= idx) {
                var pending = pendingFutures.pollFirst();
                complete(pending.future(), pending.index() == idx);
            }
        }
        // commit index chỉ cập nhật trong bộ nhớ, ghi xuống đĩa sau theo nhịp ở thread IO
        persistent.setLastCommitIndex(commitIndex);
        scheduleCommitIndexFlush();
        onConfCommitted();
        maybeShutdownRemoved();
        completeReads();
        runAppliedWaiters();
        maybeSnapshot();
    }

    // việc ghi commit index dùng chung thread IO với việc fsync log: ghi sau mỗi lần commit sẽ bắt lệnh kế tiếp
    // chờ thêm hai lần fsync, nên gom lại và ghi theo nhịp
    private void scheduleCommitIndexFlush() {
        var interval = nodeOptions.getCommitIndexFlushIntervalMs();
        if (interval <= 0) {
            runIo(persistent::flush);
            return;
        }
        if (commitIndexFlushScheduled) {
            return;
        }
        commitIndexFlushScheduled = true;
        runtime.schedule(() -> {
            lock.lock();
            try {
                commitIndexFlushScheduled = false;
            } finally {
                unlock();
            }
            runIo(persistent::flush);
        }, interval);
    }

    private void maybeSnapshot() {
        var interval = nodeOptions.getSnapshotIntervalEntries();
        var uncompacted = volatileState.getLastApplied() - persistent.getLogStore().getBaseIndex();
        if (interval > 0 && state != null && uncompacted >= interval) {
            createSnapshot();
        }
    }

    // lệnh client gửi lại (cùng clientId và sequence đã apply) có thể nằm trong log hai lần nhưng chỉ được apply một lần.
    // Mọi node quyết định giống nhau vì bảng sessions chỉ phụ thuộc vào các entry đã apply trước đó.
    private void applyCommand(LogEntry entry) {
        if (isDuplicate(entry.getClientId(), entry.getSequence())) {
            return;
        }
        stateMachine.onApply(nodeId, entry);
        if (entry.getClientId() != null) {
            sessions.computeIfAbsent(entry.getClientId(), id -> new ClientSession()).markApplied(entry.getSequence());
        }
    }
}

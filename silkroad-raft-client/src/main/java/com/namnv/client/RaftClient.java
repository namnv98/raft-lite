package com.namnv.client;

import com.namnv.rpc.MessageTransport;
import com.namnv.transport.SocketRpcClient;

import com.namnv.entity.CommandBatch;

import com.namnv.rpc.model.request.ClientReadRequest;
import com.namnv.rpc.model.request.ClientWriteRequest;
import com.namnv.rpc.model.response.ClientReadResponse;
import com.namnv.rpc.model.response.ClientWriteResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Client của cluster, dùng từ một tiến trình bất kỳ qua TCP. Nó tự tìm leader, tự chuyển sang node khác khi leader đổi
 * hoặc không trả lời, và gửi lại lệnh ghi với cùng (clientId, sequence) nên mỗi lệnh được apply nhiều nhất một lần.
 * <p>
 * Một clientId chỉ được dùng bởi một RaftClient tại một thời điểm. Lệnh ghi vẫn thất bại sau {@code maxWaitMs} để lại
 * một sequence không rõ kết quả; gọi {@link #write} tiếp vẫn đúng, nhưng server phải nhớ sequence đó cho tới khi phiên được đóng.
 */
public class RaftClient implements AutoCloseable {
    private static final ScheduledExecutorService RETRY_TIMER = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "raft-client-retry");
        thread.setDaemon(true);
        return thread;
    });
    private static final int RETRY_DELAY_MS = 20;

    private final MessageTransport transport;
    private final boolean ownsTransport;
    private final List<String> servers;
    private final String clientId;
    private final long maxWaitMs;
    private final AtomicLong nextSequence = new AtomicLong();
    private final AtomicLong rotation = new AtomicLong();
    // node mà client tin là leader
    private volatile String target;

    /**
     * @param servers          địa chỉ host:port của các node
     * @param clientId         định danh duy nhất của client này, dùng để chống ghi trùng; null nếu state machine tự chống
     *                         trùng bằng định danh của chính lệnh (lần gửi lại khi đó được apply lại và trả về kết quả
     *                         của nó, thay vì bị bỏ qua)
     * @param requestTimeoutMs thời hạn cho mỗi lần gửi tới một node
     * @param maxWaitMs        tổng thời gian tối đa cho một thao tác, tính cả các lần gửi lại
     */
    public RaftClient(List<String> servers, String clientId, int requestTimeoutMs, long maxWaitMs) {
        this(new SocketRpcClient(requestTimeoutMs), true, servers, clientId, maxWaitMs);
    }

    // nhiều RaftClient dùng chung một transport sẽ đi chung kết nối TCP tới mỗi node
    public RaftClient(MessageTransport transport, List<String> servers, String clientId, long maxWaitMs) {
        this(transport, false, servers, clientId, maxWaitMs);
    }

    private RaftClient(MessageTransport transport, boolean ownsTransport, List<String> servers, String clientId, long maxWaitMs) {
        this.transport = transport;
        this.ownsTransport = ownsTransport;
        this.servers = List.copyOf(servers);
        this.clientId = clientId;
        this.maxWaitMs = maxWaitMs;
        this.target = this.servers.get(0);
    }

    /**
     * Ghi một lệnh. Future trả về true khi lệnh đã được commit và apply; thất bại với TimeoutException nếu sau
     * {@code maxWaitMs} vẫn chưa có node nào xác nhận.
     */
    public CompletableFuture<Boolean> write(byte[] command) {
        var request = new ClientWriteRequest(clientId, nextSequence.incrementAndGet(), command);
        var result = new CompletableFuture<Boolean>();
        attemptWrite(request, result, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxWaitMs), APPLIED);
        return result;
    }

    /**
     * Ghi một lệnh và nhận về kết quả mà state machine trả cho nó (null nếu state machine không trả gì).
     * Lệnh được gửi lại tới leader mới khi không rõ kết quả, nên state machine nên chống trùng bằng định danh của chính
     * lệnh (ví dụ id giao dịch) để lần gửi lại trả về đúng kết quả.
     */
    public CompletableFuture<byte[]> submit(byte[] command) {
        var request = new ClientWriteRequest(clientId, nextSequence.incrementAndGet(), command);
        var result = new CompletableFuture<byte[]>();
        attemptWrite(request, result, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxWaitMs), RESULT);
        return result;
    }

    /**
     * Như {@link #writeBatch} nhưng nhận về kết quả của từng lệnh trong lô, gói bằng {@link CommandBatch#encode}
     * ({@link CommandBatch#forEach} để đọc); null nếu state machine không trả kết quả nào.
     */
    public CompletableFuture<byte[]> submitBatch(List<byte[]> commands) {
        var request = new ClientWriteRequest(clientId, nextSequence.incrementAndGet(), CommandBatch.encode(commands), true);
        var result = new CompletableFuture<byte[]>();
        attemptWrite(request, result, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxWaitMs), RESULT);
        return result;
    }

    private static final java.util.function.Function<ClientWriteResponse, Boolean> APPLIED = response -> Boolean.TRUE;
    private static final java.util.function.Function<ClientWriteResponse, byte[]> RESULT = response -> response.result;

    /**
     * Ghi nhiều lệnh trong một request và một entry của log: cả lô được apply cùng nhau, đúng một lần, và future
     * trả về true khi cả lô đã được apply. Nhanh hơn nhiều so với gửi từng lệnh khi client có sẵn nhiều lệnh.
     */
    public CompletableFuture<Boolean> writeBatch(List<byte[]> commands) {
        var request = new ClientWriteRequest(clientId, nextSequence.incrementAndGet(), CommandBatch.encode(commands), true);
        var result = new CompletableFuture<Boolean>();
        attemptWrite(request, result, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxWaitMs), APPLIED);
        return result;
    }

    /**
     * Đọc nhất quán: kết quả phản ánh mọi lệnh đã được xác nhận trước khi lời gọi này bắt đầu.
     */
    public CompletableFuture<byte[]> read(byte[] query) {
        var result = new CompletableFuture<byte[]>();
        attemptRead(null, new ClientReadRequest(query), result, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxWaitMs));
        return result;
    }

    // đọc nhất quán từ đúng một node (ví dụ một follower để chia tải), không chuyển sang node khác
    public CompletableFuture<byte[]> readFrom(String server, byte[] query) {
        var result = new CompletableFuture<byte[]>();
        attemptRead(server, new ClientReadRequest(query), result, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxWaitMs));
        return result;
    }

    private <T> void attemptWrite(ClientWriteRequest request, CompletableFuture<T> result, long deadlineNanos,
                                  java.util.function.Function<ClientWriteResponse, T> outcome) {
        var server = target;
        transport.send(server, request, ClientWriteResponse.class).whenComplete((response, error) -> {
            if (response != null && response.success) {
                result.complete(outcome.apply(response));
            } else if (System.nanoTime() >= deadlineNanos) {
                result.completeExceptionally(new TimeoutException("write was not confirmed within " + maxWaitMs + " ms"));
            } else {
                moveOn(server, response != null ? response.leaderId : null);
                // cùng sequence: nếu lần gửi trước thật ra đã được commit thì lần này chỉ nhận lại kết quả
                RETRY_TIMER.schedule(() -> attemptWrite(request, result, deadlineNanos, outcome), RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
            }
        });
    }

    private void attemptRead(String pinned, ClientReadRequest request, CompletableFuture<byte[]> result, long deadlineNanos) {
        var server = pinned != null ? pinned : target;
        transport.send(server, request, ClientReadResponse.class).whenComplete((response, error) -> {
            if (response != null && response.success) {
                result.complete(response.result);
            } else if (System.nanoTime() >= deadlineNanos) {
                result.completeExceptionally(new TimeoutException("read was not answered within " + maxWaitMs + " ms"));
            } else {
                if (pinned == null) {
                    moveOn(server, response != null ? response.leaderId : null);
                }
                RETRY_TIMER.schedule(() -> attemptRead(pinned, request, result, deadlineNanos), RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
            }
        });
    }

    // lần gửi tới failed không thành: theo gợi ý của node đó nếu có, không thì thử node kế tiếp
    private void moveOn(String failed, String leaderHint) {
        if (!failed.equals(target)) {
            return; // một lời gọi khác đã chuyển hướng rồi
        }
        if (leaderHint != null && !leaderHint.equals(failed) && servers.contains(leaderHint)) {
            target = leaderHint;
        } else {
            target = servers.get((int) (rotation.incrementAndGet() % servers.size()));
        }
    }

    public String getClientId() {
        return clientId;
    }

    @Override
    public void close() {
        if (ownsTransport) {
            transport.close();
        }
    }
}

package com.namnv.bench;

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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Giả lập độ trễ mạng giữa các node khi không dùng được tc netem: request được gửi đi sau nửa RTT, response được trả
 * về sau nửa RTT nữa. Một thread hẹn giờ duy nhất chạy các việc có cùng độ trễ theo đúng thứ tự gửi, nên thứ tự của
 * request (và của response) trên mỗi đường vẫn được giữ như TCP.
 */
final class DelayedRpcProcessor implements RpcProcessor {
    private final RpcProcessor inner;
    private final long halfRttNanos;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
        var thread = new Thread(r, "simulated-network");
        thread.setDaemon(true);
        return thread;
    });

    DelayedRpcProcessor(RpcProcessor inner, long rttMicros) {
        this.inner = inner;
        this.halfRttNanos = TimeUnit.MICROSECONDS.toNanos(rttMicros) / 2;
    }

    private <T> CompletableFuture<T> delayed(Supplier<CompletableFuture<T>> call) {
        var result = new CompletableFuture<T>();
        timer.schedule(() -> call.get().whenComplete((response, error) -> timer.schedule(() -> {
            if (error != null) {
                result.completeExceptionally(error);
            } else {
                result.complete(response);
            }
        }, halfRttNanos, TimeUnit.NANOSECONDS)), halfRttNanos, TimeUnit.NANOSECONDS);
        return result;
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(String serverId, RequestVoteRequest request) {
        return delayed(() -> inner.requestVote(serverId, request));
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(String serverId, AppendEntriesRequest request) {
        return delayed(() -> inner.appendEntries(serverId, request));
    }

    @Override
    public CompletableFuture<PreVoteResponse> preVote(String serverId, PreVoteRequest request) {
        return delayed(() -> inner.preVote(serverId, request));
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(String serverId, InstallSnapshotRequest request) {
        return delayed(() -> inner.installSnapshot(serverId, request));
    }

    @Override
    public CompletableFuture<TimeoutNowResponse> timeoutNow(String serverId, TimeoutNowRequest request) {
        return delayed(() -> inner.timeoutNow(serverId, request));
    }

    @Override
    public CompletableFuture<ReadIndexResponse> readIndex(String serverId, ReadIndexRequest request) {
        return delayed(() -> inner.readIndex(serverId, request));
    }
}

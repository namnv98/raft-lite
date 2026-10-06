package com.namnv.rpc.nio;

import com.namnv.agent.AgentLoop;
import com.namnv.rpc.RpcCodec;
import com.namnv.rpc.client.MessageTransport;
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Transport TCP chạy trên một {@link AgentLoop}, như publication/subscription của Aeron được poll trong duty cycle của agent.
 * Gắn vào vòng của chính node ({@code ThreadedRuntime.loop()}) thì request đi ra và response đi về đều được xử lý trên
 * thread của node, không qua thread trung gian nào. Không hỗ trợ TLS; cần TLS thì dùng SocketRpcClient.
 */
public class NioRpcClient implements RpcProcessor, MessageTransport {
    private final AgentLoop loop;
    private final boolean ownsLoop;
    private final long timeoutMs;
    // chỉ thread của vòng dùng
    private final Map<String, Link> links = new HashMap<>();
    private long nextId;

    /** dùng chung vòng của node (hoặc của một nhóm client) */
    public NioRpcClient(AgentLoop loop, int timeoutMs) {
        this(loop, false, timeoutMs);
    }

    /** tự tạo một vòng riêng */
    public NioRpcClient(int timeoutMs) {
        this(new AgentLoop("raft-nio-client"), true, timeoutMs);
    }

    private NioRpcClient(AgentLoop loop, boolean ownsLoop, int timeoutMs) {
        this.loop = loop;
        this.ownsLoop = ownsLoop;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(String address, RequestVoteRequest request) {
        return send(address, request, RequestVoteResponse.class);
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(String address, AppendEntriesRequest request) {
        return send(address, request, AppendEntriesResponse.class);
    }

    @Override
    public CompletableFuture<PreVoteResponse> preVote(String address, PreVoteRequest request) {
        return send(address, request, PreVoteResponse.class);
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(String address, InstallSnapshotRequest request) {
        return send(address, request, InstallSnapshotResponse.class);
    }

    @Override
    public CompletableFuture<TimeoutNowResponse> timeoutNow(String address, TimeoutNowRequest request) {
        return send(address, request, TimeoutNowResponse.class);
    }

    @Override
    public CompletableFuture<ReadIndexResponse> readIndex(String address, ReadIndexRequest request) {
        return send(address, request, ReadIndexResponse.class);
    }

    @Override
    public <T> CompletableFuture<T> send(String address, Object request, Class<T> responseType) {
        var result = new CompletableFuture<Object>();
        if (loop.inLoop()) {
            doSend(address, request, result);
        } else {
            try {
                loop.execute(() -> doSend(address, request, result));
            } catch (RejectedExecutionException e) {
                result.completeExceptionally(new IOException("transport closed", e));
            }
        }
        return result.orTimeout(timeoutMs, TimeUnit.MILLISECONDS).thenApply(responseType::cast);
    }

    private void doSend(String address, Object request, CompletableFuture<Object> result) {
        Link link;
        try {
            link = linkTo(address);
        } catch (IOException | RuntimeException e) {
            result.completeExceptionally(e);
            return;
        }
        long id = ++nextId;
        link.pending.put(id, result);
        // lời gọi hết hạn thì không giữ chỗ chờ response của nó nữa
        result.whenComplete((value, error) -> link.pending.remove(id));
        try {
            link.connection.send(id, request);
        } catch (RuntimeException e) {
            result.completeExceptionally(e);
        }
    }

    private Link linkTo(String address) throws IOException {
        var link = links.get(address);
        // kết nối mãi không xong (node đích không trả lời SYN) thì bỏ và thử lại
        if (link != null && !link.connection.isClosed() && !link.connection.isConnected()
                && link.connection.ageNanos() > TimeUnit.MILLISECONDS.toNanos(timeoutMs)) {
            link.connection.close(new IOException("connect to " + address + " timed out"));
            link = null;
        }
        if (link == null || link.connection.isClosed()) {
            link = new Link(address);
            links.put(address, link);
        }
        return link;
    }

    @Override
    public void close() {
        try {
            loop.execute(() -> {
                // đóng một kết nối sẽ gỡ nó khỏi links
                for (Link link : List.copyOf(links.values())) {
                    link.connection.close(new IOException("client closed"));
                }
            });
        } catch (RejectedExecutionException e) {
            // vòng đã dừng và đã đóng mọi kênh của nó
        }
        if (ownsLoop) {
            loop.close();
        }
    }

    private final class Link implements NioConnection.Listener {
        private final String address;
        // response về trên thread của vòng, còn lời gọi hết hạn được gỡ từ thread của bộ hẹn giờ
        private final Map<Long, CompletableFuture<Object>> pending = new ConcurrentHashMap<>();
        private final NioConnection connection;

        Link(String address) throws IOException {
            this.address = address;
            int colon = address.lastIndexOf(':');
            var target = new InetSocketAddress(address.substring(0, colon), Integer.parseInt(address.substring(colon + 1)));
            var channel = SocketChannel.open();
            try {
                channel.configureBlocking(false);
                channel.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);
                connection = new NioConnection(loop, channel, this);
                if (channel.connect(target)) {
                    connection.registerConnected();
                } else {
                    connection.registerConnecting();
                }
            } catch (IOException | RuntimeException e) {
                channel.close();
                throw e;
            }
        }

        @Override
        public void onFrame(NioConnection from, RpcCodec.Frame frame) {
            // response của lời gọi đã hết hạn thì không còn ai chờ, bỏ qua
            var response = pending.remove(frame.requestId());
            if (response != null) {
                response.complete(frame.message());
            }
        }

        @Override
        public void onClose(NioConnection from, IOException cause) {
            if (links.get(address) == this) {
                links.remove(address);
            }
            for (var response : pending.values()) {
                response.completeExceptionally(new IOException("connection to " + address + " closed", cause));
            }
            pending.clear();
        }
    }
}

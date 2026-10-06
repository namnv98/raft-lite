package com.namnv.transport.nio;

import com.namnv.agent.AgentLoop;
import com.namnv.rpc.ClientService;
import com.namnv.rpc.RaftServerService;
import com.namnv.rpc.RpcCodec;
import com.namnv.transport.RpcThreads;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.ClientReadRequest;
import com.namnv.rpc.model.request.ClientWriteRequest;
import com.namnv.rpc.model.request.InstallSnapshotRequest;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.ReadIndexRequest;
import com.namnv.rpc.model.request.RequestVoteRequest;
import com.namnv.rpc.model.request.TimeoutNowRequest;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * Server TCP chạy trên {@link AgentLoop} của node: nhận kết nối, đọc request và ghi response đều trên thread của node,
 * như agent của Aeron poll subscription của nó trong duty cycle. Cùng định dạng khung với SocketRpcServer.
 * <p>
 * AppendEntries, ReadIndex và yêu cầu của client chỉ được chuyển vào hàng sự kiện của node rồi trả lời khi xong, nên được
 * xử lý ngay trên vòng. Các RPC hiếm còn lại (RequestVote, PreVote, InstallSnapshot, TimeoutNow) có thể chờ đĩa nên chạy ở
 * thread riêng; thread của vòng không bao giờ chờ đĩa. Không hỗ trợ TLS; cần TLS thì dùng SocketRpcServer.
 */
@Slf4j
public class NioRpcServer implements AutoCloseable {
    private final int port;
    private final RaftServerService raftServerService;
    private final ClientService clientService;
    private final AgentLoop loop;
    private final ExecutorService blockingHandlers = RpcThreads.newExecutor("raft-nio-server-");
    // chỉ thread của vòng dùng
    private final Set<NioConnection> connections = new HashSet<>();
    private ServerSocketChannel serverChannel;

    /**
     * @param clientService nơi xử lý yêu cầu của client bên ngoài; null thì chỉ nhận RPC giữa các node
     * @param loop          vòng của node ({@code ThreadedRuntime.loop()})
     */
    public NioRpcServer(int port, RaftServerService raftServerService, ClientService clientService, AgentLoop loop) {
        this.port = port;
        this.raftServerService = raftServerService;
        this.clientService = clientService;
        this.loop = loop;
    }

    public void start() {
        try {
            serverChannel = ServerSocketChannel.open();
            serverChannel.bind(new InetSocketAddress(port));
        } catch (IOException e) {
            throw new RuntimeException("Failed to start RPC server on port " + port, e);
        }
        var registered = new CompletableFuture<Void>();
        loop.execute(() -> {
            try {
                loop.register(serverChannel, SelectionKey.OP_ACCEPT, key -> accept());
                registered.complete(null);
            } catch (IOException e) {
                registered.completeExceptionally(e);
            }
        });
        registered.join();
        log.info("RPC Server (agent loop) started on port " + port);
    }

    private void accept() {
        try {
            SocketChannel channel;
            while ((channel = serverChannel.accept()) != null) {
                channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                var connection = new NioConnection(loop, channel, listener);
                connection.registerConnected();
                connections.add(connection);
            }
        } catch (IOException e) {
            log.error("RPC Server accept error: " + e.getMessage());
        }
    }

    private final NioConnection.Listener listener = new NioConnection.Listener() {
        @Override
        public void onFrame(NioConnection connection, RpcCodec.Frame frame) {
            handle(connection, frame);
        }

        @Override
        public void onClose(NioConnection connection, IOException cause) {
            connections.remove(connection);
        }
    };

    private void handle(NioConnection connection, RpcCodec.Frame frame) {
        var message = frame.message();
        CompletableFuture<?> pending;
        try {
            if (message instanceof AppendEntriesRequest request) {
                pending = raftServerService.handleAppendEntriesAsync(request);
            } else if (message instanceof ReadIndexRequest request) {
                pending = raftServerService.handleReadIndexRequest(request);
            } else if (clientService != null && message instanceof ClientWriteRequest request) {
                pending = clientService.handleClientWrite(request);
            } else if (clientService != null && message instanceof ClientReadRequest request) {
                pending = clientService.handleClientRead(request);
            } else if (isBlockingRpc(message)) {
                pending = CompletableFuture.supplyAsync(() -> handleBlocking(message), blockingHandlers);
            } else {
                throw new IllegalArgumentException("unexpected message " + message.getClass().getSimpleName());
            }
        } catch (RuntimeException e) {
            // node chưa chạy hoặc message không phải request: đóng kết nối để phía gọi biết ngay thay vì chờ hết hạn
            connection.close(new IOException(e));
            return;
        }
        pending.whenComplete((response, error) -> {
            if (loop.inLoop()) {
                respond(connection, frame.requestId(), response, error);
            } else {
                try {
                    loop.execute(() -> respond(connection, frame.requestId(), response, error));
                } catch (RejectedExecutionException e) {
                    // vòng đã dừng cùng node
                }
            }
        });
    }

    private void respond(NioConnection connection, long requestId, Object response, Throwable error) {
        if (error != null) {
            connection.close(new IOException(error));
        } else {
            connection.send(requestId, response);
        }
    }

    private static boolean isBlockingRpc(Object message) {
        return message instanceof RequestVoteRequest || message instanceof PreVoteRequest
                || message instanceof InstallSnapshotRequest || message instanceof TimeoutNowRequest;
    }

    private Object handleBlocking(Object message) {
        if (message instanceof RequestVoteRequest request) {
            return raftServerService.handleRequestVoteRequest(request);
        }
        if (message instanceof PreVoteRequest request) {
            return raftServerService.handlePreVoteRequest(request);
        }
        if (message instanceof InstallSnapshotRequest request) {
            return raftServerService.handleInstallSnapshotRequest(request);
        }
        return raftServerService.handleTimeoutNowRequest((TimeoutNowRequest) message);
    }

    public void stop() {
        try {
            loop.execute(() -> {
                for (NioConnection connection : Set.copyOf(connections)) {
                    connection.close(new IOException("server stopped"));
                }
                try {
                    serverChannel.close();
                } catch (IOException e) {
                    // Ignore close errors
                }
            });
        } catch (RejectedExecutionException e) {
            // vòng đã dừng và đã đóng mọi kênh của nó
        }
        blockingHandlers.shutdown();
        log.info("RPC Server stopped on port " + port);
    }

    @Override
    public void close() {
        stop();
    }
}

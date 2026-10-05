package com.namnv.rpc.server;

import com.namnv.rpc.ClientService;
import com.namnv.rpc.RaftServerService;
import com.namnv.rpc.RpcCodec;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.ClientReadRequest;
import com.namnv.rpc.model.request.ClientWriteRequest;
import com.namnv.rpc.model.request.InstallSnapshotRequest;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.ReadIndexRequest;
import com.namnv.rpc.model.request.RequestVoteRequest;
import com.namnv.rpc.model.request.TimeoutNowRequest;
import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import java.io.EOFException;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class SocketRpcServer {
    // kết nối im lặng quá lâu thì đóng, client sẽ tự kết nối lại
    private static final int IDLE_TIMEOUT_MS = 60_000;

    private final int port;
    private final RaftServerService raftServerService;
    // null: nhận kết nối không mã hoá
    private final SSLContext sslContext;
    private final ClientService clientService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
    private ServerSocket serverSocket;
    private ExecutorService executor;

    public SocketRpcServer(int port, RaftServerService raftNode) {
        this(port, raftNode, null);
    }

    /**
     * @param sslContext TLS với chứng chỉ của node này; client phải xuất trình chứng chỉ mà truststore của context tin
     */
    public SocketRpcServer(int port, RaftServerService raftNode, SSLContext sslContext) {
        this(port, raftNode, sslContext, null);
    }

    /**
     * @param clientService nơi xử lý yêu cầu của client bên ngoài (thường là RaftClientService); null thì server
     *                      chỉ nhận RPC giữa các node và đóng kết nối khi gặp yêu cầu của client
     */
    public SocketRpcServer(int port, RaftServerService raftNode, SSLContext sslContext, ClientService clientService) {
        this.port = port;
        this.raftServerService = raftNode;
        this.sslContext = sslContext;
        this.clientService = clientService;
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            executor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
            try {
                if (sslContext != null) {
                    var tlsSocket = (SSLServerSocket) sslContext.getServerSocketFactory().createServerSocket(port);
                    // chỉ node có chứng chỉ được tin mới gọi được RPC
                    tlsSocket.setNeedClientAuth(true);
                    serverSocket = tlsSocket;
                } else {
                    serverSocket = new ServerSocket(port);
                }
                log.info("RPC Server started on port " + port);

                executor.submit(() -> {
                    while (running.get()) {
                        try {
                            Socket clientSocket = serverSocket.accept();
                            executor.submit(new ClientHandler(clientSocket));
                        } catch (IOException e) {
                            if (running.get()) {
                                log.error("RPC Server accept error: " + e.getMessage());
                            }
                        }
                    }
                });
            } catch (IOException e) {
                running.set(false);
                executor.shutdown();
                throw new RuntimeException("Failed to start RPC server on port " + port, e);
            }
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            try {
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                log.error("Error closing server socket: " + e.getMessage());
            }
            for (Socket client : clients) {
                try {
                    client.close();
                } catch (IOException e) {
                    // Ignore close errors
                }
            }
            if (executor != null) {
                executor.shutdown();
            }
            log.info("RPC Server stopped on port " + port);
        }
    }

    private class ClientHandler implements Runnable {
        private final Socket socket;
        // các response được ghi từ nhiều thread, mỗi lần một khung trọn vẹn
        private final ReentrantLock writeLock = new ReentrantLock();
        private DataOutputStream out;

        public ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            clients.add(socket);
            try {
                // đặt timeout trước khi đọc: với TLS, lần đọc đầu tiên còn gồm cả handshake
                socket.setSoTimeout(IDLE_TIMEOUT_MS);
                socket.setTcpNoDelay(true);
                DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

                // mỗi request được xử lý ở thread riêng và trả lời theo id của nó, nên một request chậm
                // (ví dụ ReadIndex đang chờ leader xác nhận) không chặn các request khác trên cùng kết nối
                while (running.get()) {
                    var frame = RpcCodec.read(in);
                    executor.submit(() -> handle(frame));
                }
            } catch (EOFException | SocketTimeoutException e) {
                // client đóng kết nối hoặc kết nối idle
            } catch (Exception e) {
                if (running.get()) {
                    log.error("Client handling error: " + e.getMessage());
                }
            } finally {
                close();
            }
        }

        private void handle(RpcCodec.Frame frame) {
            try {
                // các yêu cầu chỉ có câu trả lời sau một lúc được trả lời bất đồng bộ, không giữ thread
                CompletableFuture<?> pending = null;
                if (frame.message() instanceof ReadIndexRequest readIndexRequest) {
                    pending = raftServerService.handleReadIndexRequest(readIndexRequest);
                } else if (clientService != null && frame.message() instanceof ClientWriteRequest write) {
                    pending = clientService.handleClientWrite(write);
                } else if (clientService != null && frame.message() instanceof ClientReadRequest read) {
                    pending = clientService.handleClientRead(read);
                }
                if (pending != null) {
                    // Async: future này được hoàn tất bởi thread của Raft ngay lúc lệnh được apply, khi nó còn đang giữ
                    // lock của node. Ghi response ra socket ngay tại đó sẽ bắt cả node chờ từng lần ghi mạng.
                    pending.whenCompleteAsync((response, error) -> {
                        if (error == null) {
                            respond(frame.requestId(), response);
                        } else {
                            close();
                        }
                    }, executor);
                } else {
                    respond(frame.requestId(), handleCommandRequest(frame.message()));
                }
            } catch (Exception e) {
                // node chưa chạy hoặc message không phải request: đóng kết nối để phía gọi biết ngay thay vì chờ hết hạn
                if (running.get()) {
                    log.error("Client handling error: " + e.getMessage());
                }
                close();
            }
        }

        private void respond(long requestId, Object response) {
            writeLock.lock();
            try {
                RpcCodec.write(out, requestId, response);
            } catch (IOException e) {
                close();
            } finally {
                writeLock.unlock();
            }
        }

        private void close() {
            clients.remove(socket);
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore close errors
            }
        }

        private Object handleCommandRequest(Object request) {
            if (request instanceof RequestVoteRequest requestVoteRequest) {
                return raftServerService.handleRequestVoteRequest(requestVoteRequest);
            }
            if (request instanceof PreVoteRequest preVoteRequest) {
                return raftServerService.handlePreVoteRequest(preVoteRequest);
            }
            if (request instanceof AppendEntriesRequest appendEntriesRequest) {
                return raftServerService.handleAppendEntriesRequest(appendEntriesRequest);
            }
            if (request instanceof InstallSnapshotRequest snapshotRequest) {
                return raftServerService.handleInstallSnapshotRequest(snapshotRequest);
            }
            if (request instanceof TimeoutNowRequest timeoutNowRequest) {
                return raftServerService.handleTimeoutNowRequest(timeoutNowRequest);
            }
            // một response gửi nhầm chiều
            throw new IllegalArgumentException("Not a request: " + request.getClass().getSimpleName());
        }
    }
}

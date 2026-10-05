package com.namnv.rpc.server;

import com.namnv.rpc.RaftServerService;
import com.namnv.rpc.RpcCodec;
import com.namnv.rpc.model.request.AppendEntriesRequest;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class SocketRpcServer {
    // kết nối im lặng quá lâu thì đóng, client sẽ tự kết nối lại
    private static final int IDLE_TIMEOUT_MS = 60_000;
    private static final int READ_INDEX_TIMEOUT_MS = 10_000;

    private final int port;
    private final RaftServerService raftServerService;
    // null: nhận kết nối không mã hoá
    private final SSLContext sslContext;
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
        this.port = port;
        this.raftServerService = raftNode;
        this.sslContext = sslContext;
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

        public ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            clients.add(socket);
            try {
                // đặt timeout trước khi tạo stream: constructor ObjectInputStream đã đọc header
                socket.setSoTimeout(IDLE_TIMEOUT_MS);
                socket.setTcpNoDelay(true);
                DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

                // một kết nối phục vụ nhiều request nối tiếp nhau
                while (running.get()) {
                    Object request = RpcCodec.read(in);
                    RpcCodec.write(out, handleCommandRequest(request));
                }
            } catch (EOFException | SocketTimeoutException e) {
                // client đóng kết nối hoặc kết nối idle
            } catch (Exception e) {
                if (running.get()) {
                    log.error("Client handling error: " + e.getMessage());
                }
            } finally {
                clients.remove(socket);
                try {
                    socket.close();
                } catch (IOException e) {
                    // Ignore close errors
                }
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
            if (request instanceof ReadIndexRequest readIndexRequest) {
                // chờ leader xác nhận xong; quá lâu thì đóng kết nối để phía gọi coi như lỗi
                try {
                    return raftServerService.handleReadIndexRequest(readIndexRequest).get(READ_INDEX_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    throw new IllegalStateException("ReadIndex was not answered", e);
                }
            }
            // một response gửi nhầm chiều: đóng kết nối
            throw new IllegalArgumentException("Not a request: " + request.getClass().getSimpleName());
        }
    }
}

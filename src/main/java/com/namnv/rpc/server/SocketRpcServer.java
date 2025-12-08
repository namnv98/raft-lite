package com.namnv.rpc.server;

import com.namnv.rpc.RaftServerService;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.InstallSnapshotRequest;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.RequestVoteRequest;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class SocketRpcServer {
    private final int port;
    private final RaftServerService raftServerService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ServerSocket serverSocket;
    private ExecutorService executor;

    public SocketRpcServer(int port, RaftServerService raftNode) {
        this.port = port;
        this.raftServerService = raftNode;
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            executor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
            try {
                serverSocket = new ServerSocket(port);
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
            try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream()); ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

                socket.setSoTimeout(5000); // 5 second timeout for RPC operations

                Object request = in.readObject();
                Object response = handleCommandRequest(request);

                out.writeObject(response);
                out.flush();

            } catch (Exception e) {
                log.error("Client handling error: " + e.getMessage());
            } finally {
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
            return new RuntimeException("Unknown request type: " + request.getClass().getSimpleName());
        }
    }
}
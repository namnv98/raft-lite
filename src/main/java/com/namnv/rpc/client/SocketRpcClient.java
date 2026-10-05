package com.namnv.rpc.client;

import com.namnv.rpc.RpcCodec;
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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

public class SocketRpcClient implements RpcProcessor, AutoCloseable {
    private final int timeoutMs;
    // null: kết nối không mã hoá
    private final SSLContext sslContext;
    private final ExecutorService rpcExecutor;
    // mỗi peer một kết nối dùng lại, thay vì mở TCP mới cho từng RPC
    private final Map<String, Connection> connections = new ConcurrentHashMap<>();

    public SocketRpcClient(int timeoutMs) {
        this(timeoutMs, null);
    }

    /**
     * @param sslContext TLS với chứng chỉ của node này; server chỉ nhận client có chứng chỉ mà nó tin
     */
    public SocketRpcClient(int timeoutMs, SSLContext sslContext) {
        this.timeoutMs = timeoutMs;
        this.sslContext = sslContext;
        rpcExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(String address, RequestVoteRequest request) {
        return sendRPC(address, request, RequestVoteResponse.class);
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(String address, AppendEntriesRequest request) {
        return sendRPC(address, request, AppendEntriesResponse.class);
    }

    @Override
    public CompletableFuture<PreVoteResponse> preVote(String address, PreVoteRequest request) {
        return sendRPC(address, request, PreVoteResponse.class);
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(String address, InstallSnapshotRequest request) {
        return sendRPC(address, request, InstallSnapshotResponse.class);
    }

    @Override
    public CompletableFuture<TimeoutNowResponse> timeoutNow(String address, TimeoutNowRequest request) {
        return sendRPC(address, request, TimeoutNowResponse.class);
    }

    @Override
    public CompletableFuture<ReadIndexResponse> readIndex(String address, ReadIndexRequest request) {
        return sendRPC(address, request, ReadIndexResponse.class);
    }

    private <T> CompletableFuture<T> sendRPC(String address, Object request, Class<T> responseType) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Connection connection = connections.computeIfAbsent(address, Connection::new);
                return responseType.cast(connection.call(request));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, rpcExecutor);
    }

    @Override
    public void close() {
        rpcExecutor.shutdownNow();
        connections.values().forEach(Connection::close);
        connections.clear();
    }

    private class Connection {
        private final String address;
        private final ReentrantLock lock = new ReentrantLock();
        private Socket socket;
        private DataOutputStream out;
        private DataInputStream in;

        Connection(String address) {
            this.address = address;
        }

        Object call(Object request) throws IOException {
            lock.lock();
            try {
                boolean reused = socket != null;
                try {
                    return exchange(request);
                } catch (IOException e) {
                    close();
                    if (!reused) {
                        throw e;
                    }
                    // kết nối cũ có thể đã bị phía server đóng, thử lại một lần với kết nối mới
                    try {
                        return exchange(request);
                    } catch (IOException retryError) {
                        close();
                        throw retryError;
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        private Object exchange(Object request) throws IOException {
            if (socket == null) {
                connect();
            }
            RpcCodec.write(out, request);
            return RpcCodec.read(in);
        }

        private void connect() throws IOException {
            String[] parts = address.split(":");
            Socket s = sslContext != null ? sslContext.getSocketFactory().createSocket() : new Socket();
            try {
                s.connect(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])), timeoutMs);
                s.setSoTimeout(timeoutMs);
                s.setTcpNoDelay(true);
                if (s instanceof SSLSocket tls) {
                    tls.startHandshake();
                }
                out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
                socket = s;
            } catch (IOException e) {
                s.close();
                throw e;
            }
        }

        void close() {
            lock.lock();
            try {
                if (socket != null) {
                    socket.close();
                }
            } catch (IOException e) {
                // Ignore close errors
            } finally {
                socket = null;
                out = null;
                in = null;
                lock.unlock();
            }
        }
    }
}

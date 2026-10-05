package com.namnv.rpc.client;

import com.namnv.rpc.RpcSerialization;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.InstallSnapshotRequest;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.RequestVoteRequest;
import com.namnv.rpc.model.request.TimeoutNowRequest;
import com.namnv.rpc.model.response.AppendEntriesResponse;
import com.namnv.rpc.model.response.InstallSnapshotResponse;
import com.namnv.rpc.model.response.PreVoteResponse;
import com.namnv.rpc.model.response.RequestVoteResponse;
import com.namnv.rpc.model.response.TimeoutNowResponse;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
    private final ExecutorService rpcExecutor;
    // mỗi peer một kết nối dùng lại, thay vì mở TCP mới cho từng RPC
    private final Map<String, Connection> connections = new ConcurrentHashMap<>();

    public SocketRpcClient(int timeoutMs) {
        this.timeoutMs = timeoutMs;
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
        private ObjectOutputStream out;
        private ObjectInputStream in;

        Connection(String address) {
            this.address = address;
        }

        Object call(Object request) throws IOException, ClassNotFoundException {
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

        private Object exchange(Object request) throws IOException, ClassNotFoundException {
            if (socket == null) {
                connect();
            }
            out.writeObject(request);
            out.flush();
            out.reset(); // không giữ tham chiếu tới các object đã gửi
            return in.readObject();
        }

        private void connect() throws IOException {
            String[] parts = address.split(":");
            Socket s = new Socket();
            try {
                s.connect(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])), timeoutMs);
                s.setSoTimeout(timeoutMs);
                s.setTcpNoDelay(true);
                out = new ObjectOutputStream(s.getOutputStream());
                out.flush();
                in = new ObjectInputStream(s.getInputStream());
                in.setObjectInputFilter(RpcSerialization.FILTER);
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

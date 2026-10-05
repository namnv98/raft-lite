package com.namnv.rpc;

import com.namnv.entity.ClientSession;
import com.namnv.entity.ConfigurationEntry;
import com.namnv.entity.LogEntry;
import com.namnv.rpc.model.request.AppendEntriesRequest;
import com.namnv.rpc.model.request.ClientReadRequest;
import com.namnv.rpc.model.request.ClientWriteRequest;
import com.namnv.rpc.model.request.InstallSnapshotRequest;
import com.namnv.rpc.model.request.PreVoteRequest;
import com.namnv.rpc.model.request.ReadIndexRequest;
import com.namnv.rpc.model.request.RequestVoteRequest;
import com.namnv.rpc.model.request.TimeoutNowRequest;
import com.namnv.rpc.model.response.AppendEntriesResponse;
import com.namnv.rpc.model.response.ClientReadResponse;
import com.namnv.rpc.model.response.ClientWriteResponse;
import com.namnv.rpc.model.response.InstallSnapshotResponse;
import com.namnv.rpc.model.response.PreVoteResponse;
import com.namnv.rpc.model.response.ReadIndexResponse;
import com.namnv.rpc.model.response.RequestVoteResponse;
import com.namnv.rpc.model.response.TimeoutNowResponse;
import com.namnv.storage.binary.EntryFrame;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Định dạng trên dây của transport socket. Mỗi message là một khung
 * {@code [4 byte độ dài][8 byte id của request][1 byte loại message][nội dung nhị phân]}.
 * Response mang cùng id với request của nó, nên nhiều lời gọi chạy xen kẽ được trên một kết nối.
 * <p>
 * Nội dung được mã hoá bằng tay theo từng trường. Bên nhận chỉ dựng được đúng 16 loại message dưới đây,
 * mọi độ dài đọc vào đều được đối chiếu với số byte còn lại, nên dữ liệu rác không thể khiến nó khởi tạo class tuỳ ý
 * hay cấp phát bộ nhớ theo một con số bịa.
 */
public final class RpcCodec {
    // một khung lớn hơn mức này bị coi là rác (hoặc tấn công) và kết nối bị đóng
    public static final int MAX_FRAME_BYTES = 64 << 20;
    private static final int HEADER_BYTES = Long.BYTES + 1;

    // mã loại trên dây: chỉ được thêm số mới, không đổi số cũ
    private static final int PRE_VOTE_REQUEST = 0;
    private static final int PRE_VOTE_RESPONSE = 1;
    private static final int REQUEST_VOTE_REQUEST = 2;
    private static final int REQUEST_VOTE_RESPONSE = 3;
    private static final int APPEND_ENTRIES_REQUEST = 4;
    private static final int APPEND_ENTRIES_RESPONSE = 5;
    private static final int INSTALL_SNAPSHOT_REQUEST = 6;
    private static final int INSTALL_SNAPSHOT_RESPONSE = 7;
    private static final int TIMEOUT_NOW_REQUEST = 8;
    private static final int TIMEOUT_NOW_RESPONSE = 9;
    private static final int READ_INDEX_REQUEST = 10;
    private static final int READ_INDEX_RESPONSE = 11;
    private static final int CLIENT_WRITE_REQUEST = 12;
    private static final int CLIENT_WRITE_RESPONSE = 13;
    private static final int CLIENT_READ_REQUEST = 14;
    private static final int CLIENT_READ_RESPONSE = 15;

    public record Frame(long requestId, Object message) {
    }

    private RpcCodec() {
    }

    public static void write(DataOutputStream out, long requestId, Object message) throws IOException {
        writeFrame(out, requestId, message, new ByteArrayOutputStream());
        out.flush();
    }

    /**
     * Ghi một khung mà không flush, để người gọi gom nhiều khung vào một lần ghi ra socket.
     * {@code scratch} là bộ đệm dùng lại giữa các lần gọi của cùng một thread, nội dung cũ của nó bị bỏ.
     */
    public static void writeFrame(DataOutputStream out, long requestId, Object message, ByteArrayOutputStream scratch)
            throws IOException {
        scratch.reset();
        int type = encode(new DataOutputStream(scratch), message);
        if (scratch.size() + HEADER_BYTES > MAX_FRAME_BYTES) {
            throw new IOException("RPC message of " + scratch.size() + " bytes exceeds the frame limit");
        }
        out.writeInt(scratch.size() + HEADER_BYTES);
        out.writeLong(requestId);
        out.writeByte(type);
        scratch.writeTo(out);
    }

    // Nội dung được giải mã thẳng từ stream: một lệnh lớn chỉ được chép một lần, từ socket vào mảng byte cuối cùng của nó.
    // Khi khung sai định dạng, stream bị bỏ dở giữa khung nên người gọi phải đóng kết nối.
    public static Frame read(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < HEADER_BYTES || length > MAX_FRAME_BYTES) {
            throw new IOException("Invalid RPC frame length " + length);
        }
        long requestId = in.readLong();
        int type = in.readUnsignedByte();
        return new Frame(requestId, decode(type, new Reader(in, length - HEADER_BYTES)));
    }

    // ---------- mã hoá ----------

    static int encode(DataOutputStream out, Object message) throws IOException {
        if (message instanceof PreVoteRequest m) {
            out.writeLong(m.term);
            writeString(out, m.candidateId);
            out.writeLong(m.lastLogIndex);
            out.writeLong(m.lastLogTerm);
            return PRE_VOTE_REQUEST;
        }
        if (message instanceof PreVoteResponse m) {
            out.writeLong(m.term);
            out.writeBoolean(m.voteGranted);
            return PRE_VOTE_RESPONSE;
        }
        if (message instanceof RequestVoteRequest m) {
            out.writeLong(m.term);
            writeString(out, m.candidateId);
            out.writeLong(m.lastLogIndex);
            out.writeLong(m.lastLogTerm);
            return REQUEST_VOTE_REQUEST;
        }
        if (message instanceof RequestVoteResponse m) {
            out.writeLong(m.term);
            out.writeBoolean(m.voteGranted);
            return REQUEST_VOTE_RESPONSE;
        }
        if (message instanceof AppendEntriesRequest m) {
            out.writeLong(m.term);
            writeString(out, m.leaderId);
            out.writeLong(m.prevLogIndex);
            out.writeLong(m.prevLogTerm);
            out.writeLong(m.leaderCommit);
            // các entry đi dưới dạng khung của file log: leader có sẵn block thì chỉ việc chép vùng byte đó ra
            out.writeInt(m.entryCount());
            if (m.block != null) {
                byte[] frames = m.block.copy();
                out.writeInt(frames.length);
                out.write(frames);
            } else {
                ByteBuffer frames = EntryFrame.encodeAll(m.entries != null ? m.entries : List.of());
                out.writeInt(frames.capacity());
                out.write(frames.array(), 0, frames.capacity());
            }
            return APPEND_ENTRIES_REQUEST;
        }
        if (message instanceof AppendEntriesResponse m) {
            out.writeLong(m.term);
            out.writeBoolean(m.success);
            out.writeLong(m.matchIndex);
            return APPEND_ENTRIES_RESPONSE;
        }
        if (message instanceof InstallSnapshotRequest m) {
            out.writeLong(m.getTerm());
            writeString(out, m.getLeaderId());
            out.writeLong(m.getLastIncludedIndex());
            out.writeLong(m.getLastIncludedTerm());
            writeConfiguration(out, m.getConf());
            writeSessions(out, m.getSessions());
            writeStrings(out, m.getFiles() != null ? m.getFiles() : List.of());
            writeString(out, m.getFileName());
            out.writeLong(m.getOffset());
            writeBytes(out, m.getData());
            out.writeBoolean(m.isDone());
            return INSTALL_SNAPSHOT_REQUEST;
        }
        if (message instanceof InstallSnapshotResponse m) {
            out.writeLong(m.getTerm());
            out.writeBoolean(m.isSuccess());
            out.writeBoolean(m.isComplete());
            return INSTALL_SNAPSHOT_RESPONSE;
        }
        if (message instanceof TimeoutNowRequest m) {
            out.writeLong(m.term);
            writeString(out, m.leaderId);
            return TIMEOUT_NOW_REQUEST;
        }
        if (message instanceof TimeoutNowResponse m) {
            out.writeLong(m.term);
            out.writeBoolean(m.success);
            return TIMEOUT_NOW_RESPONSE;
        }
        if (message instanceof ReadIndexRequest m) {
            writeString(out, m.requesterId);
            return READ_INDEX_REQUEST;
        }
        if (message instanceof ReadIndexResponse m) {
            out.writeBoolean(m.success);
            out.writeLong(m.readIndex);
            writeString(out, m.leaderId);
            return READ_INDEX_RESPONSE;
        }
        if (message instanceof ClientWriteRequest m) {
            writeString(out, m.clientId);
            out.writeLong(m.sequence);
            writeBytes(out, m.command);
            return CLIENT_WRITE_REQUEST;
        }
        if (message instanceof ClientWriteResponse m) {
            out.writeBoolean(m.success);
            writeString(out, m.leaderId);
            return CLIENT_WRITE_RESPONSE;
        }
        if (message instanceof ClientReadRequest m) {
            writeBytes(out, m.query);
            return CLIENT_READ_REQUEST;
        }
        if (message instanceof ClientReadResponse m) {
            out.writeBoolean(m.success);
            writeString(out, m.leaderId);
            writeBytes(out, m.result);
            return CLIENT_READ_RESPONSE;
        }
        throw new IOException("Not an RPC message: " + message.getClass().getName());
    }

    // -1 thay cho độ dài nghĩa là null
    private static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        writeBytes(out, value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }

    private static void writeStrings(DataOutputStream out, List<String> values) throws IOException {
        out.writeInt(values.size());
        for (String value : values) {
            writeString(out, value);
        }
    }

    private static void writeConfiguration(DataOutputStream out, ConfigurationEntry conf) throws IOException {
        out.writeBoolean(conf != null);
        if (conf != null) {
            out.writeBoolean(conf.isJoint());
            writeStrings(out, conf.getOldNodes());
            writeStrings(out, conf.getNewNodes());
        }
    }

    private static void writeSessions(DataOutputStream out, Map<String, ClientSession> sessions) throws IOException {
        if (sessions == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(sessions.size());
        for (var session : sessions.entrySet()) {
            writeString(out, session.getKey());
            out.writeLong(session.getValue().getWatermark());
            out.writeInt(session.getValue().getAbove().size());
            for (long sequence : session.getValue().getAbove()) {
                out.writeLong(sequence);
            }
        }
    }

    // ---------- giải mã ----------

    static Object decode(int type, byte[] body) throws IOException {
        return decode(type, new Reader(new DataInputStream(new ByteArrayInputStream(body)), body.length));
    }

    private static Object decode(int type, Reader in) throws IOException {
        try {
            Object message = switch (type) {
                case PRE_VOTE_REQUEST -> new PreVoteRequest(in.readLong(), in.readString(), in.readLong(), in.readLong());
                case PRE_VOTE_RESPONSE -> new PreVoteResponse(in.readLong(), in.readBoolean());
                case REQUEST_VOTE_REQUEST -> new RequestVoteRequest(in.readLong(), in.readString(), in.readLong(), in.readLong());
                case REQUEST_VOTE_RESPONSE -> new RequestVoteResponse(in.readLong(), in.readBoolean());
                case APPEND_ENTRIES_REQUEST -> readAppendEntries(in);
                case APPEND_ENTRIES_RESPONSE -> new AppendEntriesResponse(in.readLong(), in.readBoolean(), in.readLong());
                case INSTALL_SNAPSHOT_REQUEST -> new InstallSnapshotRequest(in.readLong(), in.readString(), in.readLong(),
                        in.readLong(), in.readConfiguration(), in.readSessions(), in.readStrings(), in.readString(),
                        in.readLong(), in.readBytes(), in.readBoolean());
                case INSTALL_SNAPSHOT_RESPONSE -> new InstallSnapshotResponse(in.readLong(), in.readBoolean(), in.readBoolean());
                case TIMEOUT_NOW_REQUEST -> new TimeoutNowRequest(in.readLong(), in.readString());
                case TIMEOUT_NOW_RESPONSE -> new TimeoutNowResponse(in.readLong(), in.readBoolean());
                case READ_INDEX_REQUEST -> new ReadIndexRequest(in.readString());
                case READ_INDEX_RESPONSE -> new ReadIndexResponse(in.readBoolean(), in.readLong(), in.readString());
                case CLIENT_WRITE_REQUEST -> new ClientWriteRequest(in.readString(), in.readLong(), in.readBytes());
                case CLIENT_WRITE_RESPONSE -> new ClientWriteResponse(in.readBoolean(), in.readString());
                case CLIENT_READ_REQUEST -> new ClientReadRequest(in.readBytes());
                case CLIENT_READ_RESPONSE -> new ClientReadResponse(in.readBoolean(), in.readString(), in.readBytes());
                default -> throw new IOException("Unknown RPC message type " + type);
            };
            if (in.remaining() != 0) {
                throw new IOException(in.remaining() + " unexpected bytes after RPC message of type " + type);
            }
            return message;
        } catch (RuntimeException e) {
            // khung bị cắt cụt hoặc chứa độ dài sai
            throw new IOException("Malformed RPC message of type " + type, e);
        }
    }

    private static AppendEntriesRequest readAppendEntries(Reader in) throws IOException {
        long term = in.readLong();
        String leaderId = in.readString();
        long prevLogIndex = in.readLong();
        long prevLogTerm = in.readLong();
        long leaderCommit = in.readLong();
        int count = in.readCount();
        byte[] frames = in.readBytes();
        if (frames == null) {
            throw new IOException("AppendEntries without entry frames");
        }
        List<LogEntry> entries = EntryFrame.readAll(ByteBuffer.wrap(frames), count);
        return new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    // đọc từng trường và từ chối mọi độ dài lớn hơn số byte còn lại của khung
    private static final class Reader {
        private final DataInputStream in;
        private int remaining;

        Reader(DataInputStream in, int length) {
            this.in = in;
            this.remaining = length;
        }

        int remaining() {
            return remaining;
        }

        private void take(int bytes) throws IOException {
            if (bytes > remaining) {
                throw new IOException("RPC message is shorter than its fields");
            }
            remaining -= bytes;
        }

        long readLong() throws IOException {
            take(Long.BYTES);
            return in.readLong();
        }

        int readInt() throws IOException {
            take(Integer.BYTES);
            return in.readInt();
        }

        boolean readBoolean() throws IOException {
            take(1);
            return in.readByte() != 0;
        }

        // số phần tử của một danh sách: mỗi phần tử chiếm ít nhất một byte nên không thể nhiều hơn số byte còn lại
        int readCount() throws IOException {
            int count = readInt();
            if (count < 0 || count > remaining) {
                throw new IOException("Invalid element count " + count);
            }
            return count;
        }

        byte[] readBytes() throws IOException {
            int length = readInt();
            if (length == -1) {
                return null;
            }
            if (length < 0 || length > remaining) {
                throw new IOException("Invalid field length " + length);
            }
            take(length);
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            return bytes;
        }

        String readString() throws IOException {
            byte[] bytes = readBytes();
            return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
        }

        List<String> readStrings() throws IOException {
            int count = readCount();
            List<String> values = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                values.add(readString());
            }
            return values;
        }

        ConfigurationEntry readConfiguration() throws IOException {
            if (!readBoolean()) {
                return null;
            }
            boolean joint = readBoolean();
            return new ConfigurationEntry(readStrings(), readStrings(), joint);
        }

        Map<String, ClientSession> readSessions() throws IOException {
            int count = readInt();
            if (count == -1) {
                return null;
            }
            if (count < 0 || count > remaining) {
                throw new IOException("Invalid session count " + count);
            }
            Map<String, ClientSession> sessions = new HashMap<>();
            for (int i = 0; i < count; i++) {
                String clientId = readString();
                ClientSession session = new ClientSession();
                session.setWatermark(readLong());
                int above = readCount();
                TreeSet<Long> sequences = new TreeSet<>();
                for (int j = 0; j < above; j++) {
                    sequences.add(readLong());
                }
                session.setAbove(sequences);
                sessions.put(clientId, session);
            }
            return sessions;
        }
    }
}

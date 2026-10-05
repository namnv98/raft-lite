package com.namnv.rpc;

import com.namnv.entity.ClientSession;
import com.namnv.entity.ConfigurationEntry;
import com.namnv.entity.LogEntry;
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
 * Nội dung được mã hoá bằng tay theo từng trường. Bên nhận chỉ dựng được đúng 12 loại message dưới đây,
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

    public record Frame(long requestId, Object message) {
    }

    private RpcCodec() {
    }

    public static void write(DataOutputStream out, long requestId, Object message) throws IOException {
        var body = new ByteArrayOutputStream();
        int type = encode(new DataOutputStream(body), message);
        if (body.size() + HEADER_BYTES > MAX_FRAME_BYTES) {
            throw new IOException("RPC message of " + body.size() + " bytes exceeds the frame limit");
        }
        out.writeInt(body.size() + HEADER_BYTES);
        out.writeLong(requestId);
        out.writeByte(type);
        body.writeTo(out);
        out.flush();
    }

    public static Frame read(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < HEADER_BYTES || length > MAX_FRAME_BYTES) {
            throw new IOException("Invalid RPC frame length " + length);
        }
        long requestId = in.readLong();
        int type = in.readUnsignedByte();
        byte[] body = new byte[length - HEADER_BYTES];
        in.readFully(body);
        return new Frame(requestId, decode(type, body));
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
            List<LogEntry> entries = m.entries != null ? m.entries : List.of();
            out.writeInt(entries.size());
            for (LogEntry entry : entries) {
                writeEntry(out, entry);
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

    private static void writeEntry(DataOutputStream out, LogEntry entry) throws IOException {
        out.writeLong(entry.getIndex());
        out.writeLong(entry.getTerm());
        out.writeBoolean(entry.isConfigurationEntry());
        out.writeBoolean(entry.isSessionClose());
        writeBytes(out, entry.getCommand());
        writeConfiguration(out, entry.getConfiguration());
        writeString(out, entry.getClientId());
        out.writeLong(entry.getSequence());
    }

    // ---------- giải mã ----------

    static Object decode(int type, byte[] body) throws IOException {
        var in = new Reader(body);
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
        List<LogEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            LogEntry entry = new LogEntry();
            entry.setIndex(in.readLong());
            entry.setTerm(in.readLong());
            entry.setConfigurationEntry(in.readBoolean());
            entry.setSessionClose(in.readBoolean());
            entry.setCommand(in.readBytes());
            entry.setConfiguration(in.readConfiguration());
            entry.setClientId(in.readString());
            entry.setSequence(in.readLong());
            entries.add(entry);
        }
        return new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    // đọc từng trường và từ chối mọi độ dài lớn hơn số byte còn lại
    private static final class Reader {
        private final ByteBuffer buffer;

        Reader(byte[] body) {
            this.buffer = ByteBuffer.wrap(body);
        }

        int remaining() {
            return buffer.remaining();
        }

        long readLong() {
            return buffer.getLong();
        }

        boolean readBoolean() {
            return buffer.get() != 0;
        }

        // số phần tử của một danh sách: mỗi phần tử chiếm ít nhất một byte nên không thể nhiều hơn số byte còn lại
        int readCount() throws IOException {
            int count = buffer.getInt();
            if (count < 0 || count > buffer.remaining()) {
                throw new IOException("Invalid element count " + count);
            }
            return count;
        }

        byte[] readBytes() throws IOException {
            int length = buffer.getInt();
            if (length == -1) {
                return null;
            }
            if (length < 0 || length > buffer.remaining()) {
                throw new IOException("Invalid field length " + length);
            }
            byte[] bytes = new byte[length];
            buffer.get(bytes);
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
            int count = buffer.getInt();
            if (count == -1) {
                return null;
            }
            if (count < 0 || count > buffer.remaining()) {
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

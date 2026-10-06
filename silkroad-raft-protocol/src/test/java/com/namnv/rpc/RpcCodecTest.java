package com.namnv.rpc;

import com.namnv.storage.binary.EntryFrame;

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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcCodecTest {

    private static List<Object> everyMessage() {
        var joint = new ConfigurationEntry(List.of("A", "B", "C"), List.of("A", "D"), true);
        var session = new ClientSession();
        session.markApplied(1);
        session.markApplied(2);
        session.markApplied(5);
        session.markApplied(9);
        var sessions = new HashMap<String, ClientSession>(Map.of("client-1", session, "client-2", new ClientSession()));
        var entries = List.of(
                new LogEntry(8, 6, "xin chào".getBytes(StandardCharsets.UTF_8), "client-1", 42),
                new LogEntry(9, 6, null),
                new LogEntry(10, 6, new byte[0]),
                LogEntry.newConfigurationEntry(11, 7, joint),
                LogEntry.newSessionClose(12, 7, "client-2"));
        return List.of(
                new PreVoteRequest(3, "A", 7, 2),
                new PreVoteResponse(3, true),
                new RequestVoteRequest(Long.MAX_VALUE, "node-ấ", 7, 2),
                new RequestVoteResponse(4, false),
                new AppendEntriesRequest(6, "A", 7, 2, entries, 5),
                new AppendEntriesRequest(6, "A", 7, 2, List.of(), 5),
                new AppendEntriesResponse(6, true, 12),
                new InstallSnapshotRequest(7, "A", 10, 6, joint, sessions, List.of("a.data", "b.data"), "b.data",
                        4096, new byte[]{1, 2, 3, -1}, true),
                new InstallSnapshotRequest(7, "A", 10, 6, null, null, List.of(), null, 0, new byte[0], true),
                new InstallSnapshotResponse(7, true, false),
                new TimeoutNowRequest(8, "A"),
                new TimeoutNowResponse(8, true),
                new ReadIndexRequest("B"),
                new ReadIndexResponse(true, 99, null),
                new ReadIndexResponse(false, 0, "C"),
                new ClientWriteRequest("client-1", 42, "set x=1".getBytes(StandardCharsets.UTF_8)),
                new ClientWriteRequest(null, 0, new byte[0]),
                new ClientWriteResponse(false, "localhost:8081"),
                new ClientReadRequest(new byte[]{9, 8, 7}),
                new ClientReadResponse(true, null, "kết quả".getBytes(StandardCharsets.UTF_8)),
                new ClientReadResponse(false, "localhost:8081", null),
                new ClientWriteRequest("client-2", 43, com.namnv.entity.CommandBatch.encode(
                        List.of("a".getBytes(StandardCharsets.UTF_8), new byte[0], new byte[]{1, 2})), true));
    }

    private static byte[] frame(long requestId, Object message) throws IOException {
        var bytes = new ByteArrayOutputStream();
        RpcCodec.write(new DataOutputStream(bytes), requestId, message);
        return bytes.toByteArray();
    }

    private static RpcCodec.Frame parse(byte[] frame) throws IOException {
        return RpcCodec.read(new DataInputStream(new ByteArrayInputStream(frame)));
    }

    @Test
    void everyMessageSurvivesARoundTrip() throws IOException {
        long requestId = 1;
        for (Object message : everyMessage()) {
            var encoded = frame(requestId, message);
            var decoded = parse(encoded);
            assertEquals(requestId, decoded.requestId());
            assertEquals(message.getClass(), decoded.message().getClass());
            // mã hoá lại bản vừa giải mã phải cho đúng từng byte: không trường nào bị mất hay đổi
            assertArrayEquals(encoded, frame(requestId, decoded.message()), message.getClass().getSimpleName());
            requestId += 1_000_000_007L;
        }
    }

    // leader gửi đúng các khung mà log đã dựng (block), không mã hoá lại; follower phải nhận lại đúng các entry
    @Test
    void blocksFromTheLogCrossTheWire(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws IOException {
        var log = new com.namnv.storage.binary.BinaryLogStorage(com.namnv.storage.binary.LogOptions.builder()
                .logUri(dir.toString()).logSegmentBytes(4096).build(), 0, 0);
        var written = new java.util.ArrayList<com.namnv.entity.LogEntry>();
        for (int i = 1; i <= 40; i++) {
            written.add(new com.namnv.entity.LogEntry(i, 3, ("cmd" + i).getBytes(StandardCharsets.UTF_8)));
        }
        written.add(new com.namnv.entity.LogEntry(41, 3, new byte[]{7}, "client-1", 9));
        written.add(com.namnv.entity.LogEntry.newConfigurationEntry(42, 3,
                new com.namnv.entity.ConfigurationEntry(List.of("a", "b"))));
        log.appendEntries(written);

        var received = new java.util.ArrayList<com.namnv.entity.LogEntry>();
        while (received.size() < written.size()) {
            long next = received.size() + 1;
            var block = log.readBlock(next, 10);
            var request = new AppendEntriesRequest(3, "leader", next - 1, 3, block, 0);
            assertEquals(block.count(), request.entryCount());
            assertEquals(written.subList(received.size(), received.size() + block.count()), request.entries());
            var decoded = (AppendEntriesRequest) parse(frame(1, request)).message();
            assertEquals(next - 1, decoded.prevLogIndex);
            received.addAll(decoded.entries());
        }
        assertEquals(written, received);
        log.close();
    }

    // transport NIO giải mã khung ngay trong bộ đệm đọc của nó, nơi khung nằm giữa các khung khác
    @Test
    void inPlaceDecodingMatchesStreamDecoding() throws IOException {
        long requestId = 7;
        for (Object message : everyMessage()) {
            var encoded = frame(requestId, message);
            var buffer = ByteBuffer.allocate(encoded.length + 20);
            buffer.put(new byte[]{1, 2, 3, 4, 5, 6, 7}).put(encoded).put(new byte[13]);
            buffer.position(3).limit(7 + encoded.length + 5);
            var decoded = RpcCodec.decode(buffer, 7);
            assertEquals(3, buffer.position(), "decoding must not move the buffer");
            assertEquals(requestId, decoded.requestId());
            assertArrayEquals(encoded, frame(requestId, decoded.message()), message.getClass().getSimpleName());
        }
    }

    // follower ghi thẳng vào log khung mà mỗi entry nhận được giữ lại, nên khung đó phải đúng là của entry
    @Test
    void receivedEntriesKeepTheirOwnFrames() throws IOException {
        var original = (AppendEntriesRequest) everyMessage().get(4);
        var encoded = frame(1, original);
        for (var decoded : List.of(parse(encoded).message(), RpcCodec.decode(ByteBuffer.wrap(encoded), 0).message())) {
            var entries = ((AppendEntriesRequest) decoded).entries();
            assertEquals(original.entries().size(), entries.size());
            for (int i = 0; i < entries.size(); i++) {
                var frame = entries.get(i).getFrame();
                assertNotNull(frame);
                assertEquals(0, frame.length % 32, "frames keep their padding");
                assertEquals(entries.get(i), EntryFrame.readAll(ByteBuffer.wrap(frame), 1).get(0));
                assertEquals(original.entries().get(i), entries.get(i));
            }
        }
    }

    @Test
    void fieldsKeepTheirValuesIncludingNullAndEmpty() throws IOException {
        var append = (AppendEntriesRequest) parse(frame(1, everyMessage().get(4))).message();
        assertEquals(5, append.leaderCommit);
        assertEquals("xin chào", new String(append.entries.get(0).getCommand(), StandardCharsets.UTF_8));
        assertEquals("client-1", append.entries.get(0).getClientId());
        assertEquals(42, append.entries.get(0).getSequence());
        // null và mảng rỗng là hai thứ khác nhau: no-op có command null
        assertNull(append.entries.get(1).getCommand());
        assertEquals(0, append.entries.get(2).getCommand().length);
        assertTrue(append.entries.get(3).getConfiguration().isJoint());
        assertEquals(List.of("A", "D"), append.entries.get(3).getConfiguration().getNewNodes());
        assertTrue(append.entries.get(4).isSessionClose());

        var snapshot = (InstallSnapshotRequest) parse(frame(1, everyMessage().get(7))).message();
        assertEquals(2, snapshot.getSessions().get("client-1").getWatermark());
        assertEquals(List.of(5L, 9L), List.copyOf(snapshot.getSessions().get("client-1").getAbove()));
        assertEquals(4096, snapshot.getOffset());

        var empty = (InstallSnapshotRequest) parse(frame(1, everyMessage().get(8))).message();
        assertNull(empty.getConf());
        assertNull(empty.getSessions());
        assertNull(empty.getFileName());
    }

    @Test
    void truncatedOrPaddedMessagesAreRejected() throws IOException {
        for (Object message : everyMessage()) {
            var body = new ByteArrayOutputStream();
            int type = RpcCodec.encode(new DataOutputStream(body), message);
            var bytes = body.toByteArray();
            // thiếu byte ở bất kỳ chỗ nào, hoặc thừa byte ở cuối
            for (int length = 0; length < bytes.length; length++) {
                var cut = Arrays.copyOf(bytes, length);
                assertThrows(IOException.class, () -> RpcCodec.decode(type, cut),
                        message.getClass().getSimpleName() + " cut to " + length + " bytes");
            }
            assertThrows(IOException.class, () -> RpcCodec.decode(type, Arrays.copyOf(bytes, bytes.length + 1)));
        }
        assertThrows(IOException.class, () -> RpcCodec.decode(99, new byte[0]));
    }

    @Test
    void randomBytesNeverCauseAnythingButARejection() {
        var random = new Random(42);
        for (int i = 0; i < 20_000; i++) {
            var body = new byte[random.nextInt(200)];
            random.nextBytes(body);
            int type = random.nextInt(18);
            try {
                RpcCodec.decode(type, body);
            } catch (IOException expected) {
                // mọi lỗi khác (tràn bộ nhớ, lỗi chỉ số...) sẽ làm test fail
            }
            // cùng các byte đó qua đường giải mã tại chỗ, với phần đầu khung hợp lệ
            var framed = ByteBuffer.allocate(13 + body.length);
            framed.putInt(9 + body.length).putLong(i).put((byte) type).put(body);
            try {
                RpcCodec.decode(framed, 0);
            } catch (IOException expected) {
                // như trên
            }
        }
    }

    @Test
    void lengthFieldsCannotForceHugeAllocations() {
        // một trường khai dài 2 tỉ byte trong khung chỉ có vài byte
        var body = new ByteArrayOutputStream();
        var out = new DataOutputStream(body);
        assertThrows(IOException.class, () -> {
            out.writeLong(1);
            out.writeInt(Integer.MAX_VALUE);
            RpcCodec.decode(0, body.toByteArray());
        });
        // khung khai độ dài vô lý
        for (int length : new int[]{-1, 0, 8, RpcCodec.MAX_FRAME_BYTES + 1, Integer.MAX_VALUE}) {
            var frame = new ByteArrayOutputStream();
            assertThrows(IOException.class, () -> {
                new DataOutputStream(frame).writeInt(length);
                parse(frame.toByteArray());
            });
        }
    }
}

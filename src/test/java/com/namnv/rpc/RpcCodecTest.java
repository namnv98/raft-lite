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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
                new ReadIndexResponse(false, 0, "C"));
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
            int type = random.nextInt(14);
            try {
                RpcCodec.decode(type, body);
            } catch (IOException expected) {
                // mọi lỗi khác (tràn bộ nhớ, lỗi chỉ số...) sẽ làm test fail
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

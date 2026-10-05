package com.namnv.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Định dạng trên dây của transport socket: mỗi message là một khung
 * {@code [4 byte độ dài][1 byte loại message][JSON]}.
 * Chỉ các loại message trong danh sách dưới đây được đọc, và mỗi loại chỉ được đọc thành đúng class của nó,
 * nên bên gửi không thể khiến bên nhận khởi tạo một class tuỳ ý.
 */
public final class RpcCodec {
    // một khung lớn hơn mức này bị coi là rác (hoặc tấn công) và kết nối bị đóng
    public static final int MAX_FRAME_BYTES = 64 << 20;

    // vị trí trong danh sách là mã loại trên dây: chỉ được thêm vào cuối
    private static final List<Class<?>> TYPES = List.of(
            PreVoteRequest.class, PreVoteResponse.class,
            RequestVoteRequest.class, RequestVoteResponse.class,
            AppendEntriesRequest.class, AppendEntriesResponse.class,
            InstallSnapshotRequest.class, InstallSnapshotResponse.class,
            TimeoutNowRequest.class, TimeoutNowResponse.class,
            ReadIndexRequest.class, ReadIndexResponse.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private RpcCodec() {
    }

    public static void write(DataOutputStream out, Object message) throws IOException {
        int type = TYPES.indexOf(message.getClass());
        if (type < 0) {
            throw new IOException("Not an RPC message: " + message.getClass().getName());
        }
        byte[] json = MAPPER.writeValueAsBytes(message);
        if (json.length + 1 > MAX_FRAME_BYTES) {
            throw new IOException("RPC message of " + json.length + " bytes exceeds the frame limit");
        }
        out.writeInt(json.length + 1);
        out.writeByte(type);
        out.write(json);
        out.flush();
    }

    public static Object read(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 1 || length > MAX_FRAME_BYTES) {
            throw new IOException("Invalid RPC frame length " + length);
        }
        int type = in.readUnsignedByte();
        if (type >= TYPES.size()) {
            throw new IOException("Unknown RPC message type " + type);
        }
        byte[] json = new byte[length - 1];
        in.readFully(json);
        return MAPPER.readValue(json, TYPES.get(type));
    }
}

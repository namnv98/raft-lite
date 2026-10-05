package com.namnv.rpc.model.request;

import com.namnv.entity.ClientSession;
import com.namnv.entity.ConfigurationEntry;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Một mẩu của snapshot. Leader gửi lần lượt từng mẩu của từng file; follower ghép lại rồi cài khi nhận mẩu cuối (done).
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class InstallSnapshotRequest implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private long term;                // leader term
    private String leaderId;          // leader nodeId
    private long lastIncludedIndex;   // snapshot lastIncludedIndex
    private long lastIncludedTerm;    // snapshot lastIncludedTerm
    private ConfigurationEntry conf;  // cluster config tại lastIncludedIndex
    private Map<String, ClientSession> sessions; // clientId -> các sequence đã apply tại lastIncludedIndex
    private List<String> files;       // tên mọi file của snapshot, theo thứ tự gửi
    private String fileName;          // file mà mẩu này thuộc về, null nếu snapshot không có file nào
    private long offset;              // vị trí của mẩu trong file
    private byte[] data;
    private boolean done;             // mẩu cuối cùng của file cuối cùng
}

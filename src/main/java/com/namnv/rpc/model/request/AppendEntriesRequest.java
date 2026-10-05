package com.namnv.rpc.model.request;


import com.namnv.entity.LogEntry;
import com.namnv.storage.LogStorage;
import com.namnv.storage.binary.EntryFrame;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;


public class AppendEntriesRequest implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public final long term;
    public final String leaderId;
    public final long prevLogIndex;
    public final long prevLogTerm;
    // null khi request mang các entry dưới dạng block; đọc qua entries()
    public final List<LogEntry> entries;
    public final long leaderCommit;
    // các entry dưới dạng khung mà log của leader đã dựng sẵn, thay cho entries
    public final transient LogStorage.Block block;
    private transient volatile List<LogEntry> decoded;


    public AppendEntriesRequest(long term, String leaderId, long prevLogIndex, long prevLogTerm, List<LogEntry> entries, long leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
        this.block = null;
    }

    public AppendEntriesRequest(long term, String leaderId, long prevLogIndex, long prevLogTerm, LogStorage.Block block, long leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = null;
        this.leaderCommit = leaderCommit;
        this.block = block;
    }

    public int entryCount() {
        return block != null ? block.count() : entries == null ? 0 : entries.size();
    }

    /**
     * Các entry của request. Với request mang block (chỉ gặp khi request đi thẳng trong bộ nhớ, không qua RpcCodec)
     * các khung được giải mã ở lần gọi đầu.
     */
    public List<LogEntry> entries() {
        if (block == null) {
            return entries != null ? entries : List.of();
        }
        var result = decoded;
        if (result == null) {
            try {
                result = EntryFrame.readAll(ByteBuffer.wrap(block.copy()), block.count());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            decoded = result;
        }
        return result;
    }
}

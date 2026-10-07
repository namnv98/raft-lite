package com.namnv.entity;


import lombok.Data;
import lombok.ToString;

import java.io.Serial;
import java.io.Serializable;

@Data
@ToString
public class LogEntry implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private long index;
    private long term;
    private byte[] command; // opaque command
    private boolean isConfigurationEntry; // true nếu là config log
    private ConfigurationEntry configuration; // dữ liệu config nếu isConfiguration=true
    // định danh của lệnh từ phía client, null nếu client không cần chống ghi trùng
    private String clientId;
    private long sequence;
    // true: entry này kết thúc phiên của clientId, bảng chống trùng quên client đó
    private boolean sessionClose;
    // true: command là một lô nhiều lệnh (CommandBatch), được apply và chống ghi trùng cùng nhau
    private boolean batch;
    // thời điểm (epoch ms) leader tạo entry, giống nhau trên mọi node; 0 nếu không có (entry cũ). Không giảm dọc theo log,
    // kể cả qua các lần đổi leader, nên state machine dùng được làm thời gian của các thay đổi một cách tất định
    private long timestamp;
    // Khung nhị phân của entry (xem EntryFrame) khi entry vừa được giải mã từ khung đó, như các entry follower nhận trong
    // AppendEntries: log ghi thẳng khung này thay vì mã hoá lại. Không thuộc về giá trị của entry.
    @lombok.EqualsAndHashCode.Exclude
    @ToString.Exclude
    private transient byte[] frame;

    public LogEntry() {
    }

    // log bình thường
    public LogEntry(long index, long term, byte[] command) {
        this.index = index;
        this.term = term;
        this.command = command;
        this.isConfigurationEntry = false;
        this.configuration = null;
    }

    // log có định danh client: (clientId, sequence) đã apply rồi thì không apply lần nữa
    public LogEntry(long index, long term, byte[] command, String clientId, long sequence) {
        this(index, term, command);
        this.clientId = clientId;
        this.sequence = sequence;
    }

    public static LogEntry newSessionClose(long index, long term, String clientId) {
        LogEntry e = new LogEntry(index, term, null);
        e.clientId = clientId;
        e.sessionClose = true;
        return e;
    }

    // log cấu hình
    public static LogEntry newConfigurationEntry(long index, long term, ConfigurationEntry config) {
        LogEntry e = new LogEntry();
        e.index = index;
        e.term = term;
        e.isConfigurationEntry = true;
        e.configuration = config;
        e.command = null;
        return e;
    }

    public boolean isConfigurationEntry() {
        return isConfigurationEntry;
    }

    @Override
    public String toString() {
        if (isConfigurationEntry) {
            return "LogEntry{index=" + index + ", term=" + term + ", config=" + configuration + "}";
        } else {
            String cmdStr = command != null ? new String(command) : "null";
            return "LogEntry{index=" + index + ", term=" + term + ", command=" + cmdStr + "}";
        }
    }
}

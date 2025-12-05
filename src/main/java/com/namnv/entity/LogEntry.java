package com.namnv.entity;


import com.namnv.storage.ConfigurationEntry;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class LogEntry {
    private long index;
    private long term;
    private byte[] command; // opaque command
    private boolean isConfigurationEntry; // true nếu là config log
    private ConfigurationEntry configuration; // dữ liệu config nếu isConfiguration=true

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

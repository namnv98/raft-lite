package com.namnv.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.namnv.config.NodeOptions;
import com.namnv.entity.LogEntry;
import lombok.Data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Data
public class FileLogStorage implements LogStorage {

    private long baseIndex;
    private long baseTerm;

    private final File file;
    private final List<LogEntry> entries = new ArrayList<>();
    private final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public FileLogStorage(NodeOptions nodeOptions, long baseIndex, long baseTerm) throws IOException {
        File folder = new File(nodeOptions.getLogUri());
        if (!folder.exists()) {
            folder.mkdirs();
        }
        this.file = new File(folder, "log.json");
        if (!file.exists()) {
            file.createNewFile();
        }
        this.baseIndex = baseIndex;
        this.baseTerm = baseTerm;
        loadFromFile();
    }

    private synchronized void loadFromFile() throws IOException {
        if (file.length() == 0) {
            return;
        }
        LogEntry[] arr = objectMapper.readValue(file, LogEntry[].class);
        entries.addAll(Arrays.asList(arr));
    }

    private synchronized void saveToFile() {
        try {
            objectMapper.writeValue(file, entries);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // ---------- CORE LOG STORE ----------
    @Override
    public synchronized long lastIndex() {
        return baseIndex + entries.size();
    }

    @Override
    public synchronized long lastTerm() {
        if (entries.isEmpty()) return baseTerm; // nếu empty thì term = baseTerm
        return entries.get(entries.size() - 1).getTerm();
    }

    @Override
    public synchronized LogEntry get(long index) {
        if (index <= baseIndex) return null;
        long pos = index - baseIndex - 1;
        if (pos < 0 || pos >= entries.size()) return null;
        return entries.get((int) pos);
    }

    @Override
    public synchronized void appendEntry(LogEntry entry) {
        entries.add(entry);
        saveToFile();
    }

    @Override
    public synchronized List<LogEntry> readFrom(long fromIndex) {
        if (fromIndex <= baseIndex) fromIndex = baseIndex + 1;
        long pos = fromIndex - baseIndex - 1;
        if (pos < 0 || pos >= entries.size()) return new ArrayList<>();
        return new ArrayList<>(entries.subList((int) pos, entries.size()));
    }

    @Override
    public synchronized void truncateSuffix(long lastIndexKept) {
        if (lastIndexKept <= baseIndex) {
            // truncate EVERYTHING after snapshot
            entries.clear();
        } else {
            long pos = lastIndexKept - baseIndex - 1;
            if (pos < 0 || pos >= entries.size()) return;
            entries.subList((int) pos, entries.size()).clear();
        }
        saveToFile();
    }

    @Override
    public synchronized void truncatePrefix(long firstIndexKept) {
        // index = lastIncludedIndex + 1
        if (firstIndexKept <= baseIndex) return;

        long pos = firstIndexKept - baseIndex - 1;
        if (pos <= 0) return;
        if (pos > entries.size()) pos = entries.size();

        // cập nhật baseIndex + baseTerm theo phần tử ngay trước pos
        LogEntry lastIncluded = entries.get((int) pos - 1);
        baseIndex = lastIncluded.getIndex();
        baseTerm = lastIncluded.getTerm();

        // xóa prefix
        entries.subList(0, (int) pos).clear();

        saveToFile();
    }
}
package com.namnv.storage.snapshot;

import lombok.Data;

import java.util.List;

@Data
public class SnapshotWriter {
    private final String path;
    private final List<String> file;

    public SnapshotWriter(String path, List<String> file) {
        this.path = path;
        this.file = file;
    }

    public boolean addFile(final String fileName) {
        return this.file.add(fileName);
    }
}

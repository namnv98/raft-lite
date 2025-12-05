package com.namnv.statemachine;

import com.namnv.core.Closure;
import com.namnv.entity.LogEntry;
import com.namnv.storage.snapshot.SnapshotReader;
import com.namnv.storage.snapshot.SnapshotWriter;
import com.namnv.core.Status;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.isNull;

public class KeyValueStateMachine implements StateMachine {

    // State của ứng dụng
    private final List<String> store = new ArrayList<>();

    @Override
    public synchronized void onApply(String node, LogEntry entry) {
        if (isNull(entry)) {
            return;
        }
        if (entry.getCommand() != null) {
            store.add(new String(entry.getCommand(), StandardCharsets.UTF_8));
        }
        System.out.println(node + " applied: " + entry);
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        try {
            File snapshotFile = new File(writer.getPath(), "snapshot.data");

            // Ghi toàn bộ state store ra file snapshot
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(snapshotFile))) {
                for (String item : store) {
                    bw.write(item);
                    bw.newLine();
                }
            }

            // đăng ký file snapshot vào writer
            writer.addFile("snapshot.data");

            System.out.println("Snapshot saved, entries=" + store.size());

            done.run(Status.OK());
        } catch (Exception e) {
            e.printStackTrace();
            done.run(Status.ERROR(e.getMessage()));
        }
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        try {
            File snapshotFile = new File(reader.getPath(), "snapshot.data");
            if (!snapshotFile.exists()) {
                return true;
            }

            // clear state cũ trước khi load
            store.clear();

            // đọc lại state
            List<String> lines = Files.readAllLines(snapshotFile.toPath(), StandardCharsets.UTF_8);
            store.addAll(lines);

            System.out.println("Snapshot loaded, entries=" + store.size());
            return true;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}

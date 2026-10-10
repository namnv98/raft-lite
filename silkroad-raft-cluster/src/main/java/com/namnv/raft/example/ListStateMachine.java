package com.namnv.raft.example;

import com.namnv.entity.LogEntry;
import com.namnv.raft.StateMachine;
import com.namnv.storage.FileUtil;
import com.namnv.storage.snapshot.SnapshotReader;
import com.namnv.storage.snapshot.SnapshotWriter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.isNull;

@Slf4j
public class ListStateMachine implements StateMachine {

    // State của ứng dụng: danh sách các command đã apply theo thứ tự
    private final List<String> store = new ArrayList<>();

    public synchronized List<String> getStore() {
        return new ArrayList<>(store);
    }

    @Override
    public synchronized void onApply(String node, LogEntry entry) {
        if (isNull(entry)) {
            return;
        }
        if (entry.getCommand() != null) {
            store.add(new String(entry.getCommand(), StandardCharsets.UTF_8));
        }
        log.info(node + " applied: " + entry);
    }

    @Override
    public CompletableFuture<Void> onSnapshotSave(SnapshotWriter writer) {
        var done = new CompletableFuture<Void>();
        // chỉ chụp bản sao khi node đang giữ lock, việc ghi file diễn ra sau ở thread khác
        List<String> copy = getStore();
        runSnapshotWrite(() -> {
            try {
                File snapshotFile = new File(writer.getPath(), "snapshot.data");

                StringBuilder content = new StringBuilder();
                for (String item : copy) {
                    content.append(item).append('\n');
                }
                FileUtil.atomicWrite(snapshotFile.toPath(), content.toString().getBytes(StandardCharsets.UTF_8));

                writer.addFile("snapshot.data");

                log.info("Snapshot saved, entries=" + copy.size());

                done.complete(null);
            } catch (Exception e) {
                log.error("Snapshot save failed", e);
                done.completeExceptionally(e);
            }
        });
        return done;
    }

    // ghi file ở thread riêng để không giữ lock của node; test chạy một thread có thể override để ghi tại chỗ
    protected void runSnapshotWrite(Runnable write) {
        Thread.ofVirtual().start(write);
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        try {
            File snapshotFile = new File(reader.getPath(), "snapshot.data");
            if (!snapshotFile.exists()) {
                return true;
            }

            // đọc file trước, chỉ giữ monitor khi thay state
            List<String> lines = Files.readAllLines(snapshotFile.toPath(), StandardCharsets.UTF_8);
            synchronized (this) {
                store.clear();
                store.addAll(lines);
            }

            log.info("Snapshot loaded, entries=" + lines.size());
            return true;

        } catch (Exception e) {
            log.error("Snapshot load failed", e);
            return false;
        }
    }
}

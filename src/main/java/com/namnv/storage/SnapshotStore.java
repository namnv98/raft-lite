package com.namnv.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.namnv.statemachine.snapshot.SnapshotMeta;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Mỗi snapshot là một thư mục snapshot_&lt;index&gt; chứa cả dữ liệu của state machine lẫn meta.
 * Snapshot được ghi vào thư mục temp rồi rename một lần, nên dữ liệu và meta luôn khớp nhau dù crash ở bất kỳ lúc nào.
 */
@Slf4j
public class SnapshotStore {
    private static final String PREFIX = "snapshot_";
    private static final String TEMP = "temp";
    private static final String META_FILE = "__raft_snapshot_meta.json";

    private final Path root;
    private final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private SnapshotMeta meta;

    public SnapshotStore(String uri) throws IOException {
        this.root = Path.of(uri);
        Files.createDirectories(root);
        // snapshot ghi dở trước khi crash
        deleteRecursively(root.resolve(TEMP));

        List<Path> snapshots = new ArrayList<>();
        try (var children = Files.list(root)) {
            children.filter(p -> Files.isDirectory(p) && indexOf(p) >= 0).forEach(snapshots::add);
        }
        snapshots.sort(Comparator.comparingLong(SnapshotStore::indexOf).reversed());
        for (Path snapshot : snapshots) {
            if (meta == null) {
                try {
                    meta = objectMapper.readValue(snapshot.resolve(META_FILE).toFile(), SnapshotMeta.class);
                    continue;
                } catch (IOException e) {
                    log.warn("Ignore unreadable snapshot {}", snapshot, e);
                }
            }
            // chỉ giữ snapshot mới nhất
            deleteRecursively(snapshot);
        }
    }

    private static long indexOf(Path snapshot) {
        String name = snapshot.getFileName().toString();
        if (!name.startsWith(PREFIX)) {
            return -1;
        }
        try {
            return Long.parseLong(name.substring(PREFIX.length()));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    // null nếu chưa có snapshot nào
    public synchronized SnapshotMeta getMeta() {
        return meta;
    }

    public synchronized String currentPath() {
        return root.resolve(PREFIX + meta.getLastIncludedIndex()).toString();
    }

    // thư mục trống để ghi snapshot mới
    public synchronized String prepareTemp() throws IOException {
        Path temp = root.resolve(TEMP);
        deleteRecursively(temp);
        Files.createDirectories(temp);
        return temp.toString();
    }

    public synchronized void commit(SnapshotMeta newMeta) throws IOException {
        Path temp = root.resolve(TEMP);
        FileUtil.atomicWrite(temp.resolve(META_FILE), objectMapper.writeValueAsBytes(newMeta));

        Path target = root.resolve(PREFIX + newMeta.getLastIncludedIndex());
        deleteRecursively(target);
        Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE);

        SnapshotMeta previous = meta;
        meta = newMeta;
        if (previous != null && previous.getLastIncludedIndex() != newMeta.getLastIncludedIndex()) {
            deleteRecursively(root.resolve(PREFIX + previous.getLastIncludedIndex()));
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (var walk = Files.walk(path)) {
            for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        }
    }
}

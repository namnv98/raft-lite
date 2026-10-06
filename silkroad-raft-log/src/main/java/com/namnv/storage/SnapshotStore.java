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
import java.util.Map;
import java.util.TreeMap;

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
    private final DiskFaultInjector faults;
    private final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private SnapshotMeta meta;

    public SnapshotStore(String uri) throws IOException {
        this(uri, DiskFaultInjector.NONE);
    }

    public SnapshotStore(String uri, DiskFaultInjector faults) throws IOException {
        this.faults = faults;
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
                // thư mục snapshot_<index> chỉ xuất hiện qua rename sau khi mọi file đã ghi xong, nên nếu nó không
                // còn nguyên vẹn thì đó là dữ liệu hỏng: báo lỗi thay vì âm thầm bỏ qua
                meta = verify(snapshot);
                continue;
            }
            // chỉ giữ snapshot mới nhất
            deleteRecursively(snapshot);
        }
    }

    // nội dung file meta: meta của snapshot kèm CRC32 của từng file dữ liệu
    private static class StoredMeta {
        public SnapshotMeta meta;
        public Map<String, Long> fileChecksums;
    }

    private SnapshotMeta verify(Path snapshot) throws IOException {
        Path metaFile = snapshot.resolve(META_FILE);
        if (!Files.exists(metaFile)) {
            throw new IOException("Snapshot " + snapshot + " is corrupted: meta file is missing");
        }
        StoredMeta stored = objectMapper.readValue(
                Checksum.unwrap(Files.readAllBytes(metaFile), metaFile.toString()), StoredMeta.class);
        for (var file : stored.fileChecksums.entrySet()) {
            Path path = snapshot.resolve(file.getKey());
            if (!Files.exists(path) || checksumOf(path) != file.getValue()) {
                throw new IOException("Snapshot file " + path + " is corrupted: checksum mismatch");
            }
        }
        return stored.meta;
    }

    private static long checksumOf(Path file) throws IOException {
        byte[] data = Files.readAllBytes(file);
        return Checksum.crc32(data, 0, data.length);
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
        faults.beforeWrite("snapshot.commit");
        Path temp = root.resolve(TEMP);
        StoredMeta stored = new StoredMeta();
        stored.meta = newMeta;
        stored.fileChecksums = new TreeMap<>();
        for (String file : newMeta.getFiles()) {
            stored.fileChecksums.put(file, checksumOf(temp.resolve(file)));
        }
        FileUtil.atomicWrite(temp.resolve(META_FILE), Checksum.wrap(objectMapper.writeValueAsBytes(stored)));

        Path target = root.resolve(PREFIX + newMeta.getLastIncludedIndex());
        deleteRecursively(target);
        Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE);
        FileUtil.syncDirectory(root);

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

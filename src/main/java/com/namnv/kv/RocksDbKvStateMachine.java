package com.namnv.kv;

import com.namnv.core.Closure;
import com.namnv.core.Status;
import com.namnv.statemachine.snapshot.SnapshotReader;
import com.namnv.statemachine.snapshot.SnapshotWriter;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Checkpoint;
import org.rocksdb.CompressionType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Kho KV trên RocksDB (LSM-tree): mỗi lô là một {@link WriteBatch}.
 * <ul>
 * <li>{@code sync = true}: bật WAL của RocksDB và fsync mỗi lô, để mỗi lô bền vững như một lần commit của LMDB.</li>
 * <li>{@code sync = false}: tắt hẳn WAL. Raft log đã là WAL, và kho được dựng lại từ snapshot cộng log, nên đây là cách
 * dùng hợp lý nhất trong Raft.</li>
 * </ul>
 * Snapshot là một checkpoint của RocksDB, tạo trên thread backend đúng lúc mọi lệnh tới thời điểm chụp đã vào kho và
 * chưa lệnh nào sau đó vào: các file SST (không bao giờ bị sửa) chỉ được hard link sang thư mục snapshot, không chép.
 */
@Slf4j
public class RocksDbKvStateMachine extends BufferedKvStateMachine {
    // các file của checkpoint nằm phẳng trong thư mục snapshot với tiền tố này
    private static final String PREFIX = "rocks-";

    static {
        RocksDB.loadLibrary();
    }

    private final Path dir;
    private final Options options;
    private final WriteOptions writeOptions;
    // lúc nạp snapshot, kho được đóng rồi mở lại: lần đọc không được chạm vào kho đã đóng
    private final ReentrantReadWriteLock dbLock = new ReentrantReadWriteLock();
    private RocksDB db;

    public RocksDbKvStateMachine(Path dir) {
        this(dir, false, 100, 10_000);
    }

    /** @param sync bật WAL và fsync mỗi lô; false thì tắt WAL */
    public RocksDbKvStateMachine(Path dir, boolean sync, long batchIntervalMs, int batchLimit) {
        super("rocksdb", batchIntervalMs, batchLimit);
        this.dir = dir;
        recreateDirectory(dir);
        options = new Options()
                .setCreateIfMissing(true)
                .setMaxBackgroundJobs(4)
                .setWriteBufferSize(64L << 20)
                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setFilterPolicy(new BloomFilter(10, false))
                        .setBlockCache(new LRUCache(256L << 20)));
        writeOptions = new WriteOptions().setSync(sync).setDisableWAL(!sync);
        db = open();
    }

    private RocksDB open() {
        try {
            return RocksDB.open(options, dir.toString());
        } catch (RocksDBException e) {
            throw new IllegalStateException("cannot open RocksDB at " + dir, e);
        }
    }

    @Override
    protected void writeBatch(Map<Key, byte[]> batch) {
        dbLock.readLock().lock();
        try (var writes = new WriteBatch()) {
            for (var entry : batch.entrySet()) {
                if (entry.getValue() == TOMBSTONE) {
                    writes.delete(entry.getKey().bytes());
                } else {
                    writes.put(entry.getKey().bytes(), entry.getValue());
                }
            }
            db.write(writeOptions, writes);
        } catch (RocksDBException e) {
            throw new IllegalStateException(e);
        } finally {
            dbLock.readLock().unlock();
        }
    }

    @Override
    protected byte[] storeGet(byte[] key) {
        dbLock.readLock().lock();
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new IllegalStateException(e);
        } finally {
            dbLock.readLock().unlock();
        }
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        whenWrittenUpToNow(() -> {
            Path snapshotDir = Path.of(writer.getPath());
            Path checkpointDir = snapshotDir.resolve("rocks-checkpoint");
            dbLock.readLock().lock();
            try (var checkpoint = Checkpoint.create(db)) {
                // memtable được flush thành SST rồi các SST được hard link, nên việc này không tỉ lệ với kích thước kho
                checkpoint.createCheckpoint(checkpointDir.toString());
                try (Stream<Path> files = Files.list(checkpointDir)) {
                    for (Path file : files.toList()) {
                        var name = PREFIX + file.getFileName();
                        Files.move(file, snapshotDir.resolve(name));
                        writer.addFile(name);
                    }
                }
                Files.delete(checkpointDir);
                log.info("RocksDB snapshot saved, files={}", writer.getFile().size());
                done.run(Status.OK());
            } catch (RocksDBException | IOException | RuntimeException e) {
                log.error("RocksDB snapshot save failed", e);
                done.run(Status.ERROR(e.getMessage()));
            } finally {
                dbLock.readLock().unlock();
            }
        });
    }

    @Override
    protected long loadSnapshot(SnapshotReader reader) throws IOException {
        dbLock.writeLock().lock();
        try {
            db.close();
            deleteRecursively(dir);
            Files.createDirectories(dir);
            List<Path> files;
            try (Stream<Path> list = Files.list(Path.of(reader.getPath()))) {
                files = list.filter(p -> p.getFileName().toString().startsWith(PREFIX)).toList();
            }
            for (Path file : files) {
                Path target = dir.resolve(file.getFileName().toString().substring(PREFIX.length()));
                if (target.toString().endsWith(".sst")) {
                    linkOrCopy(file, target);
                } else {
                    // MANIFEST, CURRENT, OPTIONS... được RocksDB viết tiếp hoặc thay, nên phải là bản riêng
                    Files.copy(file, target);
                }
            }
            db = open();
            return db.getLongProperty("rocksdb.estimate-num-keys");
        } catch (RocksDBException e) {
            throw new IOException(e);
        } finally {
            dbLock.writeLock().unlock();
        }
    }

    // SST không bao giờ bị sửa sau khi ghi xong, nên dùng chung inode với snapshot là an toàn
    private static void linkOrCopy(Path source, Path target) throws IOException {
        try {
            Files.createLink(target, source);
        } catch (IOException | UnsupportedOperationException e) {
            Files.copy(source, target);
        }
    }

    @Override
    protected void closeStore() {
        dbLock.writeLock().lock();
        try {
            db.close();
            writeOptions.close();
            options.close();
        } finally {
            dbLock.writeLock().unlock();
        }
    }
}

package com.namnv.ledger.state;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Checkpoint;
import org.rocksdb.CompressionType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Lịch sử giao dịch của sổ cái: giao dịch mới nằm trong một đoạn bộ nhớ dung lượng cố định; đoạn đầy được một thread nền
 * ghi xuống RocksDB (một WriteBatch, không WAL: Raft log đã giữ độ bền) rồi giải phóng. Bộ nhớ vì vậy không tăng theo số
 * giao dịch, trừ bloom filter của các id (khoảng 1,5 byte mỗi giao dịch).
 * <ul>
 * <li>Tra một id: đoạn đang ghi, rồi các đoạn chưa xuống đĩa, rồi bloom filter; chỉ khi filter nói "có thể có" (khoảng
 * 1% với id mới) mới đọc RocksDB. Kết quả luôn chính xác, nên mọi node trả lời giống nhau.</li>
 * <li>Snapshot là một checkpoint của RocksDB (hard link tới các SST, không chép) tạo đúng lúc mọi giao dịch trước thời
 * điểm chụp đã xuống RocksDB và chưa giao dịch nào sau đó, cùng bloom filter lúc đó.</li>
 * <li>Thread nền ghi không kịp thì thread của node chờ khi đã có {@link #MAX_UNWRITTEN_SEGMENTS} đoạn chưa ghi: bộ nhớ
 * có giới hạn, đổi lại throughput không vượt quá tốc độ ghi của RocksDB.</li>
 * </ul>
 * Mọi phương thức trừ khi ghi chú khác đều gọi trên thread của node (trong lock của node).
 */
@Slf4j
final class TransferStore implements AutoCloseable {
    static final int MAX_UNWRITTEN_SEGMENTS = 4;
    private static final String PREFIX = "rocks-";
    private static final String FILTER_FILE = "transfer-ids.filter";
    private static final int VALUE_BYTES = 8 + 8 + 8 + 4;

    static {
        RocksDB.loadLibrary();
    }

    private final Path dir;
    private final int segmentCapacity;
    private final Options options;
    private final WriteOptions writeOptions;
    // lúc nạp snapshot, RocksDB được đóng rồi mở lại: lần đọc không được chạm vào bản đã đóng
    private final ReentrantReadWriteLock dbLock = new ReentrantReadWriteLock();
    private RocksDB db;
    private final ExecutorService backend;
    private final IdFilter filter;

    // các đoạn đã khoá chờ ghi (cũ nhất ở đầu) và một đoạn trống để dùng lại; thread nền cũng chạm vào, dưới lock này
    private final Object lock = new Object();
    private final ArrayDeque<TransferSegment> unwritten = new ArrayDeque<>();
    private TransferSegment spare;

    private TransferSegment active;
    private long count;

    // chỉ thread nền dùng
    private final byte[] writeKey = new byte[8];
    private final byte[] writeValue = new byte[VALUE_BYTES];

    TransferStore(Path dir, int segmentCapacity, long expectedTransfers) {
        this.dir = dir;
        this.segmentCapacity = segmentCapacity;
        recreate(dir);
        options = new Options()
                .setCreateIfMissing(true)
                .setMaxBackgroundJobs(4)
                .setWriteBufferSize(64L << 20)
                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setFilterPolicy(new BloomFilter(10, false))
                        .setBlockCache(new LRUCache(128L << 20)));
        writeOptions = new WriteOptions().setDisableWAL(true);
        db = open();
        filter = new IdFilter(expectedTransfers);
        active = new TransferSegment(segmentCapacity);
        backend = Executors.newSingleThreadExecutor(r -> {
            var thread = new Thread(r, "ledger-transfers");
            thread.setDaemon(true);
            return thread;
        });
    }

    long count() {
        return count;
    }

    void setCount(long count) {
        this.count = count;
    }

    /** true nếu có giao dịch id; khi đó out = {tài khoản nợ, tài khoản có, số tiền, ledger} */
    boolean find(long id, long[] out) {
        if (copy(active, active.find(id), out)) {
            return true;
        }
        synchronized (lock) {
            for (var segments = unwritten.descendingIterator(); segments.hasNext(); ) {
                var segment = segments.next();
                if (copy(segment, segment.find(id), out)) {
                    return true;
                }
            }
        }
        if (!filter.mightContain(id)) {
            return false;
        }
        byte[] value;
        dbLock.readLock().lock();
        try {
            value = db.get(key(id));
        } catch (RocksDBException e) {
            throw new IllegalStateException(e);
        } finally {
            dbLock.readLock().unlock();
        }
        if (value == null) {
            return false;
        }
        var in = ByteBuffer.wrap(value);
        out[0] = in.getLong();
        out[1] = in.getLong();
        out[2] = in.getLong();
        out[3] = in.getInt();
        return true;
    }

    private static boolean copy(TransferSegment segment, int slot, long[] out) {
        if (slot < 0) {
            return false;
        }
        out[0] = segment.debits[slot];
        out[1] = segment.credits[slot];
        out[2] = segment.amounts[slot];
        out[3] = segment.ledgers[slot];
        return true;
    }

    /**
     * Chuẩn bị chỗ cho một giao dịch nữa: khoá đoạn đầy (có thể chờ thread nền) và cấp phát nếu cần. Gọi trước khi đổi
     * bất cứ state nào, để hết bộ nhớ hay bị ngắt thì giao dịch không để lại gì dở dang.
     */
    void reserve() {
        if (active.full()) {
            freeze(null);
        }
        filter.reserve();
    }

    /** ghi nhận giao dịch; {@link #reserve()} phải được gọi ngay trước đó */
    void add(long id, long debit, long credit, long amount, int ledger) {
        active.add(id, debit, credit, amount, ledger);
        filter.add(id);
        count++;
    }

    // khoá đoạn đang ghi và giao nó cho thread nền; afterWrite chạy trên thread nền khi nó và mọi đoạn trước đã xuống RocksDB
    private void freeze(Runnable afterWrite) {
        var segment = active;
        TransferSegment next;
        synchronized (lock) {
            boolean interrupted = false;
            while (unwritten.size() >= MAX_UNWRITTEN_SEGMENTS) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            unwritten.addLast(segment);
            next = spare;
            spare = null;
        }
        active = next != null ? next : new TransferSegment(segmentCapacity);
        backend.execute(() -> write(segment, afterWrite));
    }

    // thread nền
    private void write(TransferSegment segment, Runnable afterWrite) {
        try {
            if (segment.count > 0) {
                var key = ByteBuffer.wrap(writeKey);
                var value = ByteBuffer.wrap(writeValue);
                dbLock.readLock().lock();
                try (var batch = new WriteBatch()) {
                    for (int i = 0; i < segment.count; i++) {
                        key.putLong(0, segment.ids[i]);
                        value.putLong(0, segment.debits[i]).putLong(8, segment.credits[i])
                                .putLong(16, segment.amounts[i]).putInt(24, segment.ledgers[i]);
                        batch.put(writeKey, writeValue);
                    }
                    db.write(writeOptions, batch);
                } finally {
                    dbLock.readLock().unlock();
                }
            }
            // chỉ bỏ đoạn khỏi bộ nhớ sau khi nó đã nằm trong RocksDB: lần tra không thấy nó ở đây thì thấy nó ở đó
            synchronized (lock) {
                unwritten.remove(segment);
                segment.clear();
                if (spare == null) {
                    spare = segment;
                }
                lock.notifyAll();
            }
            if (afterWrite != null) {
                afterWrite.run();
            }
        } catch (RocksDBException | RuntimeException e) {
            // không ghi được lịch sử giao dịch: không thể tiếp tục an toàn
            log.error("Failed to write {} transfers to RocksDB", segment.count, e);
            throw new IllegalStateException(e);
        }
    }

    // ---------- snapshot ----------

    /** phần còn lại của snapshot (tài khoản, tổng), ghi vào thư mục snapshot trên thread nền */
    interface SnapshotPart {
        List<String> write(Path snapshotDir) throws IOException;
    }

    /**
     * Gọi trong lock của node, đúng thời điểm chụp. {@code rest} ghi phần state đã chụp sẵn của sổ cái; {@code done}
     * nhận danh sách mọi file của snapshot, hoặc lỗi.
     */
    void snapshot(Path snapshotDir, SnapshotPart rest, java.util.function.BiConsumer<List<String>, Exception> done) {
        var filterImage = filter.image();
        freeze(() -> {
            // Trên thread nền, đúng lúc mọi giao dịch trước thời điểm chụp đã xuống RocksDB: chỉ tạo checkpoint (flush
            // memtable rồi hard link các SST). Phần còn lại (filter, tài khoản) đã được chụp sẵn nên ghi ở thread khác,
            // để thread nền tiếp tục ghi giao dịch mới thay vì bắt thread của node chờ.
            var files = new ArrayList<String>();
            Path checkpointDir = snapshotDir.resolve("rocks-checkpoint");
            dbLock.readLock().lock();
            try (var checkpoint = Checkpoint.create(db)) {
                checkpoint.createCheckpoint(checkpointDir.toString());
            } catch (RocksDBException | RuntimeException e) {
                done.accept(null, e instanceof Exception ex ? ex : new IllegalStateException(e));
                return;
            } finally {
                dbLock.readLock().unlock();
            }
            Thread.ofVirtual().name("ledger-snapshot").start(() -> {
                try {
                    try (Stream<Path> list = Files.list(checkpointDir)) {
                        for (Path file : list.toList()) {
                            var name = PREFIX + file.getFileName();
                            Files.move(file, snapshotDir.resolve(name));
                            files.add(name);
                        }
                    }
                    Files.delete(checkpointDir);
                    try (var stream = new FileOutputStream(snapshotDir.resolve(FILTER_FILE).toFile());
                         var out = new DataOutputStream(new BufferedOutputStream(stream, 1 << 20))) {
                        filterImage.writeTo(out);
                        out.flush();
                        stream.getFD().sync();
                    }
                    files.add(FILTER_FILE);
                    files.addAll(rest.write(snapshotDir));
                    done.accept(files, null);
                } catch (IOException | RuntimeException e) {
                    done.accept(null, e);
                }
            });
        });
    }

    /** thay toàn bộ lịch sử bằng snapshot trong {@code snapshotDir}; không có lệnh nào được apply trong lúc này */
    void load(Path snapshotDir) throws IOException {
        try {
            // trên thread nền: mọi lần ghi trước đó đã xong
            backend.submit(() -> {
                loadOnBackend(snapshotDir);
                return null;
            }).get();
        } catch (ExecutionException e) {
            throw e.getCause() instanceof IOException io ? io : new IOException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private void loadOnBackend(Path snapshotDir) throws IOException {
        synchronized (lock) {
            unwritten.clear();
        }
        active.clear();
        dbLock.writeLock().lock();
        try {
            db.close();
            recreate(dir);
            try (Stream<Path> list = Files.list(snapshotDir)) {
                for (Path file : list.filter(p -> p.getFileName().toString().startsWith(PREFIX)).toList()) {
                    Path target = dir.resolve(file.getFileName().toString().substring(PREFIX.length()));
                    if (target.toString().endsWith(".sst")) {
                        // SST không bao giờ bị sửa sau khi ghi xong: dùng chung với snapshot là an toàn
                        try {
                            Files.createLink(target, file);
                        } catch (IOException | UnsupportedOperationException e) {
                            Files.copy(file, target);
                        }
                    } else {
                        Files.copy(file, target);
                    }
                }
            }
            db = open();
        } finally {
            dbLock.writeLock().unlock();
        }
        Path filterFile = snapshotDir.resolve(FILTER_FILE);
        boolean restored = false;
        if (Files.exists(filterFile)) {
            try (var in = new DataInputStream(new BufferedInputStream(new FileInputStream(filterFile.toFile()), 1 << 20))) {
                restored = filter.readFrom(in);
            }
        }
        if (!restored) {
            // filter cũ khác kích thước (cấu hình đổi) hoặc không có: dựng lại từ các id trong RocksDB
            filter.clear();
            dbLock.readLock().lock();
            try (RocksIterator it = db.newIterator()) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    filter.add(ByteBuffer.wrap(it.key()).getLong());
                }
            } finally {
                dbLock.readLock().unlock();
            }
        }
    }

    // ---------- vòng đời ----------

    private RocksDB open() {
        try {
            return RocksDB.open(options, dir.toString());
        } catch (RocksDBException e) {
            throw new IllegalStateException("cannot open RocksDB at " + dir, e);
        }
    }

    private static byte[] key(long id) {
        return ByteBuffer.allocate(8).putLong(id).array();
    }

    private static void recreate(Path dir) {
        try {
            if (Files.exists(dir)) {
                try (Stream<Path> walk = Files.walk(dir)) {
                    for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                        Files.delete(path);
                    }
                }
            }
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** số đoạn đang nằm trong bộ nhớ chờ ghi; cho test */
    int unwrittenSegments() {
        synchronized (lock) {
            return unwritten.size();
        }
    }

    /** chờ mọi đoạn đã khoá xuống RocksDB; cho test */
    void awaitWritten() throws Exception {
        backend.submit(() -> null).get(30, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        backend.shutdown();
        try {
            backend.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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

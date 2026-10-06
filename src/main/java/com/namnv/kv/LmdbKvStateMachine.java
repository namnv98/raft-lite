package com.namnv.kv;

import com.namnv.core.Closure;
import com.namnv.core.Status;
import com.namnv.statemachine.snapshot.SnapshotReader;
import com.namnv.statemachine.snapshot.SnapshotWriter;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.Txn;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.lmdbjava.ByteArrayProxy.PROXY_BA;

/**
 * Kho KV trên LMDB: B+tree copy-on-write trên mmap, một người ghi, nhiều người đọc không khoá. Cùng thiết kế với bbolt
 * của etcd. Mỗi lô là một transaction ghi; với {@code sync} mỗi lần commit fsync, như bbolt trong etcd.
 * <p>
 * Snapshot chép toàn bộ kho ra một file: một transaction đọc mở đúng lúc chụp (LMDB giữ nguyên trang của nó tới khi
 * đóng) cộng các lô chưa ghi lúc đó, được ghi ra file ở thread riêng.
 */
@Slf4j
public class LmdbKvStateMachine extends BufferedKvStateMachine {
    private static final String SNAPSHOT_FILE = "kv.data";
    private static final int LOAD_BATCH = 50_000;

    private final Env<byte[]> env;
    private final Dbi<byte[]> db;

    /** etcd mặc định: lô 100 ms hoặc 10.000 lệnh, có fsync */
    public LmdbKvStateMachine(Path dir) {
        this(dir, true, 100, 10_000, 16L << 30);
    }

    /**
     * @param sync    fsync mỗi lần commit một lô
     * @param mapSize kích thước tối đa của file LMDB (file thưa, chỉ chiếm phần đã dùng)
     */
    public LmdbKvStateMachine(Path dir, boolean sync, long batchIntervalMs, int batchLimit, long mapSize) {
        super("lmdb", batchIntervalMs, batchLimit);
        recreateDirectory(dir);
        // NOTLS: transaction đọc của snapshot được mở trên thread của node rồi dùng ở thread ghi file
        var flags = sync ? new EnvFlags[]{EnvFlags.MDB_NOTLS} : new EnvFlags[]{EnvFlags.MDB_NOTLS, EnvFlags.MDB_NOSYNC};
        env = Env.create(PROXY_BA).setMapSize(mapSize).setMaxDbs(1).open(dir.toFile(), flags);
        db = env.openDbi("kv", DbiFlags.MDB_CREATE);
    }

    @Override
    protected void writeBatch(Map<Key, byte[]> batch) {
        try (Txn<byte[]> txn = env.txnWrite()) {
            for (var entry : batch.entrySet()) {
                if (entry.getValue() == TOMBSTONE) {
                    db.delete(txn, entry.getKey().bytes());
                } else {
                    db.put(txn, entry.getKey().bytes(), entry.getValue());
                }
            }
            txn.commit();
        }
    }

    @Override
    protected byte[] storeGet(byte[] key) {
        try (Txn<byte[]> txn = env.txnRead()) {
            return db.get(txn, key);
        }
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        // Chụp danh sách lô trước, mở transaction đọc sau: lô nào được commit giữa hai bước thì có ở cả hai nơi,
        // và ghi đè lên chính nó bằng cùng giá trị thì không sao.
        var unwritten = captureUnwritten();
        Txn<byte[]> txn = env.txnRead();
        Thread.ofVirtual().name("lmdb-snapshot").start(() -> {
            try {
                var newest = new HashMap<Key, byte[]>();
                unwritten.forEach(newest::putAll);
                File file = new File(writer.getPath(), SNAPSHOT_FILE);
                long count = 0;
                try (var out = new FileOutputStream(file);
                     var data = new DataOutputStream(new BufferedOutputStream(out, 1 << 20));
                     CursorIterable<byte[]> cursor = db.iterate(txn)) {
                    for (var kv : cursor) {
                        if (!newest.containsKey(new Key(kv.key()))) {
                            write(data, kv.key(), kv.val());
                            count++;
                        }
                    }
                    for (var entry : newest.entrySet()) {
                        if (entry.getValue() != TOMBSTONE) {
                            write(data, entry.getKey().bytes(), entry.getValue());
                            count++;
                        }
                    }
                    data.writeInt(-1);
                    data.flush();
                    out.getFD().sync();
                } finally {
                    txn.close();
                }
                writer.addFile(SNAPSHOT_FILE);
                log.info("LMDB snapshot saved, keys={}", count);
                done.run(Status.OK());
            } catch (Exception e) {
                log.error("LMDB snapshot save failed", e);
                done.run(Status.ERROR(e.getMessage()));
            }
        });
    }

    private static void write(DataOutputStream out, byte[] key, byte[] value) throws IOException {
        out.writeInt(key.length);
        out.write(key);
        out.writeInt(value.length);
        out.write(value);
    }

    @Override
    protected long loadSnapshot(SnapshotReader reader) throws IOException {
        try (Txn<byte[]> txn = env.txnWrite()) {
            db.drop(txn);
            txn.commit();
        }
        var file = new File(reader.getPath(), SNAPSHOT_FILE);
        if (!file.exists()) {
            return 0;
        }
        long count = 0;
        try (var in = new DataInputStream(new BufferedInputStream(new FileInputStream(file), 1 << 20))) {
            boolean done = false;
            while (!done) {
                try (Txn<byte[]> txn = env.txnWrite()) {
                    for (int i = 0; i < LOAD_BATCH; i++) {
                        int keyLength = in.readInt();
                        if (keyLength < 0) {
                            done = true;
                            break;
                        }
                        var key = in.readNBytes(keyLength);
                        var value = in.readNBytes(in.readInt());
                        if (key.length != keyLength) {
                            throw new EOFException("truncated KV snapshot");
                        }
                        db.put(txn, key, value);
                        count++;
                    }
                    txn.commit();
                }
            }
        }
        return count;
    }

    @Override
    protected void closeStore() {
        env.close();
    }
}

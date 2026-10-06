package com.namnv.kv;

import com.namnv.entity.LogEntry;
import com.namnv.statemachine.StateMachine;
import com.namnv.statemachine.snapshot.SnapshotReader;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Phần chung của các kho KV, theo cách backend của etcd:
 * <ul>
 * <li>{@link #onApply} chỉ ghi vào bộ đệm trong bộ nhớ, nên thread của node không bao giờ chờ kho.</li>
 * <li>Một thread backend ghi bộ đệm vào kho theo lô, mỗi {@code batchIntervalMs} hoặc khi bộ đệm đủ {@code batchLimit}
 * lệnh (etcd: 100 ms, 10.000 lệnh). Mọi lần ghi vào kho đều chạy trên thread này.</li>
 * <li>{@link #get} xem bộ đệm chưa ghi trước, rồi tới kho, nên luôn thấy mọi lệnh đã apply.</li>
 * </ul>
 * Độ bền của dữ liệu nằm ở Raft log, không ở kho: như mọi state machine của Raft Lite, kho bắt đầu rỗng mỗi lần khởi
 * động rồi được dựng lại từ snapshot và phần log sau nó.
 */
@Slf4j
public abstract class BufferedKvStateMachine implements StateMachine, AutoCloseable {
    // đánh dấu key đã bị xoá trong bộ đệm; so sánh bằng ==, nên value rỗng thật vẫn phân biệt được
    protected static final byte[] TOMBSTONE = new byte[0];

    protected record Key(byte[] bytes) {
        @Override
        public boolean equals(Object other) {
            return other instanceof Key key && Arrays.equals(bytes, key.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    // một lô chờ ghi; afterWrite (có thể null) chạy trên thread backend ngay sau khi lô và mọi lô trước nó đã vào kho
    private record Batch(Map<Key, byte[]> writes, Runnable afterWrite) {
    }

    private final int batchLimit;
    private final ScheduledExecutorService backend;
    private final Object lock = new Object();
    // lệnh đã apply mà chưa vào kho: pending đang nhận thêm, frozen là các lô đã khoá chờ ghi (cũ nhất ở đầu)
    private Map<Key, byte[]> pending = new HashMap<>();
    private final ArrayDeque<Batch> frozen = new ArrayDeque<>();
    private boolean flushRequested;

    protected BufferedKvStateMachine(String name, long batchIntervalMs, int batchLimit) {
        this.batchLimit = batchLimit;
        backend = Executors.newSingleThreadScheduledExecutor(r -> {
            var thread = new Thread(r, name + "-backend");
            thread.setDaemon(true);
            return thread;
        });
        backend.scheduleWithFixedDelay(this::flush, batchIntervalMs, batchIntervalMs, TimeUnit.MILLISECONDS);
    }

    // ---------- phần riêng của từng kho ----------

    /** ghi một lô (value == TOMBSTONE là xoá) và commit; chạy trên thread backend */
    protected abstract void writeBatch(Map<Key, byte[]> batch);

    /** đọc thẳng từ kho, không qua bộ đệm; null nếu không có key */
    protected abstract byte[] storeGet(byte[] key);

    /** thay toàn bộ kho bằng snapshot; chạy trên thread backend, bộ đệm đã được xoá; trả về số key đã nạp */
    protected abstract long loadSnapshot(SnapshotReader reader) throws Exception;

    protected abstract void closeStore();

    // ---------- ghi ----------

    @Override
    public void onApply(String node, LogEntry entry) {
        var command = entry.getCommand();
        if (command == null) {
            return;
        }
        Key key;
        byte[] value;
        try {
            switch (KvCommands.op(command)) {
                case KvCommands.PUT -> {
                    int keyLength = KvCommands.keyLength(command);
                    key = new Key(KvCommands.key(command));
                    value = KvCommands.value(command, keyLength);
                }
                case KvCommands.DELETE -> {
                    key = new Key(KvCommands.key(command));
                    value = TOMBSTONE;
                }
                default -> {
                    return; // không phải lệnh KV: mọi node bỏ qua giống nhau
                }
            }
        } catch (IllegalArgumentException malformed) {
            return;
        }
        synchronized (lock) {
            pending.put(key, value);
            if (pending.size() >= batchLimit && !flushRequested) {
                flushRequested = true;
                backend.execute(this::flush);
            }
        }
    }

    // thread backend: khoá bộ đệm hiện tại thành một lô rồi ghi mọi lô đang chờ, theo đúng thứ tự
    private void flush() {
        try {
            Batch batch;
            synchronized (lock) {
                flushRequested = false;
                freezePending(null);
                batch = frozen.peekFirst();
            }
            while (batch != null) {
                if (!batch.writes().isEmpty()) {
                    writeBatch(batch.writes());
                }
                // chỉ bỏ lô khỏi bộ đệm sau khi nó đã nằm trong kho: lần đọc không thấy nó ở đây thì thấy nó ở đó
                synchronized (lock) {
                    frozen.pollFirst();
                }
                if (batch.afterWrite() != null) {
                    batch.afterWrite().run();
                }
                synchronized (lock) {
                    batch = frozen.peekFirst();
                }
            }
        } catch (RuntimeException e) {
            // lô vẫn nằm trong bộ đệm và được thử lại ở nhịp sau
            log.error("KV backend failed to write a batch", e);
        }
    }

    // gọi khi giữ lock
    private void freezePending(Runnable afterWrite) {
        if (!pending.isEmpty() || afterWrite != null) {
            frozen.addLast(new Batch(pending, afterWrite));
            pending = new HashMap<>();
        }
    }

    /**
     * Gọi trong lock của node, lúc chụp snapshot: khoá mọi lệnh đã apply thành lô, và trả về các lô chưa vào kho
     * (cũ nhất trước). State lúc này = kho ⊕ các lô đó.
     */
    protected List<Map<Key, byte[]>> captureUnwritten() {
        synchronized (lock) {
            freezePending(null);
            var batches = new ArrayList<Map<Key, byte[]>>();
            for (Batch batch : frozen) {
                batches.add(batch.writes());
            }
            return batches;
        }
    }

    /**
     * Gọi trong lock của node, lúc chụp snapshot: {@code action} chạy trên thread backend ngay khi mọi lệnh đã apply
     * tới lúc này đã vào kho, và trước bất kỳ lệnh nào apply sau lúc này. Lúc đó kho chứa đúng state của snapshot.
     */
    protected void whenWrittenUpToNow(Runnable action) {
        synchronized (lock) {
            freezePending(action);
            if (!flushRequested) {
                flushRequested = true;
                backend.execute(this::flush);
            }
        }
    }

    // ---------- đọc ----------

    /** value của key, hoặc null; thấy mọi lệnh đã apply */
    public byte[] get(byte[] key) {
        var wrapped = new Key(key);
        synchronized (lock) {
            var value = pending.get(wrapped);
            if (value == null) {
                for (var batch = frozen.descendingIterator(); batch.hasNext() && value == null; ) {
                    value = batch.next().writes().get(wrapped);
                }
            }
            if (value != null) {
                return value == TOMBSTONE ? null : value;
            }
        }
        // đọc kho sau khi xem bộ đệm: lô nào đã rời bộ đệm thì đã vào kho trước thời điểm này
        return storeGet(key);
    }

    /** Trả lời một truy vấn đọc ({@link KvCommands#get}); dùng làm hàm truy vấn của {@code RaftClientService}. */
    public byte[] query(byte[] request) {
        if (KvCommands.op(request) != KvCommands.GET) {
            throw new IllegalArgumentException("not a KV query");
        }
        return KvCommands.found(get(KvCommands.key(request)));
    }

    // ---------- snapshot ----------

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        try {
            // trên thread backend, như mọi lần ghi vào kho
            long count = onBackend(() -> {
                synchronized (lock) {
                    pending.clear();
                    frozen.clear();
                }
                return loadSnapshot(reader);
            });
            log.info("KV snapshot loaded, keys={}", count);
            return true;
        } catch (Exception e) {
            log.error("KV snapshot load failed", e);
            return false;
        }
    }

    // ---------- vòng đời ----------

    /** ghi nốt bộ đệm vào kho (cho test và lúc tắt) */
    public void flushNow() {
        try {
            onBackend(() -> {
                flush();
                return null;
            });
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private <T> T onBackend(Callable<T> task) throws Exception {
        try {
            return backend.submit(task).get();
        } catch (ExecutionException e) {
            throw e.getCause() instanceof Exception cause ? cause : e;
        }
    }

    @Override
    public void close() {
        backend.shutdown();
        try {
            backend.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        closeStore();
    }

    protected static void recreateDirectory(Path dir) {
        try {
            deleteRecursively(dir);
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }
}

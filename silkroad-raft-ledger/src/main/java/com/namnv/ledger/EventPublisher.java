package com.namnv.ledger;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Chuyển sự kiện của sổ cái từ thread của node sang {@link EventSink} ở một thread riêng, theo lô. Hàng đợi có giới hạn:
 * sink chậm thì thread của node chờ, thay vì để hàng đợi phình ra không giới hạn.
 * <p>
 * Thường chạy trên một learner (node không bỏ phiếu): việc đẩy dữ liệu ra ngoài không làm chậm quorum ghi.
 */
@Slf4j
public final class EventPublisher implements AutoCloseable {
    private static final int MAX_BATCH = 10_000;

    // phần tử là LedgerEvent, hoặc Runnable để chạy sau khi mọi sự kiện đứng trước nó đã được ghi
    private final BlockingQueue<Object> queue;
    private final EventSink sink;
    private final Thread thread;
    private volatile boolean running = true;
    private volatile long published;

    public EventPublisher(EventSink sink, int capacity) {
        this.sink = sink;
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.thread = Thread.ofPlatform().daemon().name("ledger-events").start(this::run);
    }

    /** gọi trên thread của node, ngay sau khi lệnh được apply */
    void publish(LedgerEvent event) {
        enqueue(event);
    }

    /** {@code then} chạy (trên thread của publisher) khi mọi sự kiện đã publish trước lời gọi này đã nằm trong sink */
    void afterPublished(Runnable then) {
        enqueue(then);
    }

    private void enqueue(Object item) {
        boolean interrupted = false;
        while (true) {
            try {
                queue.put(item);
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** vị trí cuối cùng mà sink đã có lúc sổ cái được dựng lại; sự kiện từ đó trở về trước bị bỏ qua */
    long sinkIndex() {
        return sink.lastIndex();
    }

    /** số sự kiện đã ghi vào sink từ lúc khởi động */
    public long published() {
        return published;
    }

    private void run() {
        var batch = new ArrayList<Object>();
        var events = new ArrayList<LedgerEvent>();
        while (running || !queue.isEmpty()) {
            try {
                Object first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                queue.drainTo(batch, MAX_BATCH - 1);
                for (Object item : batch) {
                    if (item instanceof LedgerEvent event) {
                        events.add(event);
                    } else {
                        // một mốc: ghi mọi sự kiện đứng trước nó rồi mới chạy
                        flush(events);
                        ((Runnable) item).run();
                    }
                }
                flush(events);
                batch.clear();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (IOException | RuntimeException e) {
                // không ghi được: dừng hẳn, để không có sự kiện nào bị bỏ qua mà không ai biết; node sẽ bị chặn khi hàng
                // đợi đầy, và khởi động lại sẽ phát lại từ vị trí cuối cùng sink đã có
                log.error("Ledger event sink failed; events are no longer published", e);
                return;
            }
        }
    }

    private void flush(List<LedgerEvent> events) throws IOException {
        if (!events.isEmpty()) {
            sink.write(events);
            published += events.size();
            events.clear();
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        try {
            thread.join(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        sink.close();
    }
}

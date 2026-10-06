package com.namnv.agent;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Một thread chạy vòng làm việc (duty cycle) theo kiểu agent của Aeron ({@code AgentRunner} + {@code IdleStrategy}).
 * Mỗi vòng:
 * <ol>
 * <li>hỏi selector xem socket nào có dữ liệu ({@code selectNow}, không chờ) và xử lý ngay trên thread này;</li>
 * <li>chạy các việc được gửi vào bằng {@link #execute};</li>
 * <li>ghi ra socket mọi thứ đã được xếp trong vòng này, mỗi kết nối một lần ghi.</li>
 * </ol>
 * Không có thread đọc hay thread ghi riêng cho từng kết nối, nên trên đường đi của một request không có lần đánh thức
 * thread nào: đó là phần tốn nhất khi tải thấp.
 * <p>
 * Khi một vòng không có việc gì, idle strategy quyết định làm gì tiếp:
 * <ul>
 * <li>{@code backoff} (mặc định): quay chờ {@code spinMicros} rồi chặn trong {@code select()}. Khác BackoffIdleStrategy của
 * Aeron (ngủ mù tới 1 ms), thread đang chặn được đánh thức ngay khi có dữ liệu mạng hoặc có việc mới.</li>
 * <li>{@code busy}: không bao giờ ngủ, như BusySpinIdleStrategy của Aeron. Độ trễ thấp nhất, đổi lại chiếm trọn một CPU
 * kể cả khi rảnh.</li>
 * </ul>
 * Mọi thao tác trên selector và các kết nối chỉ được làm trên thread của vòng; thread khác dùng {@link #execute}.
 */
@Slf4j
public final class AgentLoop implements Executor, AutoCloseable {

    /** Thứ được đăng ký vào selector; được gọi trên thread của vòng khi kênh của nó sẵn sàng. */
    public interface Handler {
        void onSelected(SelectionKey key);
    }

    /** Thứ có dữ liệu chờ ghi; được flush một lần ở cuối vòng. */
    public interface Flushable {
        void flush();
    }

    public enum IdleStrategy {
        BACKOFF, BUSY
    }

    private static final long BLOCKING_SELECT_MS = 100;

    private final Selector selector;
    private final ConcurrentLinkedQueue<Runnable> commands = new ConcurrentLinkedQueue<>();
    private final List<Flushable> dirty = new ArrayList<>();
    private final List<Flushable> flushing = new ArrayList<>();
    private final IdleStrategy idleStrategy;
    private final long spinNanos;
    private final Thread thread;
    // thread của vòng đang (hoặc sắp) chặn trong select(): người gửi việc phải đánh thức nó
    private volatile boolean sleeping;
    private volatile boolean running = true;
    private int selectedCount;

    public AgentLoop(String name) {
        this(name, IdleStrategy.valueOf(System.getProperty("raft.agent.idle", "backoff").toUpperCase()),
                Long.getLong("raft.agent.spinMicros", 50));
    }

    public AgentLoop(String name, IdleStrategy idleStrategy, long spinMicros) {
        try {
            selector = Selector.open();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.idleStrategy = idleStrategy;
        this.spinNanos = TimeUnit.MICROSECONDS.toNanos(spinMicros);
        thread = new Thread(this::run, name);
        // vòng của một node không bao giờ được tắt (ví dụ trong test) không được giữ JVM sống
        thread.setDaemon(true);
        thread.start();
    }

    public boolean inLoop() {
        return Thread.currentThread() == thread;
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * Chạy {@code task} trên thread của vòng, lần lượt theo thứ tự gửi vào.
     *
     * @throws RejectedExecutionException nếu vòng đã dừng
     */
    @Override
    public void execute(Runnable task) {
        if (!running) {
            throw new RejectedExecutionException("agent loop is closed");
        }
        commands.add(task);
        // dừng đúng lúc đang thêm: thread có thể đã thoát mà không thấy việc này, khi đó trả lại cho người gọi
        if (!running && commands.remove(task)) {
            throw new RejectedExecutionException("agent loop is closed");
        }
        if (sleeping) {
            selector.wakeup();
        }
    }

    /** Đăng ký một kênh với selector. Chỉ gọi trên thread của vòng. */
    public SelectionKey register(java.nio.channels.SelectableChannel channel, int ops, Handler handler) throws IOException {
        channel.configureBlocking(false);
        return channel.register(selector, ops, handler);
    }

    /** Đánh dấu {@code target} có dữ liệu chờ ghi; nó được flush ở cuối vòng này. Chỉ gọi trên thread của vòng. */
    public void markDirty(Flushable target) {
        dirty.add(target);
    }

    @Override
    public void close() {
        running = false;
        selector.wakeup();
    }

    private void run() {
        long idleSince = 0;
        while (true) {
            int work = 0;
            try {
                work += poll();
                work += runCommands();
                work += flushDirty();
            } catch (Throwable t) {
                log.error("Agent loop {} failed", thread.getName(), t);
            }
            if (work > 0) {
                idleSince = 0;
                continue;
            }
            if (!running) {
                break; // việc đã xếp hàng trước khi dừng đã chạy hết
            }
            if (idleStrategy == IdleStrategy.BUSY) {
                Thread.onSpinWait();
                continue;
            }
            long now = System.nanoTime();
            if (idleSince == 0) {
                idleSince = now;
            }
            if (now - idleSince < spinNanos) {
                Thread.onSpinWait();
                continue;
            }
            idleSince = 0;
            // đặt cờ trước khi xem lại hàng: việc đến sau lần xem đó chắc chắn thấy cờ và đánh thức selector
            sleeping = true;
            try {
                if (commands.isEmpty() && running) {
                    selector.select(this::onSelected, BLOCKING_SELECT_MS);
                }
            } catch (IOException | RuntimeException e) {
                log.error("Agent loop {} select failed", thread.getName(), e);
            } finally {
                sleeping = false;
            }
        }
        try {
            for (SelectionKey key : selector.keys()) {
                key.channel().close();
            }
            selector.close();
        } catch (IOException e) {
            // Ignore close errors
        }
    }

    private int poll() throws IOException {
        if (selector.keys().isEmpty()) {
            return 0;
        }
        selectedCount = 0;
        selector.selectNow(this::onSelected);
        return selectedCount;
    }

    private void onSelected(SelectionKey key) {
        selectedCount++;
        try {
            ((Handler) key.attachment()).onSelected(key);
        } catch (RuntimeException e) {
            log.error("Agent loop {} handler failed", thread.getName(), e);
        }
    }

    private int runCommands() {
        int count = 0;
        Runnable task;
        while ((task = commands.poll()) != null) {
            count++;
            try {
                task.run();
            } catch (Throwable t) {
                log.error("Agent loop {} task failed", thread.getName(), t);
            }
        }
        return count;
    }

    private int flushDirty() {
        if (dirty.isEmpty()) {
            return 0;
        }
        // flush có thể đánh dấu lại (ví dụ kết nối vừa đóng báo lỗi cho các lời gọi, và callback gửi tiếp)
        flushing.addAll(dirty);
        dirty.clear();
        for (Flushable target : flushing) {
            try {
                target.flush();
            } catch (RuntimeException e) {
                log.error("Agent loop {} flush failed", thread.getName(), e);
            }
        }
        int count = flushing.size();
        flushing.clear();
        return count;
    }
}

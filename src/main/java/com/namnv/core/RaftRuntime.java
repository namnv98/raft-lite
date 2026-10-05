package com.namnv.core;

/**
 * Mọi thứ RaftNode cần từ môi trường chạy: đồng hồ, hẹn giờ, thread ghi đĩa và nguồn ngẫu nhiên.
 * Mặc định là {@link ThreadedRuntime}; test có thể thay bằng bản chạy trên một thread với thời gian ảo
 * để cả cluster chạy tất định theo một seed.
 */
public interface RaftRuntime {

    interface ScheduledTask {
        void cancel();
    }

    long nanoTime();

    ScheduledTask schedule(Runnable task, long delayMs);

    // các task chạy lần lượt theo thứ tự gửi vào, ngoài lock của node
    void executeIo(Runnable task);

    int nextInt(int bound);

    void shutdown();
}

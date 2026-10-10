package com.namnv.raft.runtime;

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

    /**
     * Giờ thực (epoch ms) mà leader gắn vào entry nó tạo ({@link com.namnv.entity.LogEntry#getTimestamp()}). Runtime mô
     * phỏng trả về thời gian ảo để cả cluster vẫn tất định.
     */
    default long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    ScheduledTask schedule(Runnable task, long delayMs);

    // các task chạy lần lượt theo thứ tự gửi vào, ngoài lock của node
    void executeIo(Runnable task);

    // việc đĩa không nằm trên đường trả lời client (đọc entry cũ của log cho một follower tụt xa, xoá phần log đã compact),
    // ngoài lock của node; không cần xếp hàng chung với việc ghi
    default void executeRead(Runnable task) {
        executeIo(task);
    }


    /**
     * Thread xử lý của node: mọi sự kiện của node (lệnh client, response RPC, timer, việc đĩa đã xong) chạy lần lượt ở đây.
     * Mặc định chạy ngay trên thread gọi, đúng cho runtime chỉ có một thread như bản mô phỏng trong test.
     *
     * @throws java.util.concurrent.RejectedExecutionException nếu runtime đã tắt
     */
    default void executeNode(Runnable task) {
        task.run();
    }

    int nextInt(int bound);

    void shutdown();
}

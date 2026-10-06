package com.namnv.rpc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Thread của transport TCP: mỗi kết nối một thread đọc, cùng các thread ghi và xử lý request dùng lại từ một pool.
 * <p>
 * Mặc định là thread của hệ điều hành. Thread đọc gần như lúc nào cũng đang chờ socket, và với virtual thread mỗi lần
 * có dữ liệu về là một lần bộ điều phối phải đánh thức rồi chạy lại nó; trên đường chạy nóng của Raft chi phí đó lớn
 * hơn cả việc xử lý message (đo được: độ trễ một lệnh giảm một nửa khi chuyển sang thread hệ điều hành).
 * Cái giá là mỗi kết nối chiếm một thread thật. Một node phải giữ hàng nghìn kết nối client cùng lúc có thể đổi lại
 * bằng {@code -Draft.rpc.virtualThreads=true}.
 */
public final class RpcThreads {

    private RpcThreads() {
    }

    public static ExecutorService newExecutor(String namePrefix) {
        if (Boolean.getBoolean("raft.rpc.virtualThreads")) {
            return Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name(namePrefix, 0).factory());
        }
        return Executors.newCachedThreadPool(Thread.ofPlatform().daemon().name(namePrefix, 0).factory());
    }
}

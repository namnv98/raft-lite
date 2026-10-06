package com.namnv.storage;

import java.io.IOException;

/**
 * Điểm chèn lỗi đĩa cho test: được gọi ngay trước mỗi thao tác ghi, ném IOException để giả lập thao tác đó thất bại.
 * Mặc định không làm gì.
 */
public interface DiskFaultInjector {
    DiskFaultInjector NONE = operation -> {
    };

    // operation: log.append, log.sync, meta.write, snapshot.commit
    void beforeWrite(String operation) throws IOException;
}

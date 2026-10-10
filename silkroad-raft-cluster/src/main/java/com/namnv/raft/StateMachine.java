package com.namnv.raft;

import com.namnv.entity.LogEntry;
import com.namnv.storage.snapshot.SnapshotReader;
import com.namnv.storage.snapshot.SnapshotWriter;

import java.util.concurrent.CompletableFuture;

public interface StateMachine {
    /**
     * Áp dụng một entry đã commit. Được gọi khi node đang giữ lock, theo đúng thứ tự index, nên cần nhanh.
     */
    void onApply(String node, LogEntry entry);

    /**
     * Như {@link #onApply} nhưng trả về kết quả của lệnh cho client đã gửi nó (ví dụ "không đủ tiền"), hoặc null nếu
     * không có. Kết quả phải chỉ phụ thuộc vào state và lệnh, để mọi node tính ra giống nhau. Chỉ kết quả trên leader
     * được gửi về client. Mặc định gọi {@link #onApply} và không có kết quả.
     */
    default byte[] onApplyWithResult(String node, LogEntry entry) {
        onApply(node, entry);
        return null;
    }

    /**
     * Lưu state hiện tại vào thư mục {@code snapshotWriter.getPath()}; future hoàn tất khi ghi xong, hoặc thất bại nếu
     * không ghi được. Được gọi khi node đang giữ lock: nên chụp bản sao state ngay trong lời gọi này, còn việc ghi file
     * thì làm ở thread khác. Ghi đồng bộ vẫn đúng nhưng sẽ chặn node trong lúc ghi.
     * Mỗi file đã ghi phải được đăng ký bằng {@code snapshotWriter.addFile(name)}.
     */
    CompletableFuture<Void> onSnapshotSave(SnapshotWriter snapshotWriter);

    /**
     * Thay toàn bộ state bằng snapshot trong {@code reader.getPath()}. Khi cài snapshot từ leader, lời gọi này
     * nằm ngoài lock của node, và node đảm bảo không có {@link #onApply} nào chạy xen vào.
     * Trả về false hoặc ném exception đều được coi là state có thể đã hỏng: node dừng hẳn để restart
     * dựng lại từ snapshot và log trên đĩa. Lúc khởi động thì node không start được.
     */
    boolean onSnapshotLoad(SnapshotReader reader);
}

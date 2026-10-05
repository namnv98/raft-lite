package com.namnv.entity;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.util.TreeSet;

/**
 * Các sequence của một client đã được apply. Sequence bắt đầu từ 1 và tăng liền nhau, nên chỉ cần nhớ
 * mốc "mọi sequence tới đây đã apply" cùng vài sequence lẻ phía trên mốc đó (các lệnh client gửi đồng thời
 * có thể vào log không theo thứ tự).
 */
@Data
public class ClientSession implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    // mọi sequence <= watermark đã được apply
    private long watermark;
    // các sequence > watermark đã được apply, còn chờ những sequence nhỏ hơn
    private TreeSet<Long> above = new TreeSet<>();

    public ClientSession() {
    }

    public ClientSession(ClientSession other) {
        this.watermark = other.watermark;
        this.above = new TreeSet<>(other.above);
    }

    public boolean applied(long sequence) {
        return sequence <= watermark || above.contains(sequence);
    }

    public void markApplied(long sequence) {
        above.add(sequence);
        while (!above.isEmpty() && above.first() == watermark + 1) {
            watermark = above.pollFirst();
        }
    }
}

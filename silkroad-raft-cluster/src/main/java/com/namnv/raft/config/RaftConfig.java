package com.namnv.raft.config;


import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
public class RaftConfig {
    private String self;
    @Builder.Default
    private List<String> peers = new ArrayList<>();
    /**
     * Learner cố định: nhận toàn bộ log như follower nhưng không bỏ phiếu, không được tính vào quorum và không bao giờ
     * làm leader (ví dụ một node chỉ để đẩy dữ liệu đã commit sang hệ thống khác). Mọi node, kể cả chính learner, khai báo
     * cùng một danh sách; danh sách này cố định từ lúc khởi động, không đổi qua thay đổi thành viên.
     */
    @Builder.Default
    private List<String> learners = new ArrayList<>();
}

package com.namnv.config;


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
}

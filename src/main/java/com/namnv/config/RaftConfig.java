package com.namnv.config;


import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

@Data
@Builder
public class RaftConfig {
    private static final String LEARNER_POSTFIX = "/learner";
    private String self;
    private List<String> peers = new ArrayList<>();
    private LinkedHashSet<String> learners = new LinkedHashSet<>();
}

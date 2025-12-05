package com.namnv.state;

import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Getter
public class LeaderState {
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();


    public LeaderState(List<String> peers, long lastLogIndex) {
        for (String p : peers) {
            nextIndex.put(p, lastLogIndex);
            matchIndex.put(p, 0L);
        }
    }
}
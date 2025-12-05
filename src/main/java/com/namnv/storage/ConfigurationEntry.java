package com.namnv.storage;

import lombok.Data;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Data
@ToString
public class ConfigurationEntry {
    private List<String> oldNodes = new ArrayList<>();  // cấu hình cũ
    private List<String> newNodes = new ArrayList<>();  // cấu hình mới
    private boolean isJoint = false;                   // joint config hay final

    public ConfigurationEntry() {
    }

    // Joint config constructor
    public ConfigurationEntry(List<String> oldNodes, List<String> newNodes, boolean isJoint) {
        this.oldNodes = new ArrayList<>(oldNodes);
        this.newNodes = new ArrayList<>(newNodes);
        this.isJoint = isJoint;
    }

    // Final config constructor
    public ConfigurationEntry(List<String> nodes) {
        this.oldNodes = new ArrayList<>();
        this.newNodes = new ArrayList<>(nodes);
        this.isJoint = false;
    }

    public boolean isJoint() {
        return isJoint;
    }
}

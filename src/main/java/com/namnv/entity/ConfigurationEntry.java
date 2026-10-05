package com.namnv.entity;

import lombok.Data;
import lombok.ToString;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.isNull;

@Data
@ToString
public class ConfigurationEntry implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private List<String> oldNodes = new ArrayList<>();  // cấu hình cũ
    private List<String> newNodes = new ArrayList<>();  // cấu hình mới
    private boolean isJoint = false;                   // joint config hay final

    public ConfigurationEntry() {
    }

    // Joint config constructor
    public ConfigurationEntry(List<String> oldNodes, List<String> newNodes, boolean isJoint) {
        if (isNull(oldNodes)) {
            this.oldNodes = new ArrayList<>();
        } else {
            this.oldNodes = new ArrayList<>(oldNodes);

        }
        if (isNull(newNodes)) {
            this.newNodes = new ArrayList<>();
        } else {
            this.newNodes = new ArrayList<>(newNodes);

        }
        this.isJoint = isJoint;
    }

    // Final config constructor
    public ConfigurationEntry(List<String> nodes) {
        this(nodes, null, false);
    }

    // toàn bộ node tham gia (old + new khi joint)
    public Set<String> allNodes() {
        Set<String> all = new LinkedHashSet<>(oldNodes);
        all.addAll(newNodes);
        return all;
    }

    public boolean contains(String nodeId) {
        return oldNodes.contains(nodeId) || newNodes.contains(nodeId);
    }

    // joint config cần majority ở cả cấu hình cũ lẫn cấu hình mới
    public boolean hasQuorum(Set<String> granted) {
        if (isJoint) {
            return hasMajority(oldNodes, granted) && hasMajority(newNodes, granted);
        }
        return hasMajority(oldNodes, granted);
    }

    private static boolean hasMajority(List<String> nodes, Set<String> granted) {
        long count = nodes.stream().filter(granted::contains).count();
        return !nodes.isEmpty() && count >= nodes.size() / 2 + 1;
    }

    public boolean isJoint() {
        return isJoint;
    }
}

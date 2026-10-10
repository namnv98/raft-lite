package com.namnv.entity;

import lombok.Data;
import lombok.ToString;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.ToLongFunction;

import static java.util.Objects.isNull;

@Data
@ToString
public class ConfigurationEntry implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private List<String> oldNodes = new ArrayList<>(); 
    private List<String> newNodes = new ArrayList<>(); 
    private boolean isJoint = false;                  

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

    /**
     * Index lớn nhất mà đa số node của cấu hình đã có (với joint config: đa số ở cả hai phía).
     */
    public long quorumIndex(ToLongFunction<String> matchIndexOf) {
        if (isJoint) {
            return Math.min(majorityIndex(oldNodes, matchIndexOf), majorityIndex(newNodes, matchIndexOf));
        }
        return majorityIndex(oldNodes, matchIndexOf);
    }

    private static long majorityIndex(List<String> nodes, ToLongFunction<String> matchIndexOf) {
        if (nodes.isEmpty()) {
            return 0;
        }
        long[] sorted = nodes.stream().mapToLong(matchIndexOf).sorted().toArray();
        // phần tử lớn thứ (size/2 + 1): đúng một đa số có index >= giá trị này
        return sorted[sorted.length - (sorted.length / 2 + 1)];
    }

    private static boolean hasMajority(List<String> nodes, Set<String> granted) {
        long count = nodes.stream().filter(granted::contains).count();
        return !nodes.isEmpty() && count >= nodes.size() / 2 + 1;
    }

    public boolean isJoint() {
        return isJoint;
    }
}

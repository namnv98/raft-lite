package com.namnv.entity;

import lombok.Data;
import lombok.ToString;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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
        this.oldNodes = new ArrayList<>();
        this.newNodes = new ArrayList<>(nodes);
        this.isJoint = false;
    }

    public boolean isJoint() {
        return isJoint;
    }
}

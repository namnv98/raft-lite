package com.namnv.state;

import lombok.Data;

import java.io.Serializable;

@Data
public class Snapshot implements Serializable {
    private long lastIncludedIndex;  // index của log cuối cùng trong snapshot
    private long lastIncludedTerm;   // term của log cuối cùng trong snapshot
    private byte[] state;            // dữ liệu state machine (serialize)
}


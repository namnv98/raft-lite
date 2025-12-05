package com.namnv.statemachine;

import com.namnv.core.Closure;
import com.namnv.storage.snapshot.SnapshotReader;
import com.namnv.entity.LogEntry;
import com.namnv.storage.snapshot.SnapshotWriter;

public interface StateMachine {
    void onApply(String node, LogEntry entry);

    void onSnapshotSave(SnapshotWriter snapshotWriter, Closure done);

    boolean onSnapshotLoad(SnapshotReader reader);
}
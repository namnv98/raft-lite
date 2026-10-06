package com.namnv.kv;

import java.nio.file.Path;

class LmdbKvStateMachineTest extends KvStateMachineContractTest {
    @Override
    protected BufferedKvStateMachine newMachine(Path dir, boolean sync, long batchIntervalMs, int batchLimit) {
        return new LmdbKvStateMachine(dir, sync, batchIntervalMs, batchLimit, 1L << 28);
    }
}

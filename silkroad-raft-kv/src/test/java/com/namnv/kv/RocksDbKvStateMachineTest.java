package com.namnv.kv;

import java.nio.file.Path;

class RocksDbKvStateMachineTest extends KvStateMachineContractTest {
    @Override
    protected BufferedKvStateMachine newMachine(Path dir, boolean sync, long batchIntervalMs, int batchLimit) {
        return new RocksDbKvStateMachine(dir, sync, batchIntervalMs, batchLimit);
    }
}

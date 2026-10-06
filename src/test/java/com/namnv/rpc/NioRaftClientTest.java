package com.namnv.rpc;

/**
 * Các test của {@link RaftClientTest} với transport NIO: node đọc ghi socket ngay trên vòng của nó, client dùng vòng riêng.
 */
class NioRaftClientTest extends RaftClientTest {
    @Override
    protected boolean nio() {
        return true;
    }
}

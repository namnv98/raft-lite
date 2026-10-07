package com.namnv.ledger.client;

import com.namnv.ledger.codec.LedgerCodec;
import com.namnv.ledger.model.LedgerAccount;
import com.namnv.ledger.model.LedgerBalance;
import com.namnv.ledger.model.LedgerResult;
import com.namnv.ledger.model.LedgerTotals;
import com.namnv.ledger.model.LedgerTransfer;
import com.namnv.client.RaftClient;
import com.namnv.entity.CommandBatch;
import com.namnv.rpc.MessageTransport;
import com.namnv.transport.nio.NioRpcClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Client của sổ cái. Lệnh đi tới leader; khi không rõ kết quả (leader đổi, mất mạng) lệnh được gửi lại, và nhờ id của
 * tài khoản / giao dịch là duy nhất nên lần gửi lại trả về {@link LedgerResult#EXISTS} thay vì ghi hai lần.
 * Gom nhiều giao dịch vào một lời gọi {@link #transfers} nhanh hơn nhiều so với gửi từng cái.
 */
public class LedgerClient implements AutoCloseable {
    private final RaftClient client;
    private final MessageTransport ownedTransport;

    /** kết nối tới các node ở {@code servers} (host:port) qua một transport NIO riêng */
    public LedgerClient(List<String> servers, long maxWaitMs) {
        var transport = new NioRpcClient(2000);
        this.ownedTransport = transport;
        // chống trùng bằng id của tài khoản / giao dịch, không dùng phiên của Raft (clientId null)
        this.client = new RaftClient(transport, servers, null, maxWaitMs);
    }

    /** dùng chung một transport với các client khác */
    public LedgerClient(MessageTransport transport, List<String> servers, long maxWaitMs) {
        this.ownedTransport = null;
        this.client = new RaftClient(transport, servers, null, maxWaitMs);
    }

    public CompletableFuture<List<LedgerResult>> createAccounts(List<LedgerAccount> accounts) {
        var commands = new ArrayList<byte[]>(accounts.size());
        accounts.forEach(account -> commands.add(LedgerCodec.createAccount(account)));
        return client.submitBatch(commands).thenApply(LedgerClient::results);
    }

    public CompletableFuture<LedgerResult> transfer(LedgerTransfer transfer) {
        return client.submit(LedgerCodec.transfer(transfer)).thenApply(LedgerCodec::result);
    }

    /** các giao dịch trong một entry của log; mỗi giao dịch được kiểm tra và ghi riêng, theo đúng thứ tự */
    public CompletableFuture<List<LedgerResult>> transfers(List<LedgerTransfer> transfers) {
        var commands = new ArrayList<byte[]>(transfers.size());
        transfers.forEach(transfer -> commands.add(LedgerCodec.transfer(transfer)));
        return client.submitBatch(commands).thenApply(LedgerClient::results);
    }

    /** đọc nhất quán: thấy mọi giao dịch đã được xác nhận trước lời gọi này */
    public CompletableFuture<List<LedgerBalance>> lookupAccounts(List<Long> ids) {
        return client.read(LedgerCodec.lookupAccounts(ids)).thenApply(answer -> LedgerCodec.accounts(ids, answer));
    }

    public CompletableFuture<List<LedgerTransfer>> lookupTransfers(List<Long> ids) {
        return client.read(LedgerCodec.lookupTransfers(ids)).thenApply(answer -> LedgerCodec.transfers(ids, answer));
    }

    public CompletableFuture<LedgerTotals> totals() {
        return client.read(LedgerCodec.totals()).thenApply(LedgerCodec::totals);
    }

    /** đọc nhất quán trên đúng một node (ví dụ một follower), để so sánh các bản sao */
    public CompletableFuture<LedgerTotals> totalsFrom(String server) {
        return client.readFrom(server, LedgerCodec.totals()).thenApply(LedgerCodec::totals);
    }

    public CompletableFuture<List<LedgerBalance>> lookupAccountsFrom(String server, List<Long> ids) {
        return client.readFrom(server, LedgerCodec.lookupAccounts(ids)).thenApply(answer -> LedgerCodec.accounts(ids, answer));
    }

    private static List<LedgerResult> results(byte[] batch) {
        var results = new ArrayList<LedgerResult>();
        CommandBatch.forEach(batch, result -> results.add(LedgerCodec.result(result)));
        return results;
    }

    @Override
    public void close() {
        client.close();
        if (ownedTransport != null) {
            ownedTransport.close();
        }
    }
}

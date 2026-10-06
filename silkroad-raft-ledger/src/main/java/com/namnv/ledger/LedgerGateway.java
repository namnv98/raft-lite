package com.namnv.ledger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Cổng HTTP/JSON trước sổ cái, kiểu API mà các hệ thống khác (app, core banking, đối tác) gọi tới. Bên trong, cổng nói
 * giao thức nhị phân với cụm Raft qua {@link LedgerClient}, và gom các request lẻ thành lô bằng {@link TransferBatcher}.
 * <pre>
 * POST /transfers   {"debitAccountId":2,"creditAccountId":3,"amount":100,"ledger":840}   header Idempotency-Key: ...
 * POST /accounts    {"id":2,"ledger":840,"flags":1}
 * GET  /accounts/{id}   GET /transfers/{id}   GET /totals   GET /stats (số lô và số giao dịch đã gom)
 * </pre>
 * Chống trùng: id của giao dịch là id trong body, hoặc nếu không có thì được suy ra (SHA-256) từ header
 * {@code Idempotency-Key}. Gửi lại cùng một key sau khi mất kết nối hay nhận 503 không bao giờ ghi hai lần: lần sau nhận 200
 * {@code EXISTS}.
 * <p>
 * Mã trả về: 201 ghi mới; 200 đã có từ lần gửi trước; 409 id đã dùng cho giao dịch khác; 422 bị từ chối theo quy tắc của
 * sổ (không đủ tiền, sai sổ...); 400 request sai; 404 không có; 503 không rõ kết quả (đổi leader, quá hạn, quá tải) — gửi
 * lại với cùng key.
 * <pre>
 * java ... com.namnv.ledger.LedgerGateway cổng host1:port1,host2:port2,host3:port3
 * </pre>
 * -Dgateway.batch=false: gửi mỗi giao dịch một lệnh Raft, không gom. -Dgateway.maxBatch=1000 -Dgateway.maxInflight=4:
 * xem {@link TransferBatcher}.
 */
@Slf4j
public final class LedgerGateway implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final long WAIT_SECONDS = 30;

    private final HttpServer server;
    private final ExecutorService executor;
    private final LedgerClient client;
    private final TransferBatcher batcher;

    record TransferRequest(Long id, long debitAccountId, long creditAccountId, long amount, int ledger) {
    }

    record AccountRequest(long id, int ledger, int flags) {
    }

    record Answer(long id, LedgerResult result) {
    }

    /** lỗi của request, trả về với mã {@code status} */
    private static final class HttpError extends Exception {
        final int status;

        HttpError(int status, String message) {
            super(message, null, false, false);
            this.status = status;
        }
    }

    private LedgerGateway(HttpServer server, ExecutorService executor, LedgerClient client, TransferBatcher batcher) {
        this.server = server;
        this.executor = executor;
        this.client = client;
        this.batcher = batcher;
    }

    /**
     * @param port    0 để chọn cổng trống
     * @param batcher gom giao dịch thành lô, hoặc null để gửi từng giao dịch một
     */
    public static LedgerGateway start(int port, LedgerClient client, TransferBatcher batcher) throws IOException {
        // mặc định HttpServer chỉ giữ 200 kết nối keep-alive rảnh và đóng những cái thừa trong khi client vẫn dùng lại
        // chúng (lỗi "header parser received no bytes" khi có hàng trăm client). Được đọc một lần, khi HttpServer đầu tiên
        // được tạo trong tiến trình.
        if (System.getProperty("sun.net.httpserver.maxIdleConnections") == null) {
            System.setProperty("sun.net.httpserver.maxIdleConnections", "10000");
        }
        var server = HttpServer.create(new InetSocketAddress(port), 4096);
        // mỗi request một virtual thread: chờ kết quả từ cụm Raft không giữ thread hệ điều hành nào
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(executor);
        var gateway = new LedgerGateway(server, executor, client, batcher);
        server.createContext("/transfers", gateway::handleTransfers);
        server.createContext("/accounts", gateway::handleAccounts);
        server.createContext("/totals", exchange -> gateway.handle(exchange, () -> {
            requireMethod(exchange, "GET");
            return Map.entry(200, client.totals().get(WAIT_SECONDS, TimeUnit.SECONDS));
        }));
        server.createContext("/stats", exchange -> gateway.handle(exchange, () -> {
            long[] counters = batcher != null ? batcher.counters() : new long[2];
            return Map.entry(200, Map.of("batching", batcher != null, "batches", counters[0], "transfers", counters[1]));
        }));
        server.start();
        return gateway;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private interface Action {
        Map.Entry<Integer, Object> run() throws Exception;
    }

    private void handleTransfers(HttpExchange exchange) {
        handle(exchange, () -> {
            Long id = pathId(exchange, "/transfers");
            if (id != null) {
                requireMethod(exchange, "GET");
                var found = client.lookupTransfers(List.of(id)).get(WAIT_SECONDS, TimeUnit.SECONDS).getFirst();
                return found == null ? notFound(id) : Map.entry(200, found);
            }
            requireMethod(exchange, "POST");
            var request = read(exchange, TransferRequest.class);
            long transferId = request.id() != null ? request.id() : idFromKey(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
            var transfer = new LedgerTransfer(transferId, request.debitAccountId(), request.creditAccountId(),
                    request.amount(), request.ledger());
            CompletableFuture<LedgerResult> result = batcher != null ? batcher.submit(transfer) : client.transfer(transfer);
            return answer(transferId, result.get(WAIT_SECONDS, TimeUnit.SECONDS));
        });
    }

    private void handleAccounts(HttpExchange exchange) {
        handle(exchange, () -> {
            Long id = pathId(exchange, "/accounts");
            if (id != null) {
                requireMethod(exchange, "GET");
                var found = client.lookupAccounts(List.of(id)).get(WAIT_SECONDS, TimeUnit.SECONDS).getFirst();
                return found == null ? notFound(id) : Map.entry(200, found);
            }
            requireMethod(exchange, "POST");
            var request = read(exchange, AccountRequest.class);
            var result = client.createAccounts(List.of(new LedgerAccount(request.id(), request.ledger(), request.flags())))
                    .get(WAIT_SECONDS, TimeUnit.SECONDS).getFirst();
            return answer(request.id(), result);
        });
    }

    private static Map.Entry<Integer, Object> answer(long id, LedgerResult result) {
        int status = switch (result) {
            case OK -> 201;
            case EXISTS -> 200;
            case EXISTS_WITH_DIFFERENT_FIELDS -> 409;
            case MALFORMED -> 400;
            default -> 422;
        };
        return Map.entry(status, new Answer(id, result));
    }

    private static Map.Entry<Integer, Object> notFound(long id) {
        return Map.entry(404, Map.of("id", id, "error", "not found"));
    }

    private void handle(HttpExchange exchange, Action action) {
        int status;
        Object body;
        try {
            var outcome = action.run();
            status = outcome.getKey();
            body = outcome.getValue();
        } catch (HttpError e) {
            status = e.status;
            body = Map.of("error", e.getMessage());
        } catch (ExecutionException | TimeoutException e) {
            // không rõ lệnh đã được ghi hay chưa: gửi lại với cùng Idempotency-Key là an toàn
            status = 503;
            body = Map.of("error", "outcome unknown, retry with the same Idempotency-Key: "
                    + (e.getCause() != null ? e.getCause().getMessage() : "timed out"));
        } catch (Exception e) {
            log.warn("gateway request failed", e);
            status = 500;
            body = Map.of("error", String.valueOf(e.getMessage()));
        }
        try (exchange) {
            byte[] bytes = JSON.writeValueAsBytes(body);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            if (status == 503) {
                exchange.getResponseHeaders().set("Retry-After", "1");
            }
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
        } catch (IOException e) {
            log.debug("could not answer the client", e);
        }
    }

    private static <T> T read(HttpExchange exchange, Class<T> type) throws IOException, HttpError {
        try (var in = exchange.getRequestBody()) {
            return JSON.readValue(in, type);
        } catch (JsonProcessingException e) {
            throw new HttpError(400, "malformed JSON: " + e.getOriginalMessage());
        }
    }

    private static void requireMethod(HttpExchange exchange, String method) throws HttpError {
        if (!exchange.getRequestMethod().equals(method)) {
            throw new HttpError(405, exchange.getRequestMethod() + " is not allowed here");
        }
    }

    /** id trong đường dẫn {@code prefix/{id}}, hoặc null nếu đường dẫn đúng là {@code prefix} */
    private static Long pathId(HttpExchange exchange, String prefix) throws HttpError {
        String path = exchange.getRequestURI().getPath();
        if (path.equals(prefix) || path.equals(prefix + "/")) {
            return null;
        }
        try {
            return Long.parseLong(path.substring(prefix.length() + 1));
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            throw new HttpError(404, "no such resource: " + path);
        }
    }

    /**
     * id của giao dịch suy ra từ Idempotency-Key: 63 bit đầu của SHA-256 (khác 0). Hai key khác nhau trùng id với xác suất
     * khoảng n²/2^64; khi đó giao dịch sau bị từ chối với 409, không bao giờ ghi đè.
     */
    static long idFromKey(String key) throws HttpError {
        if (key == null || key.isBlank()) {
            throw new HttpError(400, "a transfer needs an \"id\" or an Idempotency-Key header");
        }
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8));
            long id = 0;
            for (int i = 0; i < 8; i++) {
                id = (id << 8) | (hash[i] & 0xff);
            }
            id &= Long.MAX_VALUE;
            return id == 0 ? 1 : id;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        server.stop(0);
        executor.close();
        client.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: LedgerGateway <port> <host1:port1,host2:port2,...>");
            System.exit(2);
        }
        var client = new LedgerClient(Arrays.asList(args[1].split(",")), 10_000);
        TransferBatcher batcher = Boolean.parseBoolean(System.getProperty("gateway.batch", "true"))
                ? new TransferBatcher(client, Integer.getInteger("gateway.maxBatch", 1000),
                Integer.getInteger("gateway.maxInflight", 4), Integer.getInteger("gateway.maxQueued", 100_000))
                : null;
        var gateway = start(Integer.parseInt(args[0]), client, batcher);
        log.info("ledger gateway listening on port {} (batching {})", gateway.port(), batcher != null);
        Runtime.getRuntime().addShutdownHook(new Thread(gateway::close));
        Thread.currentThread().join();
    }
}

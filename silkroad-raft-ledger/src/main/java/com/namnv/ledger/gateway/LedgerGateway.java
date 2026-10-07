package com.namnv.ledger.gateway;

import com.namnv.ledger.client.LedgerClient;
import com.namnv.ledger.view.LedgerView;
import com.namnv.ledger.model.LedgerAccount;
import com.namnv.ledger.model.LedgerResult;
import com.namnv.ledger.model.LedgerTransfer;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.AsciiString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Cổng HTTP/JSON trước sổ cái, kiểu API mà các hệ thống khác (app, core banking, đối tác) gọi tới. Bên trong, cổng nói
 * giao thức nhị phân với cụm Raft qua {@link LedgerClient}, và gom các request lẻ thành lô bằng {@link TransferBatcher}.
 * <pre>
 * POST /transfers   {"debitAccountId":2,"creditAccountId":3,"amount":100,"ledger":840}   header Idempotency-Key: ...
 * POST /accounts    {"id":2,"ledger":840,"flags":1}
 * GET  /accounts/{id}   GET /transfers/{id}   GET /totals   GET /stats (số lô và số giao dịch đã gom)
 * GET  /accounts/{id}/entries?limit=50&amp;before=index.position    sao kê, từ phía truy vấn (cần {@link LedgerView})
 * GET  /view/position   sự kiện cuối cùng phía truy vấn đã có
 * </pre>
 * Chống trùng: id của giao dịch là id trong body, hoặc nếu không có thì được suy ra (SHA-256) từ header
 * {@code Idempotency-Key}. Gửi lại cùng một key sau khi mất kết nối hay nhận 503 không bao giờ ghi hai lần: lần sau nhận 200
 * {@code EXISTS}.
 * <p>
 * Mã trả về: 201 ghi mới; 200 đã có từ lần gửi trước; 409 id đã dùng cho giao dịch khác; 422 bị từ chối theo quy tắc của
 * sổ (không đủ tiền, sai sổ...); 400 request sai; 404 không có; 413 body quá lớn; 503 không rõ kết quả (đổi leader, quá
 * hạn, quá tải) — gửi lại với cùng key.
 * <p>
 * Chạy trên Netty (epoll trên Linux): vài event loop, không thread nào đứng chờ cụm Raft. Request được đọc ngay trên event
 * loop (body bằng parser streaming của Jackson, không dựng object trung gian), lệnh đi tới cụm, và trả lời được ghi khi
 * kết quả về, cũng trên event loop của kết nối. Client gửi nhiều request liền trên một kết nối (HTTP pipelining) nhận trả
 * lời đúng thứ tự.
 * <pre>
 * java ... com.namnv.ledger.gateway.LedgerGateway cổng host1:port1,host2:port2,host3:port3
 * </pre>
 * -Dgateway.batch=false: gửi mỗi giao dịch một lệnh Raft, không gom. -Dgateway.maxBatch=1000 -Dgateway.maxInflight=4:
 * xem {@link TransferBatcher}. -Dgateway.eventLoops: số event loop (mặc định 1/4 số CPU).
 */
@Slf4j
public final class LedgerGateway implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonFactory JSON_FACTORY = JSON.getFactory();
    private static final int MAX_BODY_BYTES = 64 * 1024;

    private static final AsciiString IDEMPOTENCY_KEY = AsciiString.cached("idempotency-key");
    private static final AsciiString RETRY_AFTER = AsciiString.cached("retry-after");
    private static final AsciiString ONE = AsciiString.cached("1");

    // các trường của body, theo thứ tự trong mảng giá trị
    private static final String[] TRANSFER_FIELDS = {"id", "debitAccountId", "creditAccountId", "amount", "ledger"};
    private static final String[] ACCOUNT_FIELDS = {"id", "ledger", "flags"};

    private final EventLoopGroup boss;
    private final EventLoopGroup workers;
    private Channel serverChannel;
    private final LedgerClient client;
    private final TransferBatcher batcher;
    // phía truy vấn (CQRS) cho sao kê, hoặc null
    private final LedgerView view;

    /** lỗi của request, trả về với mã {@code status} */
    private static final class HttpError extends Exception {
        final HttpResponseStatus status;

        HttpError(HttpResponseStatus status, String message) {
            super(message, null, false, false);
            this.status = status;
        }
    }

    /** trả lời đã sẵn sàng: mã và body JSON */
    private record Reply(HttpResponseStatus status, byte[] body) {
    }

    private LedgerGateway(EventLoopGroup boss, EventLoopGroup workers, LedgerClient client, TransferBatcher batcher,
                          LedgerView view) {
        this.view = view;
        this.boss = boss;
        this.workers = workers;
        this.client = client;
        this.batcher = batcher;
    }

    /**
     * @param port    0 để chọn cổng trống
     * @param batcher gom giao dịch thành lô, hoặc null để gửi từng giao dịch một
     */
    public static LedgerGateway start(int port, LedgerClient client, TransferBatcher batcher) throws InterruptedException {
        return start(port, client, batcher, null);
    }

    /** @param view phía truy vấn cho sao kê ({@code /accounts/{id}/entries}), hoặc null */
    public static LedgerGateway start(int port, LedgerClient client, TransferBatcher batcher, LedgerView view)
            throws InterruptedException {
        return start(port, client, batcher, view,
                Integer.getInteger("gateway.eventLoops", Math.max(1, Runtime.getRuntime().availableProcessors() / 4)));
    }

    public static LedgerGateway start(int port, LedgerClient client, TransferBatcher batcher, LedgerView view,
                                      int eventLoops) throws InterruptedException {
        boolean epoll = Epoll.isAvailable();
        EventLoopGroup boss = epoll ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1);
        EventLoopGroup workers = epoll ? new EpollEventLoopGroup(eventLoops) : new NioEventLoopGroup(eventLoops);
        Class<? extends ServerChannel> channelType = epoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
        var gateway = new LedgerGateway(boss, workers, client, batcher, view);
        try {
            gateway.serverChannel = new ServerBootstrap()
                    .group(boss, workers)
                    .channel(channelType)
                    .option(ChannelOption.SO_BACKLOG, 4096)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    new HttpServerCodec(),
                                    new HttpServerKeepAliveHandler(),
                                    new HttpObjectAggregator(MAX_BODY_BYTES),
                                    gateway.new Connection());
                        }
                    })
                    .bind(port).sync().channel();
            log.info("ledger gateway on port {} ({}, {} event loops, batching {})",
                    gateway.port(), epoll ? "epoll" : "nio", eventLoops, batcher != null);
            return gateway;
        } catch (RuntimeException | InterruptedException e) {
            boss.shutdownGracefully();
            workers.shutdownGracefully();
            throw e;
        }
    }

    public int port() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    /**
     * Một kết nối. Mọi phương thức chạy trên event loop của kết nối; trả lời đi ra theo đúng thứ tự request đến.
     */
    private final class Connection extends SimpleChannelInboundHandler<FullHttpRequest> {
        private final ArrayDeque<Slot> pending = new ArrayDeque<>();
        private byte[] scratch = new byte[256];

        private static final class Slot {
            boolean keepAlive;
            Reply reply;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
            var slot = new Slot();
            slot.keepAlive = HttpUtil.isKeepAlive(request);
            pending.add(slot);
            if (!request.decoderResult().isSuccess()) {
                complete(ctx, slot, error(HttpResponseStatus.BAD_REQUEST, "malformed HTTP request"));
                return;
            }
            CompletableFuture<Reply> reply;
            try {
                reply = route(request);
            } catch (HttpError e) {
                complete(ctx, slot, error(e.status, e.getMessage()));
                return;
            } catch (RuntimeException | IOException e) {
                log.warn("gateway request failed", e);
                complete(ctx, slot, error(HttpResponseStatus.INTERNAL_SERVER_ERROR, String.valueOf(e.getMessage())));
                return;
            }
            reply.whenComplete((answer, failure) -> {
                // kết quả từ cụm về trên thread của transport: chuyển sang event loop của kết nối để ghi
                Reply done = failure == null ? answer : unknownOutcome(failure);
                if (ctx.executor().inEventLoop()) {
                    complete(ctx, slot, done);
                } else {
                    ctx.executor().execute(() -> complete(ctx, slot, done));
                }
            });
        }

        private void complete(ChannelHandlerContext ctx, Slot slot, Reply reply) {
            slot.reply = reply;
            boolean wrote = false;
            while (!pending.isEmpty() && pending.peek().reply != null) {
                Slot head = pending.poll();
                ctx.write(response(ctx, head.reply, head.keepAlive));
                wrote = true;
            }
            if (wrote) {
                ctx.flush();
            }
        }

        private FullHttpResponse response(ChannelHandlerContext ctx, Reply reply, boolean keepAlive) {
            ByteBuf body = ctx.alloc().buffer(reply.body.length).writeBytes(reply.body);
            var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, reply.status, body);
            response.headers()
                    .set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                    .setInt(HttpHeaderNames.CONTENT_LENGTH, reply.body.length);
            if (reply.status == HttpResponseStatus.SERVICE_UNAVAILABLE) {
                response.headers().set(RETRY_AFTER, ONE);
            }
            HttpUtil.setKeepAlive(response, keepAlive);
            return response;
        }

        private CompletableFuture<Reply> route(FullHttpRequest request) throws HttpError, IOException {
            String uri = request.uri();
            int query = uri.indexOf('?');
            String path = query < 0 ? uri : uri.substring(0, query);
            HttpMethod method = request.method();
            if (path.equals("/transfers")) {
                requireMethod(method, HttpMethod.POST);
                return postTransfer(request);
            }
            if (path.startsWith("/transfers/")) {
                requireMethod(method, HttpMethod.GET);
                long id = pathId(path, "/transfers/");
                return client.lookupTransfers(List.of(id)).thenApply(found -> found.getFirst() == null
                        ? notFound(id) : json(HttpResponseStatus.OK, found.getFirst()));
            }
            if (path.equals("/accounts")) {
                requireMethod(method, HttpMethod.POST);
                long[] fields = parse(request.content(), ACCOUNT_FIELDS);
                long id = fields[0];
                var account = new LedgerAccount(id, (int) fields[1], (int) fields[2]);
                return client.createAccounts(List.of(account)).thenApply(results -> answer(id, results.getFirst()));
            }
            if (path.startsWith("/accounts/") && path.endsWith("/entries")) {
                requireMethod(method, HttpMethod.GET);
                long id = pathId(path.substring(0, path.length() - "/entries".length()), "/accounts/");
                return statement(id, new QueryStringDecoder(uri).parameters());
            }
            if (path.equals("/view/position")) {
                requireMethod(method, HttpMethod.GET);
                return requireView().position().thenApply(p -> json(HttpResponseStatus.OK, Map.of("position", p.toString())));
            }
            if (path.startsWith("/accounts/")) {
                requireMethod(method, HttpMethod.GET);
                long id = pathId(path, "/accounts/");
                return client.lookupAccounts(List.of(id)).thenApply(found -> found.getFirst() == null
                        ? notFound(id) : json(HttpResponseStatus.OK, found.getFirst()));
            }
            if (path.equals("/totals")) {
                requireMethod(method, HttpMethod.GET);
                return client.totals().thenApply(totals -> json(HttpResponseStatus.OK, totals));
            }
            if (path.equals("/stats")) {
                requireMethod(method, HttpMethod.GET);
                long[] counters = batcher != null ? batcher.counters() : new long[2];
                return CompletableFuture.completedFuture(json(HttpResponseStatus.OK,
                        Map.of("batching", batcher != null, "batches", counters[0], "transfers", counters[1])));
            }
            throw new HttpError(HttpResponseStatus.NOT_FOUND, "no such resource: " + path);
        }

        // sao kê từ phía truy vấn: các bút toán mới nhất của tài khoản, kèm vị trí để lấy trang kế tiếp
        private CompletableFuture<Reply> statement(long id, Map<String, List<String>> parameters) throws HttpError {
            LedgerView source = requireView();
            int limit;
            LedgerView.Position before = null;
            try {
                limit = parameters.containsKey("limit") ? Integer.parseInt(parameters.get("limit").getFirst()) : 50;
                if (parameters.containsKey("before")) {
                    before = LedgerView.Position.parse(parameters.get("before").getFirst());
                }
            } catch (IllegalArgumentException e) {
                throw new HttpError(HttpResponseStatus.BAD_REQUEST, "bad limit or before: " + e.getMessage());
            }
            if (limit < 1 || limit > 1000) {
                throw new HttpError(HttpResponseStatus.BAD_REQUEST, "limit must be between 1 and 1000");
            }
            int pageSize = limit;
            // vị trí của phía truy vấn đọc trước: các bút toán trả về chắc chắn không mới hơn nó
            return source.position().thenCombine(source.entries(id, pageSize, before), (position, entries) -> {
                var body = new java.util.LinkedHashMap<String, Object>();
                body.put("accountId", id);
                body.put("viewPosition", position.toString());
                body.put("entries", entries);
                body.put("next", entries.size() == pageSize
                        ? new LedgerView.Position(entries.getLast().logIndex(), entries.getLast().logPosition()).toString() : null);
                return json(HttpResponseStatus.OK, body);
            });
        }

        // đường nóng: không dựng object trung gian cho body, trả lời được ghi tay
        private CompletableFuture<Reply> postTransfer(FullHttpRequest request) throws HttpError, IOException {
            long[] fields = parse(request.content(), TRANSFER_FIELDS);
            long id;
            if (present(fields, 0)) {
                id = fields[0];
            } else {
                id = idFromKey(request.headers().get(IDEMPOTENCY_KEY));
            }
            var transfer = new LedgerTransfer(id, fields[1], fields[2], fields[3], (int) fields[4]);
            CompletableFuture<LedgerResult> result = batcher != null ? batcher.submit(transfer) : client.transfer(transfer);
            return result.thenApply(r -> answer(id, r));
        }

        /**
         * Đọc một object JSON phẳng chỉ có số (và null), lấy các trường {@code names}; trường lạ bị bỏ qua. Phần tử cuối
         * của mảng trả về là bitmask các trường có mặt với giá trị khác null.
         */
        private long[] parse(ByteBuf content, String[] names) throws HttpError, IOException {
            int length = content.readableBytes();
            byte[] bytes;
            int offset;
            if (content.hasArray()) {
                bytes = content.array();
                offset = content.arrayOffset() + content.readerIndex();
            } else {
                if (scratch.length < length) {
                    scratch = new byte[Math.max(length, scratch.length * 2)];
                }
                content.getBytes(content.readerIndex(), scratch, 0, length);
                bytes = scratch;
                offset = 0;
            }
            long[] values = new long[names.length + 1];
            try (JsonParser parser = JSON_FACTORY.createParser(bytes, offset, length)) {
                if (parser.nextToken() != JsonToken.START_OBJECT) {
                    throw new HttpError(HttpResponseStatus.BAD_REQUEST, "malformed JSON: expected an object");
                }
                JsonToken token;
                while ((token = parser.nextToken()) == JsonToken.FIELD_NAME) {
                    String name = parser.currentName();
                    JsonToken value = parser.nextToken();
                    int field = indexOf(names, name);
                    if (field < 0) {
                        parser.skipChildren();
                    } else if (value != JsonToken.VALUE_NULL) {
                        if (value != JsonToken.VALUE_NUMBER_INT) {
                            throw new HttpError(HttpResponseStatus.BAD_REQUEST, "\"" + name + "\" must be an integer");
                        }
                        values[field] = parser.getLongValue();
                        values[names.length] |= 1L << field;
                    }
                }
                if (token != JsonToken.END_OBJECT || parser.nextToken() != null) {
                    throw new HttpError(HttpResponseStatus.BAD_REQUEST, "malformed JSON");
                }
            } catch (JsonProcessingException e) {
                throw new HttpError(HttpResponseStatus.BAD_REQUEST, "malformed JSON: " + e.getOriginalMessage());
            }
            return values;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.debug("gateway connection failed", cause);
            ctx.close();
        }
    }

    private static boolean present(long[] values, int field) {
        return (values[values.length - 1] & (1L << field)) != 0;
    }

    private static int indexOf(String[] names, String name) {
        for (int i = 0; i < names.length; i++) {
            if (names[i].equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private static Reply answer(long id, LedgerResult result) {
        HttpResponseStatus status = switch (result) {
            case OK -> HttpResponseStatus.CREATED;
            case EXISTS -> HttpResponseStatus.OK;
            case EXISTS_WITH_DIFFERENT_FIELDS -> HttpResponseStatus.CONFLICT;
            case MALFORMED -> HttpResponseStatus.BAD_REQUEST;
            default -> HttpResponseStatus.UNPROCESSABLE_ENTITY;
        };
        // {"id":…,"result":"…"} ghi tay: đây là trả lời của mọi giao dịch
        String body = "{\"id\":" + id + ",\"result\":\"" + result.name() + "\"}";
        return new Reply(status, body.getBytes(StandardCharsets.US_ASCII));
    }

    private LedgerView requireView() throws HttpError {
        if (view == null) {
            throw new HttpError(HttpResponseStatus.NOT_IMPLEMENTED, "no ledger view configured (-Dgateway.viewJdbcUrl)");
        }
        return view;
    }

    private static Reply notFound(long id) {
        return json(HttpResponseStatus.NOT_FOUND, Map.of("id", id, "error", "not found"));
    }

    private static Reply error(HttpResponseStatus status, String message) {
        return json(status, Map.of("error", message));
    }

    private static Reply unknownOutcome(Throwable failure) {
        Throwable cause = failure instanceof CompletionException && failure.getCause() != null ? failure.getCause() : failure;
        // không rõ lệnh đã được ghi hay chưa: gửi lại với cùng Idempotency-Key là an toàn
        return error(HttpResponseStatus.SERVICE_UNAVAILABLE,
                "outcome unknown, retry with the same Idempotency-Key: " + cause.getMessage());
    }

    private static Reply json(HttpResponseStatus status, Object body) {
        try {
            return new Reply(status, JSON.writeValueAsBytes(body));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void requireMethod(HttpMethod actual, HttpMethod expected) throws HttpError {
        if (!actual.equals(expected)) {
            throw new HttpError(HttpResponseStatus.METHOD_NOT_ALLOWED, actual + " is not allowed here");
        }
    }

    private static long pathId(String path, String prefix) throws HttpError {
        try {
            return Long.parseLong(path.substring(prefix.length()));
        } catch (NumberFormatException e) {
            throw new HttpError(HttpResponseStatus.NOT_FOUND, "no such resource: " + path);
        }
    }

    /**
     * id của giao dịch suy ra từ Idempotency-Key: 63 bit đầu của SHA-256 (khác 0). Hai key khác nhau trùng id với xác suất
     * khoảng n²/2^64; khi đó giao dịch sau bị từ chối với 409, không bao giờ ghi đè.
     */
    static long idFromKey(String key) throws HttpError {
        if (key == null || key.isBlank()) {
            throw new HttpError(HttpResponseStatus.BAD_REQUEST, "a transfer needs an \"id\" or an Idempotency-Key header");
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
        serverChannel.close().syncUninterruptibly();
        boss.shutdownGracefully(0, 2, java.util.concurrent.TimeUnit.SECONDS).syncUninterruptibly();
        workers.shutdownGracefully(0, 2, java.util.concurrent.TimeUnit.SECONDS).syncUninterruptibly();
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
        // -Dgateway.viewJdbcUrl=jdbc:postgresql://...: sao kê đọc từ phía truy vấn mà learner ghi (JdbcEventSink)
        String viewUrl = System.getProperty("gateway.viewJdbcUrl");
        LedgerView view = viewUrl == null ? null : new LedgerView(viewUrl, System.getProperty("gateway.viewUser"),
                System.getProperty("gateway.viewPassword"), Integer.getInteger("gateway.viewThreads", 4));
        var gateway = start(Integer.parseInt(args[0]), client, batcher, view);
        Runtime.getRuntime().addShutdownHook(new Thread(gateway::close));
        Thread.currentThread().join();
    }
}

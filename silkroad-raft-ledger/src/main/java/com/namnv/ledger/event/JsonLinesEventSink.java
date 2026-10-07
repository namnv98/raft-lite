package com.namnv.ledger.event;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * Dòng sự kiện ghi vào một file, mỗi dòng một sự kiện JSON, fsync sau mỗi lô: đóng vai một topic Kafka mà hệ thống khác
 * đọc theo (tail) để dựng mô hình truy vấn, gửi thông báo hay đối soát. Khi mở lại, vị trí của dòng cuối cùng cho biết
 * đã ghi tới đâu, và các sự kiện được sinh lại không bị ghi trùng.
 */
public final class JsonLinesEventSink implements EventSink {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final FileChannel channel;
    private long lastIndex;
    private int lastPosition = -1;

    public JsonLinesEventSink(Path file) throws IOException {
        Files.createDirectories(file.toAbsolutePath().getParent());
        // một dòng ghi dở (mất điện giữa chừng) bị cắt bỏ: mọi dòng trước nó đều đã được fsync
        if (Files.exists(file)) {
            byte[] content = Files.readAllBytes(file);
            int end = content.length;
            while (end > 0 && content[end - 1] != '\n') {
                end--;
            }
            if (end < content.length) {
                try (var truncate = FileChannel.open(file, StandardOpenOption.WRITE)) {
                    truncate.truncate(end);
                }
            }
            if (end > 0) {
                int start = end - 1;
                while (start > 0 && content[start - 1] != '\n') {
                    start--;
                }
                var last = JSON.readValue(new String(content, start, end - 1 - start, StandardCharsets.UTF_8), LedgerEvent.class);
                lastIndex = last.index();
                lastPosition = last.position();
            }
        }
        channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    @Override
    public void write(List<LedgerEvent> events) throws IOException {
        var out = new StringBuilder();
        LedgerEvent newest = null;
        for (LedgerEvent event : events) {
            if (event.isAfter(lastIndex, lastPosition) && (newest == null || event.isAfter(newest.index(), newest.position()))) {
                out.append(JSON.writeValueAsString(event)).append('\n');
                newest = event;
            }
        }
        if (newest == null) {
            return;
        }
        var bytes = ByteBuffer.wrap(out.toString().getBytes(StandardCharsets.UTF_8));
        while (bytes.hasRemaining()) {
            channel.write(bytes);
        }
        channel.force(false);
        lastIndex = newest.index();
        lastPosition = newest.position();
    }

    @Override
    public long lastIndex() {
        return lastIndex;
    }

    @Override
    public int lastPosition() {
        return lastPosition;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}

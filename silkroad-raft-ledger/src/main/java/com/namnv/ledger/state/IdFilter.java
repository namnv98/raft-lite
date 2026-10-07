package com.namnv.ledger.state;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Bloom filter cho id của giao dịch: trả lời "chắc chắn chưa có" mà không phải đọc RocksDB, cho gần như mọi giao dịch mới.
 * Mọi bit của một id nằm trong cùng một khối 512 bit (một cache line), nên mỗi lần kiểm tra chỉ chạm bộ nhớ một lần.
 * Khoảng 12 bit mỗi id, tỉ lệ báo nhầm "có thể có" chừng 1%. Đầy một đoạn thì thêm đoạn mới, nên không có giới hạn
 * số id; mỗi đoạn thêm một lần chạm bộ nhớ cho mỗi lần kiểm tra.
 * <p>
 * Chỉ thread của node ghi; thread snapshot có thể đọc cùng lúc: thấy thêm vài bit mới cũng không sao, vì một filter
 * chứa nhiều id hơn chỉ báo nhầm "có thể có" nhiều hơn, không bao giờ báo sai "chưa có".
 */
final class IdFilter {
    private static final int BITS_PER_ID = 12;
    private static final int WORDS_PER_BLOCK = 8; // 512 bit
    private static final int PROBES = 6;
    private static final int CHUNK_BYTES = 1 << 20;

    private final int idsPerSegment;
    private final int blocksPerSegment;
    private final List<long[]> segments = new ArrayList<>();
    private int idsInLast;

    IdFilter(long expectedIds) {
        this.idsPerSegment = (int) Math.min(Integer.MAX_VALUE / BITS_PER_ID, Math.max(1 << 16, expectedIds));
        this.blocksPerSegment = Math.max(1, (int) ((long) idsPerSegment * BITS_PER_ID / 512));
        segments.add(new long[blocksPerSegment * WORDS_PER_BLOCK]);
    }

    /** chuẩn bị chỗ cho một id nữa (có thể cấp phát một đoạn mới), để {@link #add} không bao giờ cấp phát */
    void reserve() {
        if (idsInLast == idsPerSegment) {
            segments.add(new long[blocksPerSegment * WORDS_PER_BLOCK]);
            idsInLast = 0;
        }
    }

    void add(long id) {
        reserve();
        long[] words = segments.get(segments.size() - 1);
        long h = mix(id);
        int base = block(h) * WORDS_PER_BLOCK;
        for (int p = 0; p < PROBES; p++) {
            int bit = (int) (h >>> (p * 9)) & 511;
            words[base + (bit >>> 6)] |= 1L << (bit & 63);
        }
        idsInLast++;
    }

    /** false: chắc chắn chưa từng add; true: có thể đã add */
    boolean mightContain(long id) {
        long h = mix(id);
        int base = block(h) * WORDS_PER_BLOCK;
        for (long[] words : segments) {
            if (contains(words, base, h)) {
                return true;
            }
        }
        return false;
    }

    private static boolean contains(long[] words, int base, long h) {
        for (int p = 0; p < PROBES; p++) {
            int bit = (int) (h >>> (p * 9)) & 511;
            if ((words[base + (bit >>> 6)] & (1L << (bit & 63))) == 0) {
                return false;
            }
        }
        return true;
    }

    private int block(long h) {
        // 10 bit cao nhất không dùng cho các vị trí trong khối; chọn khối bằng phần còn lại của một hash khác
        long g = (h ^ (h >>> 31)) * 0xBF58476D1CE4E5B9L;
        return (int) Long.remainderUnsigned(g >>> 7, blocksPerSegment);
    }

    private static long mix(long id) {
        long z = id + 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    void clear() {
        segments.clear();
        segments.add(new long[blocksPerSegment * WORDS_PER_BLOCK]);
        idsInLast = 0;
    }

    /** ảnh của filter tại một thời điểm; gọi trên thread của node (chỉ chép danh sách, không chép các mảng bit) */
    Image image() {
        return new Image(idsPerSegment, blocksPerSegment, idsInLast, List.copyOf(segments));
    }

    record Image(int idsPerSegment, int blocksPerSegment, int idsInLast, List<long[]> segments) {
        /**
         * Ghi ở thread khác trong lúc node vẫn thêm id: các mảng bit có thể có thêm bit mới, tức filter ghi ra chứa
         * nhiều id hơn lúc chụp. Điều đó vô hại (chỉ báo nhầm "có thể có" thêm), còn mọi id có trước lúc chụp thì chắc
         * chắn có mặt.
         */
        void writeTo(DataOutputStream out) throws IOException {
            out.writeInt(idsPerSegment);
            out.writeInt(blocksPerSegment);
            out.writeInt(idsInLast);
            out.writeInt(segments.size());
            // theo khối 1 MB: chép cả đoạn mảng một lần thay vì gọi writeLong cho từng từ
            var chunk = java.nio.ByteBuffer.allocate(CHUNK_BYTES);
            for (long[] words : segments) {
                for (int from = 0; from < words.length; from += CHUNK_BYTES / Long.BYTES) {
                    int n = Math.min(CHUNK_BYTES / Long.BYTES, words.length - from);
                    chunk.clear();
                    chunk.asLongBuffer().put(words, from, n);
                    out.write(chunk.array(), 0, n * Long.BYTES);
                }
            }
        }
    }

    /** thay toàn bộ nội dung bằng một filter đã ghi bằng {@link #writeTo}; trả về false nếu kích thước khác */
    boolean readFrom(DataInputStream in) throws IOException {
        int ids = in.readInt();
        int blocks = in.readInt();
        int inLast = in.readInt();
        int count = in.readInt();
        if (ids != idsPerSegment || blocks != blocksPerSegment || count < 1) {
            return false;
        }
        segments.clear();
        var chunk = java.nio.ByteBuffer.allocate(CHUNK_BYTES);
        for (int s = 0; s < count; s++) {
            long[] words = new long[blocksPerSegment * WORDS_PER_BLOCK];
            for (int from = 0; from < words.length; from += CHUNK_BYTES / Long.BYTES) {
                int n = Math.min(CHUNK_BYTES / Long.BYTES, words.length - from);
                in.readFully(chunk.array(), 0, n * Long.BYTES);
                chunk.clear();
                chunk.asLongBuffer().get(words, from, n);
            }
            segments.add(words);
        }
        idsInLast = inLast;
        return true;
    }
}

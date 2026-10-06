package com.namnv.storage.binary;

import com.namnv.storage.DiskFaultInjector;
import lombok.Builder;
import lombok.Getter;

/**
 * Tham số của {@link BinaryLogStorage}. Tầng đồng thuận dựng nó từ NodeOptions; log không phụ thuộc vào tầng đó.
 * Ý nghĩa và giá trị mặc định giống các trường cùng tên của NodeOptions.
 */
@Getter
@Builder
public class LogOptions {
    // thư mục chứa các file segment
    private final String logUri;
    @Builder.Default
    private final DiskFaultInjector diskFaults = DiskFaultInjector.NONE;
    @Builder.Default
    private final int logCacheEntries = 16_384;
    @Builder.Default
    private final int logSegmentBytes = 64 << 20;
    @Builder.Default
    private final boolean logSync = true;
    @Builder.Default
    private final boolean logPreallocate = true;
}

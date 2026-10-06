# Đo Aeron Cluster trên cùng máy

Project Maven riêng (không thuộc build của Raft Lite) để chạy Aeron Cluster theo đúng kịch bản của
`com.namnv.bench.ClusterBenchmark`: 3 node là 3 tiến trình trên máy này, tiến trình thứ tư là client, mọi thứ đi qua
mạng loopback (UDP với Aeron). State machine (`EchoService`) chỉ đếm số lệnh và xác nhận từng lệnh, tương đương
`CountingMachine` của Raft Lite.

- `AeronNode`: một node = media driver + archive + consensus module + service container, dựng bằng `ClusterConfig`
  của `aeron-samples`.
- `AeronBench`: khởi động 3 node, kết nối một phiên client và giữ N lệnh đang chờ xác nhận cùng lúc (vòng kín, mỗi lệnh
  được gửi tiếp ngay khi lệnh trước của nó được xác nhận), rồi in TPS và độ trễ p50/p99/max.

## Chạy

```bash
cd bench/aeron-cluster
mvn -q compile
CP="target/classes:$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)"
OPENS="--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED"

# tham số: thư mục dữ liệu | mức fsync (0 = không fsync, 1 = fsync log)
java $OPENS -cp "$CP" bench.AeronBench target/aeron-data 0
# cấu hình độ trễ thấp: mọi thread của node và client quay liên tục (BusySpinIdleStrategy), media driver SHARED
java $OPENS -Dbench.tuned=true -cp "$CP" bench.AeronBench target/aeron-data 1
```

Tuỳ chọn: `-Dbench.seconds=5`, `-Dbench.payloads=128,4096`, `-Dbench.clients=1,32,512`,
`-Dbench.keep=true` (giữ log của các node), `-Dbench.nodeArgs="..."` (tham số JVM thêm cho các node).
Đặt thư mục dữ liệu trên ổ thật (không phải `/tmp` nếu đó là tmpfs) khi đo với fsync.

## Kết quả đã đo

i5-13500, NVMe, Aeron 1.53.3, lệnh 128 byte, cấu hình do mình tự đặt (không phải cấu hình được chuyên gia tune):

| | 1 client | 512 lệnh đang chờ |
|---|---|---|
| mặc định, không fsync | ~167 TPS (p50 5,8 ms) | — |
| busy-spin, không fsync | 30.700–34.300 TPS (p50 26–28 µs) | ~240.000 TPS |
| busy-spin, fsync mức 1 | ~1.770 TPS (p50 0,5 ms) | ~137.000 TPS |

Cấu hình mặc định dùng `BackoffIdleStrategy`: thread ngủ tới 1 ms khi rảnh và không có gì đánh thức nó, nên một client
gửi tuần tự chỉ đạt vài trăm lệnh mỗi giây.

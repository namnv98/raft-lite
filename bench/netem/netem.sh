#!/usr/bin/env bash
# Giả lập độ trễ mạng giữa các node của ClusterBenchmark bằng tc netem trên loopback (cần sudo).
#
#   bench/netem/netem.sh on 500us   # mỗi chiều 500 µs, tức RTT ~1 ms giữa các node
#   java -Dbench.nodeIps=true ... com.namnv.bench.ClusterBenchmark
#   bench/netem/netem.sh off
#
# Với Aeron Cluster (UDP) dùng "udp" thay cho "on" và -Dbench.netemIps=true cho bench.AeronBench.
#
# Chỉ gói tin có cả nguồn lẫn đích trong 127.0.1.0/24 bị làm chậm: với -Dbench.nodeIps=true mỗi node có một địa chỉ
# 127.0.1.N và nối tới node khác từ địa chỉ đó, còn client đi từ 127.0.0.1 nên không bị làm chậm.
#
# Bật cả RPS trên lo: netem thả gói từ bộ hẹn giờ chạy trên CPU bất kỳ, và lo đẩy gói vào hàng đợi của CPU đang chạy, nên
# các gói của cùng một kết nối có thể bị xử lý sai thứ tự. TCP coi đó là mất gói, gửi lại và có lúc chờ ~200 ms.
# RPS dồn mọi gói của một kết nối về cùng một CPU. Mạng thật qua card mạng không có hiện tượng này.
set -euo pipefail
RPS=/sys/class/net/lo/queues/rx-0/rps_cpus
SAVED=/tmp/silkroad-raft-netem-rps.orig

case "${1:-}" in
  on|udp)
    delay=${2:?"cần độ trễ một chiều, ví dụ 500us hoặc 5ms"}
    [ -f "$SAVED" ] || cat "$RPS" > "$SAVED"
    all=$(printf '%x' $(( (1 << $(nproc)) - 1 )))
    echo "$all" | sudo tee "$RPS" >/dev/null
    sudo tc qdisc del dev lo root 2>/dev/null || true
    # prio 4 band: gói thường đi các band 1-3 như cũ; gói khớp bộ lọc vào band 4 có netem
    sudo tc qdisc add dev lo root handle 1: prio bands 4 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1
    sudo tc qdisc add dev lo parent 1:4 handle 40: netem delay "$delay" limit 100000
    if [ "$1" = on ]; then
      sudo tc filter add dev lo parent 1: protocol ip prio 1 u32 \
        match ip src 127.0.1.0/24 match ip dst 127.0.1.0/24 flowid 1:4
    else
      # Aeron chỉ nhận interface=<địa chỉ> nếu địa chỉ đó được gắn trên một card mạng: gắn tạm địa chỉ của client lên lo
      ip -4 addr show dev lo | grep -q "127.0.3.1/32" || sudo ip addr add 127.0.3.1/32 dev lo
      # Aeron Cluster (bench/aeron-cluster, -Dbench.netemIps=true): gói UDP trả lời luồng quay về địa chỉ nguồn 127.0.0.1,
      # nên làm chậm mọi gói UDP trên lo trừ gói của client (cổng client của node ở 127.0.2.N, client ở 127.0.3.1)
      sudo tc filter add dev lo parent 1: protocol ip prio 1 u32 match ip dst 127.0.2.0/24 flowid 1:2
      sudo tc filter add dev lo parent 1: protocol ip prio 2 u32 match ip dst 127.0.3.0/24 flowid 1:2
      sudo tc filter add dev lo parent 1: protocol ip prio 3 u32 match ip src 127.0.3.0/24 flowid 1:2
      sudo tc filter add dev lo parent 1: protocol ip prio 4 u32 match ip protocol 17 0xff flowid 1:4
    fi
    echo "RTT giữa các node: $(ping -c 3 -i 0.2 -I 127.0.1.2 127.0.1.1 | tail -1 | cut -d/ -f5) ms"
    ;;
  off)
    sudo tc qdisc del dev lo root 2>/dev/null || true
    sudo ip addr del 127.0.3.1/32 dev lo 2>/dev/null || true
    if [ -f "$SAVED" ]; then
      sudo tee "$RPS" < "$SAVED" >/dev/null
      rm -f "$SAVED"
    fi
    echo "đã gỡ netem và trả lại rps_cpus"
    ;;
  *)
    echo "dùng: $0 on|udp <độ trễ một chiều> | off   (udp: cho bench/aeron-cluster)" >&2
    exit 1
    ;;
esac

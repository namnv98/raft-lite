#!/usr/bin/env bash
# Đo cổng HTTP của sổ cái bằng wrk, tất cả trong một file:
#   1. tìm wrk (biến WRK, hoặc trong PATH); không có thì tự build từ github.com/wg/wrk vào target/wrk
#   2. build project nếu chưa có (hoặc BUILD=1)
#   3. dựng cụm 3 node (3 tiến trình) + 10.000 tài khoản + hai cổng HTTP: "http" (mỗi request một lệnh Raft) và
#      "http-batch" (gom lô)
#   4. chạy wrk trên từng cổng với từng số kết nối, mỗi request là một POST /transfers với Idempotency-Key mới
#   5. in bảng tổng kết, kiểm tra sổ cân, rồi tắt mọi thứ
#
# Chạy từ bất kỳ đâu:
#   bench/wrk/run.sh                       # mặc định 1 64 512 kết nối, có fsync
#   bench/wrk/run.sh 64 512 2048
#   FSYNC=false bench/wrk/run.sh           # không fsync Raft log
#
# Biến môi trường:
#   WRK=/đường/dẫn/wrk   THREADS=4 (thread của wrk)   DURATION=10 (giây mỗi lượt)   WARMUP=3 (giây khởi động mỗi cổng)
#   FSYNC=true   SNAPSHOT_INTERVAL=2000000 (entry giữa hai snapshot)   MODES="http http-batch"
#   MAX_BATCH=1000 MAX_INFLIGHT=4 (của cổng gom lô)   EVENT_LOOPS (event loop Netty của cổng, mặc định 1/4 số CPU)
#   NODE_HEAP=2g   BUILD=1 (build lại)
#   WRK_CPUS=12-19 (ghim wrk vào các core này bằng taskset, ví dụ E-core, để bớt tranh CPU với cụm)
#   KEEP=1 (giữ thư mục dữ liệu và log của node trong silkroad-raft-ledger/target/wrk-bench)
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"

CONNECTIONS=${*:-1 64 512}
THREADS=${THREADS:-4}
DURATION=${DURATION:-10}
WARMUP=${WARMUP:-3}
FSYNC=${FSYNC:-true}
SNAPSHOT_INTERVAL=${SNAPSHOT_INTERVAL:-2000000}
MODES=${MODES:-http http-batch}
DATA_DIR=$ROOT/silkroad-raft-ledger/target/wrk-bench
WORK=$(mktemp -d)

log() { printf '\033[1m>> %s\033[0m\n' "$*" >&2; }

# ---------- 1. wrk ----------
if [ -z "${WRK:-}" ]; then
    if command -v wrk >/dev/null; then
        WRK=$(command -v wrk)
    elif [ -x "$ROOT/target/wrk/wrk" ]; then
        WRK=$ROOT/target/wrk/wrk
    else
        log "không thấy wrk: build từ github.com/wg/wrk vào target/wrk (cần git, gcc, make, openssl)"
        rm -rf "$ROOT/target/wrk"
        mkdir -p "$ROOT/target"
        git clone -q --depth 1 https://github.com/wg/wrk.git "$ROOT/target/wrk"
        make -C "$ROOT/target/wrk" -j"$(nproc)" >"$ROOT/target/wrk/build.log" 2>&1 \
            || { tail -20 "$ROOT/target/wrk/build.log"; exit 1; }
        WRK=$ROOT/target/wrk/wrk
    fi
fi
log "wrk: $WRK ($("$WRK" --version 2>&1 | head -1 | cut -d' ' -f1-2))"
if [ -n "${WRK_CPUS:-}" ]; then
    WRK_CMD=(taskset -c "$WRK_CPUS" "$WRK")
else
    WRK_CMD=("$WRK")
fi

# ---------- 2. build ----------
if [ "${BUILD:-0}" = 1 ] || [ ! -f target/ledger-cp.txt ] \
        || [ ! -f silkroad-raft-ledger/target/classes/com/namnv/ledger/GatewayBenchmark.class ]; then
    log "build project"
    mvn -q -o install -DskipTests -pl silkroad-raft-ledger -am 2>/dev/null \
        || mvn -q install -DskipTests -pl silkroad-raft-ledger -am
    mvn -q -o dependency:build-classpath -pl silkroad-raft-ledger -Dmdep.includeScope=runtime \
            -Dmdep.outputFile="$ROOT/target/ledger-cp.txt" 2>/dev/null \
        || mvn -q dependency:build-classpath -pl silkroad-raft-ledger -Dmdep.includeScope=runtime \
            -Dmdep.outputFile="$ROOT/target/ledger-cp.txt"
fi

# ---------- script Lua cho wrk ----------
# mỗi request chuyển 1 đơn vị giữa hai tài khoản ngẫu nhiên trong 2..10001 (tài khoản do cụm tạo sẵn, không giới hạn số dư)
# với Idempotency-Key mới; đếm các trả lời không phải 201
cat >"$WORK/transfer.lua" <<'LUA'
local threads = {}
local next_id = 0

function setup(thread)
   thread:set("tid", next_id)
   next_id = next_id + 1
   table.insert(threads, thread)
end

function init(args)
   counter = 0
   refused = 0
   prefix = string.format("wrk-%d-%d-%d-", os.time(), math.random(1, 1000000000), tid)
   math.randomseed(os.time() * 100 + tid)
end

function request()
   counter = counter + 1
   local from = math.random(2, 10001)
   local to = math.random(2, 10001)
   if to == from then
      to = (from == 2) and 3 or 2
   end
   local body = string.format('{"debitAccountId":%d,"creditAccountId":%d,"amount":1,"ledger":840}', from, to)
   return wrk.format("POST", "/transfers",
      { ["Content-Type"] = "application/json", ["Idempotency-Key"] = prefix .. counter }, body)
end

function response(status, headers, body)
   if status ~= 201 then
      refused = refused + 1
   end
end

function done(summary, latency, requests)
   local total = 0
   for _, thread in ipairs(threads) do
      total = total + thread:get("refused")
   end
   io.write(string.format("RESULT rps=%.0f p50=%.3f p90=%.3f p99=%.3f max=%.3f not201=%d errors=%d\n",
      summary.requests / (summary.duration / 1e6),
      latency:percentile(50) / 1000, latency:percentile(90) / 1000, latency:percentile(99) / 1000,
      latency.max / 1000, total,
      summary.errors.connect + summary.errors.read + summary.errors.write + summary.errors.timeout))
end
LUA

# ---------- 3. cụm + hai cổng ----------
BENCH_PID=
cleanup() {
    if [ -n "$BENCH_PID" ]; then
        kill "$BENCH_PID" 2>/dev/null || true
        wait "$BENCH_PID" 2>/dev/null || true
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

log "dựng cụm 3 node + 2 cổng (fsync $FSYNC, snapshot mỗi $SNAPSHOT_INTERVAL entry)"
java -Xmx2g \
    -Dgateway.modes=serve -Dgateway.serveSeconds=86400 \
    -Dgateway.maxBatch="${MAX_BATCH:-1000}" -Dgateway.maxInflight="${MAX_INFLIGHT:-4}" \
    -Dgateway.eventLoops="${EVENT_LOOPS:-$(( $(nproc) / 4 > 0 ? $(nproc) / 4 : 1 ))}" \
    -Dledger.logSync="$FSYNC" -Dledger.snapshotInterval="$SNAPSHOT_INTERVAL" \
    -Dledger.nodeHeap="${NODE_HEAP:-2g}" -Dledger.keep="$([ "${KEEP:-0}" = 1 ] && echo true || echo false)" \
    -cp "silkroad-raft-ledger/target/classes:$(cat target/ledger-cp.txt)" \
    com.namnv.ledger.bench.GatewayBenchmark "$DATA_DIR" >"$WORK/cluster.log" 2>&1 &
BENCH_PID=$!

until grep -q READY "$WORK/cluster.log" 2>/dev/null; do
    if ! kill -0 "$BENCH_PID" 2>/dev/null; then
        cat "$WORK/cluster.log"
        exit 1
    fi
    sleep 0.5
done
read -r PORT_HTTP PORT_BATCH < <(grep READY "$WORK/cluster.log" | sed -E 's/.*http=([0-9]+) http-batch=([0-9]+).*/\1 \2/')
log "sẵn sàng: http=localhost:$PORT_HTTP  http-batch=localhost:$PORT_BATCH"

stat_of() { # in "lô transfers" của cổng gom lô
    curl -s "http://localhost:$1/stats" | sed -E 's/.*"batches":([0-9]+).*/\1/;t;d' | tr -d '\n'
    printf ' '
    curl -s "http://localhost:$1/stats" | sed -E 's/.*"transfers":([0-9]+).*/\1/;t;d'
}

# ---------- 4. đo ----------
ROWS=()
for mode in $MODES; do
    case $mode in
        http) port=$PORT_HTTP ;;
        http-batch) port=$PORT_BATCH ;;
        *) echo "mode không hợp lệ: $mode" >&2; exit 2 ;;
    esac
    log "$mode: khởi động JIT ${WARMUP}s"
    "${WRK_CMD[@]}" -t"$THREADS" -c64 -d"${WARMUP}s" -s "$WORK/transfer.lua" "http://localhost:$port" >/dev/null
    for c in $CONNECTIONS; do
        t=$(( c < THREADS ? c : THREADS ))
        log "$mode: $c kết nối, $t thread, ${DURATION}s"
        before=$(stat_of "$port")
        out=$("${WRK_CMD[@]}" -t"$t" -c"$c" -d"${DURATION}s" --latency -s "$WORK/transfer.lua" "http://localhost:$port")
        after=$(stat_of "$port")
        echo "$out" | grep -vE "^RESULT" >&2
        result=$(echo "$out" | grep "^RESULT")
        batch=1
        if [ "$mode" = http-batch ]; then
            read -r b0 t0 <<<"$before"
            read -r b1 t1 <<<"$after"
            batch=$(awk -v b=$((b1 - b0)) -v t=$((t1 - t0)) 'BEGIN { printf "%.1f", b ? t / b : 0 }')
        fi
        ROWS+=("$mode $c $result batch=$batch")
    done
done

# ---------- 5. tổng kết ----------
echo
echo "# Cổng HTTP của sổ cái, đo bằng wrk ($THREADS thread, ${DURATION}s mỗi lượt); fsync $FSYNC; $(nproc) CPU"
echo
# tiêu đề viết sẵn: printf đếm byte, chữ có dấu làm lệch cột
echo "| cổng       |  kết nối |      req/s |   p50 ms |   p90 ms |   p99 ms |   max ms |  lô TB | không 201 |"
echo "|------------|----------|------------|----------|----------|----------|----------|--------|-----------|"
for row in "${ROWS[@]}"; do
    # shellcheck disable=SC2086
    set -- $row
    mode=$1 conns=$2
    get() { echo "$row" | sed -E "s/.* $1=([^ ]+).*/\1/"; }
    printf '| %-10s | %8s | %10s | %8s | %8s | %8s | %8s | %6s | %9s |\n' \
        "$mode" "$conns" "$(get rps)" "$(get p50)" "$(get p90)" "$(get p99)" "$(get max)" "$(get batch)" \
        "$(( $(get not201) + $(get errors) ))"
done
echo
echo "tổng của sổ (GET /totals): $(curl -s "http://localhost:$PORT_HTTP/totals")"

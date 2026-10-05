package com.namnv.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.namnv.entity.LogEntry;
import com.namnv.storage.Checksum;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Các bất biến của Raft và thao tác "mất điện", dùng chung cho test fault injection chạy thread thật
 * ({@link RaftChaosTest}) và chạy mô phỏng tất định ({@link RaftSimulationTest}).
 */
final class RaftInvariants {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private RaftInvariants() {
    }

    // một lần ghi của client; ackedAt = Long.MAX_VALUE nếu client không nhận được xác nhận (kết quả không rõ)
    record Op(String command, long invokedAt, long ackedAt) {
        boolean acked() {
            return ackedAt != Long.MAX_VALUE;
        }
    }

    // một lần đọc nhất quán thành công: thấy size lệnh đầu tiên, lệnh cuối cùng nó thấy là last
    record Read(long invokedAt, long completedAt, int size, String last) {
    }

    /**
     * Linearizability cho lần đọc: kết quả là một tiền tố của chuỗi đã commit, chứa mọi lệnh được xác nhận trước khi
     * lần đọc bắt đầu, không chứa lệnh nào được gửi sau khi nó kết thúc, và các lần đọc nối tiếp nhau không đi lùi.
     */
    static void assertReadsLinearizable(List<String> committed, Collection<Op> ops, List<Read> reads) {
        var position = new HashMap<String, Integer>();
        for (int i = 0; i < committed.size(); i++) {
            position.put(committed.get(i), i);
        }
        for (Read read : reads) {
            assertTrue(read.size() <= committed.size(), "read saw " + read.size() + " commands but only "
                    + committed.size() + " were ever committed");
            if (read.size() > 0) {
                assertTrue(committed.get(read.size() - 1).equals(read.last()),
                        "read saw " + read.last() + " at position " + (read.size() - 1) + " but "
                                + committed.get(read.size() - 1) + " was committed there");
            }
            for (Op op : ops) {
                var at = position.get(op.command());
                if (at == null) {
                    continue;
                }
                if (op.ackedAt() < read.invokedAt()) {
                    assertTrue(at < read.size(), "stale read: " + op.command() + " was acknowledged before the read"
                            + " started but the read only saw " + read.size() + " commands");
                }
                if (op.invokedAt() > read.completedAt()) {
                    assertTrue(at >= read.size(), "read saw " + op.command() + " which was sent after the read finished");
                }
            }
        }
        var byCompletion = reads.stream().sorted(Comparator.comparingLong(Read::completedAt)).toList();
        int largestFinished = 0;
        int cursor = 0;
        for (Read read : reads.stream().sorted(Comparator.comparingLong(Read::invokedAt)).toList()) {
            while (cursor < byCompletion.size() && byCompletion.get(cursor).completedAt() < read.invokedAt()) {
                largestFinished = Math.max(largestFinished, byCompletion.get(cursor++).size());
            }
            assertTrue(read.size() >= largestFinished, "read went backwards: saw " + read.size()
                    + " commands after an earlier read had already seen " + largestFinished);
        }
    }

    // State Machine Safety: hai node bất kỳ không bao giờ apply hai lệnh khác nhau ở cùng một vị trí
    static void checkAppliedPrefixes(Map<String, List<String>> stores, Collection<String> violations) {
        for (String a : stores.keySet()) {
            for (String b : stores.keySet()) {
                var first = stores.get(a);
                var second = stores.get(b);
                if (a.compareTo(b) >= 0 && first.size() == second.size() || first.size() > second.size()) {
                    continue;
                }
                for (int i = 0; i < first.size(); i++) {
                    if (!first.get(i).equals(second.get(i))) {
                        violations.add("applied sequences diverge between " + a + " and " + b + " at position " + i
                                + ": " + first.get(i) + " vs " + second.get(i));
                        break;
                    }
                }
            }
        }
    }

    // Log Matching: hai log có entry cùng index và cùng term thì giống hệt nhau từ đó trở về trước.
    // Mỗi cặp (index, term) xác định duy nhất phần log đứng trước nó, nên so sánh hai log đọc ở hai thời điểm khác nhau vẫn đúng.
    static void checkLogMatching(Map<String, List<LogEntry>> logs, Collection<String> violations) {
        var byIndex = new HashMap<String, Map<Long, LogEntry>>();
        for (var log : logs.entrySet()) {
            var entries = new HashMap<Long, LogEntry>();
            for (LogEntry entry : log.getValue()) {
                entries.put(entry.getIndex(), entry);
            }
            byIndex.put(log.getKey(), entries);
        }
        for (String a : logs.keySet()) {
            for (String b : logs.keySet()) {
                if (a.compareTo(b) >= 0) {
                    continue;
                }
                var first = byIndex.get(a);
                var second = byIndex.get(b);
                var common = first.keySet().stream().filter(second::containsKey).sorted(Comparator.reverseOrder()).toList();
                var matched = false;
                for (long index : common) {
                    var x = first.get(index);
                    var y = second.get(index);
                    var sameTerm = x.getTerm() == y.getTerm();
                    var same = sameTerm && Arrays.equals(x.getCommand(), y.getCommand())
                            && x.isConfigurationEntry() == y.isConfigurationEntry();
                    if (sameTerm && !same) {
                        violations.add("same index and term but different entries on " + a + " and " + b + ": " + x + " vs " + y);
                        break;
                    }
                    if (matched && !same) {
                        violations.add("logs of " + a + " and " + b + " match at a later index but differ at " + index
                                + ": " + x + " vs " + y);
                        break;
                    }
                    matched |= sameTerm;
                }
            }
        }
    }

    // không lệnh nào được apply hai lần
    static void assertNoDuplicates(List<String> committed) {
        var seen = new HashSet<String>();
        for (String command : committed) {
            assertTrue(seen.add(command), "command applied twice: " + command);
        }
    }

    // state machine không chứa lệnh nào mà client chưa từng gửi
    static void assertOnlyInvokedCommands(List<String> committed, Set<String> invoked) {
        for (String command : committed) {
            assertTrue(invoked.contains(command), "unknown command in state machine: " + command);
        }
    }

    // lệnh đã được xác nhận với client thì không bao giờ mất
    static void assertAckedCommandsSurvive(List<String> committed, Collection<Op> ops) {
        var present = new HashSet<>(committed);
        for (Op op : ops) {
            if (op.acked()) {
                assertTrue(present.contains(op.command()), "acknowledged command lost: " + op.command());
            }
        }
    }

    // Linearizability cho chuỗi ghi: lệnh A được xác nhận trước khi lệnh B được gửi thì A phải đứng trước B
    static void assertRealTimeOrder(List<String> committed, Collection<Op> ops) {
        var position = new HashMap<String, Integer>();
        for (int i = 0; i < committed.size(); i++) {
            position.put(committed.get(i), i);
        }
        var applied = ops.stream()
                .filter(op -> position.containsKey(op.command()))
                .sorted(Comparator.comparingInt((Op op) -> position.get(op.command())))
                .toList();
        Op latestInvoked = null;
        for (Op op : applied) {
            if (latestInvoked != null && op.ackedAt() < latestInvoked.invokedAt()) {
                fail(op.command() + " was acknowledged before " + latestInvoked.command()
                        + " was sent, but was applied after it");
            }
            if (latestInvoked == null || op.invokedAt() > latestInvoked.invokedAt()) {
                latestInvoked = op;
            }
        }
    }

    /**
     * Tắt node như bị mất điện: mọi thứ chưa fsync biến mất. Giữ lock của node để log không đổi trong lúc chụp lại
     * trạng thái đĩa; mọi lời ack node đã gửi ra trước thời điểm này đều dựa trên dữ liệu đã bền vững nên vẫn nằm trong bản chụp.
     */
    static void powerLoss(RaftNode node, Path folder) throws IOException {
        var meta = folder.resolve("raft_meta.json");
        var segments = new HashMap<Path, byte[]>();
        byte[] durableMeta;
        long durableIndex;
        node.getLock().lock();
        try {
            durableIndex = node.getPersistent().getLogStore().durableIndex();
            for (Path segment : logSegments(folder)) {
                segments.put(segment, Files.readAllBytes(segment));
            }
            // file meta chỉ đổi qua rename sau khi đã fsync, nên nội dung đang thấy chính là phần bền vững
            durableMeta = Files.exists(meta) ? Files.readAllBytes(meta) : null;
            node.shutdown();
        } finally {
            node.getLock().unlock();
        }

        // shutdown() vừa flush thêm; trả đĩa về đúng phần đã bền vững
        for (Path segment : logSegments(folder)) {
            Files.delete(segment);
        }
        for (var segment : segments.entrySet()) {
            var durable = dropUnsyncedEntries(segment.getValue(), durableIndex);
            if (durable.length > 0) {
                Files.write(segment.getKey(), durable);
            }
        }
        if (durableMeta == null) {
            Files.deleteIfExists(meta);
        } else {
            Files.write(meta, durableMeta);
        }
    }

    private static List<Path> logSegments(Path folder) throws IOException {
        try (var files = Files.list(folder)) {
            return files.filter(p -> p.getFileName().toString().matches("log_\\d+\\.jsonl")).toList();
        }
    }

    // giữ lại các dòng nguyên vẹn có index <= durableIndex
    private static byte[] dropUnsyncedEntries(byte[] segment, long durableIndex) throws IOException {
        var out = new ByteArrayOutputStream();
        int lineStart = 0;
        for (int i = 0; i < segment.length; i++) {
            if (segment[i] != '\n') {
                continue;
            }
            var payload = Checksum.decodeLine(segment, lineStart, i);
            if (payload == null || OBJECT_MAPPER.readValue(payload, LogEntry.class).getIndex() > durableIndex) {
                break;
            }
            out.write(segment, lineStart, i - lineStart + 1);
            lineStart = i + 1;
        }
        return out.toByteArray();
    }
}

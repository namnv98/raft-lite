package com.namnv.entity;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigurationEntryTest {

    @Test
    void simpleConfigurationNeedsMajority() {
        var conf = new ConfigurationEntry(List.of("A", "B", "C"));

        assertFalse(conf.hasQuorum(Set.of("A")));
        assertTrue(conf.hasQuorum(Set.of("A", "C")));
        // phiếu của node ngoài cấu hình không được tính
        assertFalse(conf.hasQuorum(Set.of("A", "X", "Y")));
        assertFalse(new ConfigurationEntry(List.of()).hasQuorum(Set.of("A")));
    }

    @Test
    void jointConfigurationNeedsMajorityOfBothSides() {
        var conf = new ConfigurationEntry(List.of("A", "B", "C"), List.of("C", "D", "E"), true);

        // đủ đa số cấu hình cũ nhưng thiếu cấu hình mới, và ngược lại
        assertFalse(conf.hasQuorum(Set.of("A", "B", "C")));
        assertFalse(conf.hasQuorum(Set.of("C", "D", "E")));
        assertTrue(conf.hasQuorum(Set.of("A", "C", "D")));
        assertTrue(conf.hasQuorum(Set.of("A", "B", "D", "E")));

        assertEquals(Set.of("A", "B", "C", "D", "E"), conf.allNodes());
        assertTrue(conf.contains("E"));
        assertFalse(conf.contains("X"));
    }

    @Test
    void quorumIndexIsTheHighestIndexHeldByAMajority() {
        var three = new ConfigurationEntry(List.of("A", "B", "C"));
        assertEquals(5, three.quorumIndex(Map.of("A", 9L, "B", 5L, "C", 1L)::get));
        assertEquals(0, three.quorumIndex(Map.of("A", 9L, "B", 0L, "C", 0L)::get));

        var four = new ConfigurationEntry(List.of("A", "B", "C", "D"));
        assertEquals(3, four.quorumIndex(Map.of("A", 9L, "B", 7L, "C", 3L, "D", 1L)::get));

        // joint: lấy giá trị nhỏ hơn giữa hai phía
        var joint = new ConfigurationEntry(List.of("A", "B", "C"), List.of("C", "D", "E"), true);
        assertEquals(2, joint.quorumIndex(Map.of("A", 9L, "B", 9L, "C", 9L, "D", 2L, "E", 1L)::get));
        assertEquals(0, new ConfigurationEntry(List.of()).quorumIndex(id -> 9L));
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

class BufferExtractorTest {

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldExtractFirstEntries() {
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", 2);
        map.put("c", 3);
        map.put("d", 4);

        List<Integer> result = BufferExtractor.extractFirstEntries(map, 2);

        assertThat(result).containsExactly(1, 2);
        assertThat(map).hasSize(2).containsKeys("c", "d");
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldExtractAllEntries() {
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", 2);

        List<Integer> result = BufferExtractor.extractFirstEntries(map, 2);

        assertThat(result).containsExactly(1, 2);
        assertThat(map).isEmpty();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldHandleCountLargerThanMapSize() {
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 1);

        List<Integer> result = BufferExtractor.extractFirstEntries(map, 5);

        assertThat(result).containsExactly(1);
        assertThat(map).isEmpty();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnEmptyListForEmptyMap() {
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();

        List<Integer> result = BufferExtractor.extractFirstEntries(map, 3);

        assertThat(result).isEmpty();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldExtractZeroEntries() {
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", 2);

        List<Integer> result = BufferExtractor.extractFirstEntries(map, 0);

        assertThat(result).isEmpty();
        assertThat(map).hasSize(2);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldPreserveInsertionOrder() {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("first", "A");
        map.put("second", "B");
        map.put("third", "C");
        map.put("fourth", "D");

        List<String> result = BufferExtractor.extractFirstEntries(map, 3);

        assertThat(result).containsExactly("A", "B", "C");
        assertThat(map).containsOnlyKeys("fourth");
    }
}

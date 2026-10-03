/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Unit test for {@link Collect}.
 *
 * @author Gunnar Morling
 */
class CollectTest {

    @Test
    public void unmodifiableSetForIteratorShouldReturnExpectedElements() {
        Set<Integer> values = Collect.unmodifiableSet(Arrays.asList(1, 2, 3, 42).iterator());
        assertThat(values).containsOnly(1, 2, 3, 42);
    }

    @Test
    void unmodifiableSetForIteratorShouldRaiseExceptionUponModification() {
        assertThrows(UnsupportedOperationException.class, () -> {
            Set<Integer> values = Collect.unmodifiableSet(Arrays.asList(1, 2, 3, 42).iterator());
            values.remove(1);
        });
    }

    @Test
    @FixFor("debezium/dbz#2784")
    public void fixedSizeMapShouldHoldUpToMaximumEntries() {
        final Map<String, String> map = Collect.fixedSizeMap(3);
        map.put("k1", "v1");
        map.put("k2", "v2");
        map.put("k3", "v3");

        assertThat(map).hasSize(3);
        assertThat(map).containsOnlyKeys("k1", "k2", "k3");
    }

    @Test
    @FixFor("debezium/dbz#2784")
    public void fixedSizeMapShouldEvictEldestEntryWhenExceedingCapacity() {
        final Map<String, String> map = Collect.fixedSizeMap(3);
        map.put("k1", "v1");
        map.put("k2", "v2");
        map.put("k3", "v3");
        map.put("k4", "v4");

        assertThat(map).hasSize(3);
        assertThat(map).containsOnlyKeys("k2", "k3", "k4");
    }

    @Test
    @FixFor("debezium/dbz#2784")
    public void fixedSizeMapShouldEvictLeastRecentlyUsedEntry() {
        final Map<String, String> map = Collect.fixedSizeMap(3);
        map.put("k1", "v1");
        map.put("k2", "v2");
        map.put("k3", "v3");

        // Access k1 so that k2 becomes the eldest (LRU) entry
        map.get("k1");
        map.put("k4", "v4");

        assertThat(map).hasSize(3);
        assertThat(map).containsOnlyKeys("k1", "k3", "k4");
    }

    @Test
    @FixFor("debezium/dbz#2784")
    public void fixedSizeMapWithCapacityOneShouldHoldSingleEntry() {
        final Map<String, String> map = Collect.fixedSizeMap(1);
        map.put("k1", "v1");

        assertThat(map).hasSize(1);
        assertThat(map).containsOnlyKeys("k1");

        map.put("k2", "v2");
        assertThat(map).hasSize(1);
        assertThat(map).containsOnlyKeys("k2");
    }
}

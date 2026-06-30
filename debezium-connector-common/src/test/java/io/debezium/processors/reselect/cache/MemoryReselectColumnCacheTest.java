/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.reselect.cache;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.processors.reselect.cache.ReselectColumnCache.Hit;
import io.debezium.processors.reselect.cache.ReselectColumnCache.RowCache;

/**
 * Unit tests for {@link MemoryReselectColumnCache}, exercising the row-scoped get/put/invalidate
 * contract, TTL expiry, key isolation across rows and columns, content-based comparison of binary key
 * values, schema-driven cache misses, and caching of {@code null} values.
 *
 * @author Gaurav Miglani
 */
public class MemoryReselectColumnCacheTest {

    private static final Schema INT_KEY = SchemaBuilder.struct().name("key").field("id", Schema.INT32_SCHEMA).build();
    private static final Schema BYTES_KEY = SchemaBuilder.struct().name("key").field("id", Schema.BYTES_SCHEMA).build();

    private MemoryReselectColumnCache cache;

    @BeforeEach
    public void before() {
        cache = new MemoryReselectColumnCache();
        cache.configure(Configuration.create()
                .with("reselect.cache.max.size", 1000)
                .with("reselect.cache.ttl.ms", 60_000)
                .build());
    }

    private static Struct intKey(int id) {
        return new Struct(INT_KEY).put("id", id);
    }

    private RowCache row(int id) {
        return cache.forRow(intKey(id));
    }

    @Test
    public void getReturnsEmptyOnMiss() {
        assertThat(row(1).get("data")).isEmpty();
    }

    @Test
    public void putThenGetReturnsValue() {
        row(1).put("data", "AAA");
        assertThat(row(1).get("data")).map(Hit::value).contains("AAA");
    }

    @Test
    public void invalidateRemovesEntry() {
        row(1).put("data", "AAA");
        row(1).invalidate("data");
        assertThat(row(1).get("data")).isEmpty();
    }

    @Test
    public void differentColumnsAndRowsAreIsolated() {
        row(1).put("data", "AAA");
        row(1).put("name", "BBB");
        row(2).put("data", "CCC");

        assertThat(row(1).get("data")).map(Hit::value).contains("AAA");
        assertThat(row(1).get("name")).map(Hit::value).contains("BBB");
        assertThat(row(2).get("data")).map(Hit::value).contains("CCC");
    }

    @Test
    public void invalidatingOneColumnLeavesOthersIntact() {
        final RowCache r = row(1);
        r.put("data", "AAA");
        r.put("name", "BBB");

        r.invalidate("data");

        assertThat(r.get("data")).isEmpty();
        assertThat(r.get("name")).map(Hit::value).contains("BBB");
    }

    @Test
    public void nullValuesAreCachedAndDistinguishedFromMiss() {
        row(1).put("data", null);

        // A cached null is a hit carrying a null value, not a miss.
        assertThat(row(1).get("data")).isPresent();
        assertThat(row(1).get("data").get().value()).isNull();

        // A column that was never cached is still a miss.
        assertThat(row(1).get("name")).isEmpty();
    }

    @Test
    public void binaryKeyValuesAreComparedByContent() {
        cache.forRow(new Struct(BYTES_KEY).put("id", new byte[]{ 1, 2, 3 })).put("data", "AAA");
        // A distinct byte[] instance with the same content must resolve to the same entry.
        assertThat(cache.forRow(new Struct(BYTES_KEY).put("id", new byte[]{ 1, 2, 3 })).get("data"))
                .map(Hit::value).contains("AAA");
        assertThat(cache.forRow(new Struct(BYTES_KEY).put("id", new byte[]{ 9, 9, 9 })).get("data")).isEmpty();
    }

    @Test
    public void differentKeySchemaDoesNotShareEntry() {
        // Same logical id value but a different key schema (e.g. after a DDL change) is a distinct key
        // and must not return the earlier entry, avoiding a false hit against stale data.
        final Schema renamed = SchemaBuilder.struct().name("key").field("pk", Schema.INT32_SCHEMA).build();
        cache.forRow(intKey(1)).put("data", "AAA");

        assertThat(cache.forRow(new Struct(renamed).put("pk", 1)).get("data")).isEmpty();
    }

    @Test
    public void columnBoundEvictsLruColumnWhenExceeded() {
        final MemoryReselectColumnCache bounded = new MemoryReselectColumnCache();
        bounded.configure(Configuration.create()
                .with("reselect.cache.max.size", 1000)
                .with("reselect.cache.ttl.ms", 60_000)
                .with("reselect.cache.max.columns.per.row", 3)
                .build());

        final RowCache r = bounded.forRow(intKey(99));
        r.put("col1", "A");
        r.put("col2", "B");
        r.put("col3", "C");
        // col4 pushes col1 out (LRU eviction on the inner map)
        r.put("col4", "D");

        // col4, col3, col2 should still be present; col1 was the LRU entry and was evicted
        assertThat(r.get("col4")).map(Hit::value).contains("D");
        assertThat(r.get("col3")).map(Hit::value).contains("C");
        assertThat(r.get("col2")).map(Hit::value).contains("B");
        assertThat(r.get("col1")).isEmpty();
    }

    @Test
    public void entryExpiresAfterTtl() throws Exception {
        final MemoryReselectColumnCache shortTtl = new MemoryReselectColumnCache();
        shortTtl.configure(Configuration.create()
                .with("reselect.cache.max.size", 1000)
                .with("reselect.cache.ttl.ms", 50)
                .build());

        shortTtl.forRow(intKey(1)).put("data", "AAA");
        assertThat(shortTtl.forRow(intKey(1)).get("data")).map(Hit::value).contains("AAA");

        Thread.sleep(80); // exceed the 50ms TTL
        assertThat(shortTtl.forRow(intKey(1)).get("data")).isEmpty();
    }
}
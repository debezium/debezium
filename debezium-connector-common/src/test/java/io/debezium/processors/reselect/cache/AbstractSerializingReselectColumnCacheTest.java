/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.reselect.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.processors.reselect.cache.ReselectColumnCache.Hit;
import io.debezium.processors.reselect.cache.ReselectColumnCache.RowCache;

/**
 * Unit tests for {@link AbstractSerializingReselectColumnCache} using an in-memory byte-store backend,
 * exercising the row-key encoding (schema-fingerprint identity, positional values, content-based binary
 * comparison), value round-trips including cached nulls, read-side TTL enforcement, and the degradation
 * contract for unsupported types and undecodable stored bytes.
 *
 * @author Chris Cranford
 */
public class AbstractSerializingReselectColumnCacheTest {

    private static final Schema INT_KEY = SchemaBuilder.struct().name("key").field("id", Schema.INT32_SCHEMA).build();
    private static final Schema BYTES_KEY = SchemaBuilder.struct().name("key").field("id", Schema.BYTES_SCHEMA).build();

    /**
     * A minimal backend over a concurrent map keyed by wrapped bytes, with a controllable clock so TTL
     * behavior is exercised deterministically, and per-operation failure switches so storage-failure
     * degradation is exercised as well.
     */
    private static class InMemoryByteStoreCache extends AbstractSerializingReselectColumnCache {

        private final Map<ByteBuffer, byte[]> store = new ConcurrentHashMap<>();
        private long clockMs;
        private boolean failGets;
        private boolean failPuts;
        private boolean failRemoves;

        @Override
        protected void configureStorage(Configuration config) {
        }

        @Override
        protected byte[] getFromStorage(byte[] key) {
            if (failGets) {
                throw new RuntimeException("simulated storage read failure");
            }
            return store.get(ByteBuffer.wrap(key));
        }

        @Override
        protected void putToStorage(byte[] key, byte[] value) {
            if (failPuts) {
                throw new RuntimeException("simulated storage write failure");
            }
            store.put(ByteBuffer.wrap(key), value);
        }

        @Override
        protected void removeFromStorage(byte[] key) {
            if (failRemoves) {
                throw new RuntimeException("simulated storage removal failure");
            }
            store.remove(ByteBuffer.wrap(key));
        }

        @Override
        public void close() {
            store.clear();
        }

        @Override
        protected long currentTimeMillis() {
            return clockMs;
        }
    }

    private InMemoryByteStoreCache cache;

    @BeforeEach
    public void before() {
        cache = new InMemoryByteStoreCache();
        cache.configure(Configuration.empty());
    }

    private static Struct intKey(int id) {
        return new Struct(INT_KEY).put("id", id);
    }

    private RowCache row(int id) {
        return cache.forRow(intKey(id));
    }

    @Test
    public void putThenGetRoundTripsThroughBytes() {
        row(1).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        assertThat(row(1).get("data")).map(Hit::value).contains("AAA");
        assertThat(row(1).get("other")).isEmpty();
        assertThat(row(2).get("data")).isEmpty();
    }

    @Test
    public void cachedNullIsAHitNotAMiss() {
        row(1).put("data", Schema.OPTIONAL_STRING_SCHEMA, null);
        assertThat(row(1).get("data")).isPresent();
        assertThat(row(1).get("data").get().value()).isNull();
    }

    @Test
    public void invalidateRemovesOnlyThatColumn() {
        final RowCache r = row(1);
        r.put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        r.put("name", Schema.OPTIONAL_STRING_SCHEMA, "BBB");

        r.invalidate("data");

        assertThat(r.get("data")).isEmpty();
        assertThat(r.get("name")).map(Hit::value).contains("BBB");
    }

    @Test
    public void keySchemaChangeYieldsMiss() {
        // Same logical id value but a renamed key field (e.g. after DDL) must not hit the stale entry.
        final Schema renamed = SchemaBuilder.struct().name("key").field("pk", Schema.INT32_SCHEMA).build();
        row(1).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");

        assertThat(cache.forRow(new Struct(renamed).put("pk", 1)).get("data")).isEmpty();
    }

    @Test
    public void primaryKeyReorderYieldsMiss() {
        final Schema ab = SchemaBuilder.struct().name("key")
                .field("a", Schema.INT32_SCHEMA).field("b", Schema.INT32_SCHEMA).build();
        final Schema ba = SchemaBuilder.struct().name("key")
                .field("b", Schema.INT32_SCHEMA).field("a", Schema.INT32_SCHEMA).build();

        cache.forRow(new Struct(ab).put("a", 1).put("b", 2)).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");

        assertThat(cache.forRow(new Struct(ba).put("a", 1).put("b", 2)).get("data")).isEmpty();
    }

    @Test
    public void binaryKeyValuesAreComparedByContent() {
        cache.forRow(new Struct(BYTES_KEY).put("id", new byte[]{ 1, 2, 3 })).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");

        // A distinct byte[] instance, and a ByteBuffer, with the same content resolve to the same entry.
        assertThat(cache.forRow(new Struct(BYTES_KEY).put("id", new byte[]{ 1, 2, 3 })).get("data"))
                .map(Hit::value).contains("AAA");
        assertThat(cache.forRow(new Struct(BYTES_KEY).put("id", ByteBuffer.wrap(new byte[]{ 1, 2, 3 }))).get("data"))
                .map(Hit::value).contains("AAA");
        assertThat(cache.forRow(new Struct(BYTES_KEY).put("id", new byte[]{ 9, 9, 9 })).get("data")).isEmpty();
    }

    @Test
    public void nestedStructKeySerializesPositionally() {
        // A VariableScaleDecimal primary key (e.g. Oracle NUMBER) is a nested struct in the message key.
        final Schema keySchema = SchemaBuilder.struct().name("key")
                .field("id", VariableScaleDecimal.schema()).build();
        final Struct key = new Struct(keySchema)
                .put("id", VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new BigDecimal("42.42")));

        cache.forRow(key).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");

        final Struct sameKey = new Struct(keySchema)
                .put("id", VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new BigDecimal("42.42")));
        final Struct otherKey = new Struct(keySchema)
                .put("id", VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new BigDecimal("99.99")));

        assertThat(cache.forRow(sameKey).get("data")).map(Hit::value).contains("AAA");
        assertThat(cache.forRow(otherKey).get("data")).isEmpty();
    }

    @Test
    public void zeroTtlNeverExpires() {
        row(1).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        cache.clockMs += 1_000_000_000L;
        assertThat(row(1).get("data")).map(Hit::value).contains("AAA");
    }

    @Test
    public void positiveTtlExpiresAndDeletesAtRead() {
        final InMemoryByteStoreCache ttlCache = new InMemoryByteStoreCache();
        ttlCache.configure(Configuration.create().with("reselect.cache.ttl.ms", 100).build());

        ttlCache.forRow(intKey(1)).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        ttlCache.clockMs += 99;
        assertThat(ttlCache.forRow(intKey(1)).get("data")).map(Hit::value).contains("AAA");

        ttlCache.clockMs += 1;
        assertThat(ttlCache.forRow(intKey(1)).get("data")).isEmpty();
        // The expired entry was deleted from storage, not just skipped.
        assertThat(ttlCache.store).isEmpty();
    }

    @Test
    public void unsupportedValueTypeIsSkippedNotFatal() {
        row(1).put("data", null, new Object());
        assertThat(row(1).get("data")).isEmpty();
        assertThat(cache.store).isEmpty();
    }

    @Test
    public void undecodableStoredBytesAreDeletedAndTreatedAsMiss() {
        row(1).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        // Corrupt the stored entry as a stand-in for a format change.
        cache.store.replaceAll((k, v) -> new byte[]{ 99 });

        assertThat(row(1).get("data")).isEmpty();
        assertThat(cache.store).isEmpty();
    }

    @Test
    public void storageReadFailureDegradesToMiss() {
        // The cache is an optimization: a failing backend must surface as a miss, never as an exception
        // that would fail event processing, so the column falls back to re-selection.
        row(1).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        cache.failGets = true;

        assertThat(row(1).get("data")).isEmpty();

        // The entry is untouched and served again once the backend recovers.
        cache.failGets = false;
        assertThat(row(1).get("data")).map(Hit::value).contains("AAA");
    }

    @Test
    public void storageWriteFailureIsSkippedNotFatal() {
        cache.failPuts = true;
        row(1).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");

        cache.failPuts = false;
        assertThat(row(1).get("data")).isEmpty();
        assertThat(cache.store).isEmpty();
    }

    @Test
    public void storageRemovalFailureDuringUndecodableReadStillYieldsMiss() {
        row(1).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        cache.store.replaceAll((k, v) -> new byte[]{ 99 });
        cache.failRemoves = true;

        assertThat(row(1).get("data")).isEmpty();
    }

    @Test
    public void storageRemovalFailureDuringInvalidateIsNotFatal() {
        row(1).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        cache.failRemoves = true;

        row(1).invalidate("data");

        cache.failRemoves = false;
        assertThat(row(1).get("data")).map(Hit::value).contains("AAA");
    }
}
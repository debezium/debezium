/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.rocksdb.reselect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.processors.reselect.cache.ReselectColumnCache.Hit;

/**
 * Unit tests for {@link RocksDbReselectColumnCache}. RocksDB runs embedded, so these cover the full
 * store-bytes-on-disk path without a database: value round-trips, persistence across close and reopen,
 * cleanup semantics, required-path validation, TTL expiry, and concurrent access.
 *
 * @author Chris Cranford
 */
public class RocksDbReselectColumnCacheTest {

    private static final Schema INT_KEY = SchemaBuilder.struct().name("key").field("id", Schema.INT32_SCHEMA).build();

    @TempDir
    Path tempDir;

    private RocksDbReselectColumnCache cache;

    @AfterEach
    public void after() {
        if (cache != null) {
            cache.close();
            cache = null;
        }
    }

    private RocksDbReselectColumnCache open(Configuration config) {
        final RocksDbReselectColumnCache opened = new RocksDbReselectColumnCache();
        opened.configure(config);
        return opened;
    }

    private Configuration.Builder config() {
        return Configuration.create().with(RocksDbReselectColumnCache.PATH, tempDir.resolve("reselect-cache").toString());
    }

    private static Struct intKey(int id) {
        return new Struct(INT_KEY).put("id", id);
    }

    @Test
    public void putThenGetRoundTripsThroughRocksDb() {
        cache = open(config().build());

        cache.forRow(intKey(1)).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        cache.forRow(intKey(1)).put("empty", Schema.OPTIONAL_STRING_SCHEMA, null);

        assertThat(cache.forRow(intKey(1)).get("data")).map(Hit::value).contains("AAA");
        assertThat(cache.forRow(intKey(1)).get("empty")).isPresent();
        assertThat(cache.forRow(intKey(1)).get("empty").get().value()).isNull();
        assertThat(cache.forRow(intKey(1)).get("missing")).isEmpty();
        assertThat(cache.forRow(intKey(2)).get("data")).isEmpty();
    }

    @Test
    public void entriesSurviveCloseAndReopen() {
        cache = open(config().build());
        cache.forRow(intKey(1)).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        cache.close();

        cache = open(config().build());
        assertThat(cache.forRow(intKey(1)).get("data")).map(Hit::value).contains("AAA");
    }

    @Test
    public void cleanupOnCloseRemovesDatabaseDirectory() {
        final Path dbDir = tempDir.resolve("reselect-cache");
        cache = open(config().with(RocksDbReselectColumnCache.CLEANUP, true).build());
        cache.forRow(intKey(1)).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        cache.close();
        cache = null;

        assertThat(dbDir).doesNotExist();
    }

    @Test
    public void directoryIsRetainedOnCloseByDefault() {
        final Path dbDir = tempDir.resolve("reselect-cache");
        cache = open(config().build());
        cache.forRow(intKey(1)).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        cache.close();
        cache = null;

        assertThat(dbDir).exists();
    }

    @Test
    public void missingPathFailsWithPropertyName() {
        assertThatThrownBy(() -> open(Configuration.empty()))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining(RocksDbReselectColumnCache.PATH);
    }

    @Test
    public void positiveTtlExpiresEntries() throws Exception {
        cache = open(config().with("reselect.cache.ttl.ms", 100).build());

        cache.forRow(intKey(1)).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        assertThat(cache.forRow(intKey(1)).get("data")).map(Hit::value).contains("AAA");

        Thread.sleep(150); // exceed the 100ms TTL; expiry is enforced at read time
        assertThat(cache.forRow(intKey(1)).get("data")).isEmpty();
    }

    @Test
    public void concurrentPutsAndGetsAreConsistent() throws Exception {
        cache = open(config().build());

        final int threads = 4;
        final int rowsPerThread = 100;
        final CountDownLatch start = new CountDownLatch(1);
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            final List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int thread = t;
                futures.add(executor.submit(() -> {
                    start.await();
                    for (int i = 0; i < rowsPerThread; i++) {
                        final int id = thread * rowsPerThread + i;
                        cache.forRow(intKey(id)).put("data", Schema.OPTIONAL_STRING_SCHEMA, "value-" + id);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        }
        finally {
            executor.shutdownNow();
        }

        for (int id = 0; id < threads * rowsPerThread; id++) {
            assertThat(cache.forRow(intKey(id)).get("data")).map(Hit::value).contains("value-" + id);
        }
    }
}
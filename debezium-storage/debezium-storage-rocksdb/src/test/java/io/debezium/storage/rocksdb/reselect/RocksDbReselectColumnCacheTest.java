/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.rocksdb.reselect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.TtlDB;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.processors.reselect.cache.ReselectColumnCache.Hit;
import io.debezium.storage.rocksdb.RocksDbUtil;

/**
 * Unit tests for {@link RocksDbReselectColumnCache}. RocksDB runs embedded, so these cover the full
 * store-bytes-on-disk path without a database: value round-trips, persistence across close and reopen,
 * cleanup semantics, required-path validation, TTL expiry, and concurrent access.
 *
 * @author Chris Cranford
 */
public class RocksDbReselectColumnCacheTest {

    private static final Schema INT_KEY = SchemaBuilder.struct().name("key").field("id", Schema.INT32_SCHEMA).build();
    private static final Pattern OPTIONS_FILE_NAME = Pattern.compile("OPTIONS-\\d+");
    private static final Pattern PERIODIC_COMPACTION_OPTION = Pattern.compile("^\\s*periodic_compaction_seconds=(\\d+)\\s*$", Pattern.MULTILINE);

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

    /**
     * Returns the {@code periodic_compaction_seconds} value recorded in the newest {@code OPTIONS-<number>}
     * file that RocksDB persists in the given database directory, i.e. the effective value it was opened with.
     */
    private static long persistedPeriodicCompactionSeconds(Path dbDir) throws IOException {
        final Path latest;
        try (Stream<Path> files = Files.list(dbDir)) {
            latest = files
                    .filter(p -> OPTIONS_FILE_NAME.matcher(p.getFileName().toString()).matches())
                    .max(Comparator.comparingLong(p -> Long.parseLong(p.getFileName().toString().substring("OPTIONS-".length()))))
                    .orElseThrow(() -> new AssertionError("No RocksDB OPTIONS file found in " + dbDir));
        }
        final Matcher matcher = PERIODIC_COMPACTION_OPTION.matcher(Files.readString(latest));
        assertThat(matcher.find()).as("periodic_compaction_seconds in %s", latest).isTrue();
        return Long.parseUnsignedLong(matcher.group(1));
    }

    /**
     * Returns the value RocksDB itself records for a TTL database opened with the same base options as the
     * cache, without the periodic compaction setter ever being called.
     */
    private long rocksDbDefaultTtlPeriodicCompactionSeconds() throws Exception {
        final Path baselineDir = tempDir.resolve("baseline");
        Files.createDirectories(baselineDir);
        try (Options options = RocksDbUtil.createOptions();
                RocksDB db = TtlDB.open(options, baselineDir.toString(), 60, false)) {
            db.put(new byte[]{ 1 }, new byte[]{ 1 });
        }
        return persistedPeriodicCompactionSeconds(baselineDir);
    }

    private Path cacheDir() {
        return tempDir.resolve("reselect-cache");
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
    public void enablingTtlAcrossReopenDiscardsOldEntriesAsMisses() {
        // Without a TTL the database is opened plain; with a TTL it is opened as a TtlDB, whose on-disk
        // value format differs. Entries written in the other mode must degrade to misses, never fail.
        cache = open(config().build());
        cache.forRow(intKey(1)).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        cache.close();

        cache = open(config().with("reselect.cache.ttl.ms", 60_000).build());
        assertThat(cache.forRow(intKey(1)).get("data")).isEmpty();

        // The cache remains fully usable in the new mode.
        cache.forRow(intKey(1)).put("data", Schema.OPTIONAL_STRING_SCHEMA, "BBB");
        assertThat(cache.forRow(intKey(1)).get("data")).map(Hit::value).contains("BBB");
    }

    @Test
    public void disablingTtlAcrossReopenRemainsReadable() {
        cache = open(config().with("reselect.cache.ttl.ms", 60_000).build());
        cache.forRow(intKey(1)).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        cache.close();

        // TtlDB-written values carry a trailing timestamp the envelope decoder ignores, so entries stay
        // readable when the TTL is later disabled.
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
    @FixFor("debezium/dbz#2865")
    public void periodicCompactionSecondsIsAppliedWhenTtlEnabled() throws Exception {
        cache = open(config()
                .with("reselect.cache.ttl.ms", 60_000)
                .with(RocksDbReselectColumnCache.PERIODIC_COMPACTION_SECONDS, 3600)
                .build());
        cache.forRow(intKey(1)).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        cache.close();
        cache = null;

        assertThat(persistedPeriodicCompactionSeconds(cacheDir())).isEqualTo(3600L);
    }

    @Test
    @FixFor("debezium/dbz#2865")
    public void periodicCompactionSecondsIsAppliedWithoutTtl() throws Exception {
        cache = open(config().with(RocksDbReselectColumnCache.PERIODIC_COMPACTION_SECONDS, 7200).build());
        cache.forRow(intKey(1)).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        assertThat(cache.forRow(intKey(1)).get("data")).map(Hit::value).contains("AAA");
        cache.close();
        cache = null;

        assertThat(persistedPeriodicCompactionSeconds(cacheDir())).isEqualTo(7200L);
    }

    @Test
    @FixFor("debezium/dbz#2865")
    public void periodicCompactionDefaultLeavesRocksDbDefault() throws Exception {
        final long rocksDbDefault = rocksDbDefaultTtlPeriodicCompactionSeconds();

        cache = open(config().with("reselect.cache.ttl.ms", 60_000).build());
        cache.forRow(intKey(1)).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        cache.close();
        cache = null;

        // RocksDB enables 30-day periodic compaction by default for a TTL database. The reselect-columns
        // documentation quotes this value, so update it if a rocksdbjni upgrade changes the default.
        assertThat(rocksDbDefault).isEqualTo(2_592_000L);
        assertThat(persistedPeriodicCompactionSeconds(cacheDir())).isEqualTo(rocksDbDefault);
    }

    @Test
    @FixFor("debezium/dbz#2865")
    public void explicitZeroPeriodicCompactionSecondsLeavesRocksDbDefault() throws Exception {
        final long rocksDbDefault = rocksDbDefaultTtlPeriodicCompactionSeconds();

        // Unlike RocksDB's own periodic_compaction_seconds=0, the property's 0 does not disable periodic compaction
        cache = open(config()
                .with("reselect.cache.ttl.ms", 60_000)
                .with(RocksDbReselectColumnCache.PERIODIC_COMPACTION_SECONDS, 0)
                .build());
        cache.forRow(intKey(1)).put("data", Schema.OPTIONAL_STRING_SCHEMA, "AAA");
        cache.close();
        cache = null;

        assertThat(rocksDbDefault).isNotZero();
        assertThat(persistedPeriodicCompactionSeconds(cacheDir())).isEqualTo(rocksDbDefault);
    }

    @Test
    @FixFor("debezium/dbz#2865")
    public void negativePeriodicCompactionSecondsFails() {
        assertThatThrownBy(() -> open(config().with(RocksDbReselectColumnCache.PERIODIC_COMPACTION_SECONDS, -1).build()))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining(RocksDbReselectColumnCache.PERIODIC_COMPACTION_SECONDS);
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
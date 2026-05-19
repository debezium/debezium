/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.ydb.offset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.storage.ydb.YdbTestEnvironment;

/**
 * Integration test for {@link YdbOffsetBackingStore}. Boots YDB via the bundled
 * docker-compose file in {@code @BeforeAll}.
 */
class YdbOffsetBackingStoreIT {

    private static final String PROP_PREFIX = "offset.storage.ydb.";

    private String connectorName;
    private String tableName;

    @BeforeAll
    static void startYdb() throws InterruptedException {
        YdbTestEnvironment.start();
    }

    @AfterAll
    static void stopYdb() {
        YdbTestEnvironment.stop();
    }

    @BeforeEach
    public void setUp() {
        // Distinct connector name per test isolates rows; distinct table avoids contention across runs.
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        this.connectorName = "it-" + suffix;
        this.tableName = "debezium/dbz_offsets_it_" + suffix;
    }

    @Test
    @DisplayName("set() persists offsets and a fresh store load() returns them")
    public void setAndReloadOffsets() throws Exception {
        YdbOffsetBackingStore store = newStore();
        try {
            ByteBuffer key = buf("partition-a");
            ByteBuffer value = buf("offset-1");
            store.set(Collections.singletonMap(key, value), null).get(10, TimeUnit.SECONDS);

            Map<ByteBuffer, ByteBuffer> result = store.get(Collections.singletonList(key)).get(10, TimeUnit.SECONDS);
            assertEquals("offset-1", asString(result.get(key)));
        }
        finally {
            store.stop();
        }

        // New store instance must observe the previously persisted offset after start->load.
        YdbOffsetBackingStore reopened = newStore();
        try {
            ByteBuffer key = buf("partition-a");
            Map<ByteBuffer, ByteBuffer> result = reopened.get(Collections.singletonList(key)).get(10, TimeUnit.SECONDS);
            assertEquals("offset-1", asString(result.get(key)));
        }
        finally {
            reopened.stop();
        }
    }

    @Test
    @DisplayName("get() returns null for unknown keys without throwing")
    public void getReturnsNullForMissingKey() throws Exception {
        YdbOffsetBackingStore store = newStore();
        try {
            ByteBuffer key = buf("does-not-exist");
            Map<ByteBuffer, ByteBuffer> result = store.get(Collections.singletonList(key)).get(10, TimeUnit.SECONDS);
            assertNotNull(result);
            assertEquals(1, result.size());
            assertNull(result.get(key));
        }
        finally {
            store.stop();
        }
    }

    @Test
    @DisplayName("set() overwrites a previously stored offset for the same key")
    public void overwriteOffset() throws Exception {
        YdbOffsetBackingStore store = newStore();
        try {
            ByteBuffer key = buf("partition-b");
            store.set(Collections.singletonMap(key, buf("v1")), null).get(10, TimeUnit.SECONDS);
            store.set(Collections.singletonMap(key, buf("v2")), null).get(10, TimeUnit.SECONDS);

            Map<ByteBuffer, ByteBuffer> result = store.get(Collections.singletonList(key)).get(10, TimeUnit.SECONDS);
            assertEquals("v2", asString(result.get(key)));
        }
        finally {
            store.stop();
        }
    }

    private YdbOffsetBackingStore newStore() {
        YdbOffsetBackingStore store = new YdbOffsetBackingStore();
        store.configure(workerConfig());
        store.start();
        return store;
    }

    private WorkerConfig workerConfig() {
        Map<String, String> props = new HashMap<>();
        // Required StandaloneConfig fields.
        props.put("bootstrap.servers", "localhost:9092");
        props.put("offset.storage.file.filename", "/tmp/unused-" + connectorName + ".dat");
        props.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        // YDB-specific settings.
        props.put("endpoint", YdbTestEnvironment.ENDPOINT);
        props.put("database", YdbTestEnvironment.DATABASE);
        props.put(PROP_PREFIX + "connector.name", connectorName);
        props.put(PROP_PREFIX + "table.name", tableName);
        return new StandaloneConfig(props);
    }

    private static ByteBuffer buf(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String asString(ByteBuffer b) {
        if (b == null) {
            return null;
        }
        return StandardCharsets.UTF_8.decode(b.asReadOnlyBuffer()).toString();
    }
}
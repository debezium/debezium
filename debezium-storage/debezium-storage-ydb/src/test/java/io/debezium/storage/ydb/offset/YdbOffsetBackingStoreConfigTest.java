/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.ydb.offset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

/**
 * Unit tests for {@link YdbOffsetBackingStoreConfig}.
 */
public class YdbOffsetBackingStoreConfigTest {

    private static Configuration baseConfig() {
        Map<String, String> props = new HashMap<>();
        props.put("endpoint", "grpc://localhost:2136");
        props.put("database", "/local");
        props.put("offset.storage.ydb.connector.name", "test-connector");
        return Configuration.from(props);
    }

    @Test
    public void tableNameDefaultsWhenMissing() {
        YdbOffsetBackingStoreConfig cfg = new YdbOffsetBackingStoreConfig(baseConfig());
        assertEquals("debezium/dbz_offsets", cfg.getTableName());
        assertEquals("test-connector", cfg.getConnectorName());
        assertEquals("grpc://localhost:2136", cfg.getEndpoint());
        assertEquals("/local", cfg.getDatabase());
    }

    @Test
    public void tableNameOverrideIsRespected() {
        Map<String, String> props = new HashMap<>();
        props.put("endpoint", "grpc://localhost:2136");
        props.put("database", "/local");
        props.put("offset.storage.ydb.connector.name", "c1");
        props.put("offset.storage.ydb.table.name", "custom/offsets");
        YdbOffsetBackingStoreConfig cfg = new YdbOffsetBackingStoreConfig(Configuration.from(props));
        assertEquals("custom/offsets", cfg.getTableName());
    }

    @Test
    public void authIsOptional() {
        YdbOffsetBackingStoreConfig cfg = new YdbOffsetBackingStoreConfig(baseConfig());
        assertNull(cfg.getAuthUser());
        assertNull(cfg.getAuthPassword());
    }

    @Test
    public void authCredentialsArePassedThrough() {
        Map<String, String> props = new HashMap<>();
        props.put("endpoint", "grpc://localhost:2136");
        props.put("database", "/local");
        props.put("offset.storage.ydb.connector.name", "c1");
        props.put("auth.user", "alice");
        props.put("auth.password", "secret");
        YdbOffsetBackingStoreConfig cfg = new YdbOffsetBackingStoreConfig(Configuration.from(props));
        assertEquals("alice", cfg.getAuthUser());
        assertEquals("secret", cfg.getAuthPassword());
    }
}
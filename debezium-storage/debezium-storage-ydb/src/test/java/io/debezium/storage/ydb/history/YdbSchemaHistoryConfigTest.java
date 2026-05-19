/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.ydb.history;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

/**
 * Unit tests for {@link YdbSchemaHistoryConfig}.
 */
public class YdbSchemaHistoryConfigTest {

    private static Configuration baseConfig() {
        Map<String, String> props = new HashMap<>();
        props.put("endpoint", "grpc://localhost:2136");
        props.put("database", "/local");
        props.put("schema.history.internal.ydb.connector.name", "test-connector");
        return Configuration.from(props);
    }

    @Test
    public void tableNameDefaultsWhenMissing() {
        YdbSchemaHistoryConfig cfg = new YdbSchemaHistoryConfig(baseConfig());
        assertEquals("debezium/dbz_schema_history", cfg.getTableName());
        assertEquals("test-connector", cfg.getConnectorName());
        assertEquals("grpc://localhost:2136", cfg.getEndpoint());
        assertEquals("/local", cfg.getDatabase());
    }

    @Test
    public void tableNameOverrideIsRespected() {
        Map<String, String> props = new HashMap<>();
        props.put("endpoint", "grpc://localhost:2136");
        props.put("database", "/local");
        props.put("schema.history.internal.ydb.connector.name", "c1");
        props.put("schema.history.internal.ydb.table.name", "custom/history");
        YdbSchemaHistoryConfig cfg = new YdbSchemaHistoryConfig(Configuration.from(props));
        assertEquals("custom/history", cfg.getTableName());
    }

    @Test
    public void authIsOptional() {
        YdbSchemaHistoryConfig cfg = new YdbSchemaHistoryConfig(baseConfig());
        assertNull(cfg.getAuthUser());
        assertNull(cfg.getAuthPassword());
    }
}
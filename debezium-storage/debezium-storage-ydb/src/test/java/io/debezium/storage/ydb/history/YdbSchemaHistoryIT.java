/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.ydb.history;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.ydb.YdbTestEnvironment;

/**
 * Integration test for {@link YdbSchemaHistory}. Boots YDB via Testcontainers in
 * {@code @BeforeAll}.
 */
class YdbSchemaHistoryIT {

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
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        this.connectorName = "it-" + suffix;
        this.tableName = "debezium/dbz_schema_history_it_" + suffix;
    }

    @Test
    @DisplayName("exists() is false before any record is stored")
    public void existsIsFalseInitially() {
        YdbSchemaHistory history = newStarted();
        try {
            assertTrue(history.storageExists(), "storageExists() always returns true for YDB-backed history");
            assertFalse(history.exists(), "exists() should be false before any record is stored");
        }
        finally {
            history.stop();
        }
    }

    @Test
    @DisplayName("storeRecord() makes exists() true, and a fresh instance sees the record")
    public void storeAndSeeExistsAfterRestart() {
        Map<String, Object> source = Collections.singletonMap("server", "test");
        Map<String, Object> position = Collections.singletonMap("ts", System.currentTimeMillis());

        YdbSchemaHistory writer = newStarted();
        try {
            writer.record(source, position, "db", "schema", "CREATE TABLE t (id INT)", null, Instant.now());
            assertTrue(writer.exists(), "exists() should be true after a record is stored");
        }
        finally {
            writer.stop();
        }

        YdbSchemaHistory reader = newStarted();
        try {
            assertTrue(reader.exists(), "fresh instance should see the previously stored record");
        }
        finally {
            reader.stop();
        }
    }

    @Test
    @DisplayName("seq_no continues across restarts so the second write does not collide on PK")
    public void seqNoSurvivesRestart() {
        Map<String, Object> source = Collections.singletonMap("server", "test");
        Map<String, Object> position = Collections.singletonMap("ts", System.currentTimeMillis());

        YdbSchemaHistory first = newStarted();
        try {
            first.record(source, position, "db", "schema", "CREATE TABLE t1 (id INT)", null, Instant.now());
        }
        finally {
            first.stop();
        }

        YdbSchemaHistory second = newStarted();
        try {
            // If seq_no didn't carry over, this would collide on PK (connector_name, db_name, seq_no).
            second.record(source, position, "db", "schema", "CREATE TABLE t2 (id INT)", null, Instant.now());
            assertTrue(second.exists());
        }
        finally {
            second.stop();
        }
    }

    @Test
    @DisplayName("records for different databases use independent seq_no spaces (multi-task safe)")
    public void perDbSeqIsIndependent() {
        Map<String, Object> source = Collections.singletonMap("server", "test");
        Map<String, Object> position = Collections.singletonMap("ts", System.currentTimeMillis());

        YdbSchemaHistory history = newStarted();
        try {
            // Each (connector, db) has its own seq_no space; collisions across dbs are impossible.
            history.record(source, position, "db_a", "schema", "CREATE TABLE a1 (id INT)", null, Instant.now());
            history.record(source, position, "db_b", "schema", "CREATE TABLE b1 (id INT)", null, Instant.now());
            history.record(source, position, "db_a", "schema", "CREATE TABLE a2 (id INT)", null, Instant.now());
            history.record(source, position, "db_b", "schema", "CREATE TABLE b2 (id INT)", null, Instant.now());
            assertTrue(history.exists());
        }
        finally {
            history.stop();
        }

        YdbSchemaHistory reader = newStarted();
        try {
            assertTrue(reader.exists(), "fresh instance should see records for all databases");
        }
        finally {
            reader.stop();
        }
    }

    private YdbSchemaHistory newStarted() {
        YdbSchemaHistory history = new YdbSchemaHistory();
        history.configure(config(), HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, true);
        history.start();
        return history;
    }

    private Configuration config() {
        Map<String, String> props = new HashMap<>();
        props.put("endpoint", YdbTestEnvironment.ENDPOINT);
        props.put("database", YdbTestEnvironment.DATABASE);
        props.put("schema.history.internal.ydb.connector.name", connectorName);
        props.put("schema.history.internal.ydb.table.name", tableName);
        return Configuration.from(props);
    }
}
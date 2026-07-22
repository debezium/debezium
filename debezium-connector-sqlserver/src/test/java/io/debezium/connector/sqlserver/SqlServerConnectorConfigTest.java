/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Predicate;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

public class SqlServerConnectorConfigTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnectorConfigTest.class);

    @Test
    void nullDatabaseNames() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig().build());
        assertFalse(connectorConfig.validateAndRecord(SqlServerConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    void emptyDatabaseNames() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.DATABASE_NAMES, "")
                        .build());
        assertFalse(connectorConfig.validateAndRecord(SqlServerConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    void nonEmptyDatabaseNames() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.DATABASE_NAMES, "testDB1")
                        .build());
        assertTrue(connectorConfig.validateAndRecord(SqlServerConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    void hostnameAndDefaultPortConnectionUrl() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.HOSTNAME, "example.com")
                        .build());
        assertEquals(connectionUrl(connectorConfig), "jdbc:sqlserver://${hostname}:${port}");
    }

    @Test
    void hostnameAndPortConnectionUrl() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.HOSTNAME, "example.com")
                        .with(SqlServerConnectorConfig.PORT, "11433")
                        .build());
        assertEquals(connectionUrl(connectorConfig), "jdbc:sqlserver://${hostname}:${port}");
    }

    @Test
    void hostnameAndInstanceConnectionUrl() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.HOSTNAME, "example.com")
                        .with(SqlServerConnectorConfig.INSTANCE, "instance")
                        .build());
        assertEquals(connectionUrl(connectorConfig), "jdbc:sqlserver://${hostname}\\instance");
    }

    @Test
    void hostnameAndInstanceAndPortConnectionUrl() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.HOSTNAME, "example.com")
                        .with(SqlServerConnectorConfig.INSTANCE, "instance")
                        .with(SqlServerConnectorConfig.PORT, "11433")
                        .build());
        assertEquals(connectionUrl(connectorConfig), "jdbc:sqlserver://${hostname}\\instance:${port}");
    }

    @Test
    void validQueryFetchSizeDefaults() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                        .build());
        assertEquals(connectorConfig.getQueryFetchSize(), 10_000);
    }

    @Test
    void validQueryFetchSizeAvailable() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                        .with(SqlServerConnectorConfig.QUERY_FETCH_SIZE, 20_000)
                        .build());
        assertEquals(connectorConfig.getQueryFetchSize(), 20_000);
    }

    @Test
    void captureInstanceFilterAppliesIncludeAndExcludeLists() {
        // No list configured: every capture instance is accepted.
        assertTrue(captureInstanceFilter(null, null).test("anything"));

        // Include list: only matching instances are accepted.
        final Predicate<String> included = captureInstanceFilter("dbo_tablea,dbo_tableb", null);
        assertTrue(included.test("dbo_tablea"));
        assertTrue(included.test("dbo_tableb"));
        assertFalse(included.test("dbo_tablec"));

        // Exclude list: matching instances are rejected, everything else accepted.
        final Predicate<String> excluded = captureInstanceFilter(null, "dbo_tableb");
        assertFalse(excluded.test("dbo_tableb"));
        assertTrue(excluded.test("dbo_tablea"));
    }

    @Test
    void captureInstanceFilterMatchingIsAnchoredAndCaseInsensitive() {
        final Predicate<String> filter = captureInstanceFilter(null, "dbo_table.*");
        // Case-insensitive.
        assertFalse(filter.test("DBO_TableA"));
        // Anchored: a leading segment that is not part of the pattern must not match.
        assertTrue(filter.test("other_dbo_tablea"));
    }

    @Test
    void captureInstanceIncludeAndExcludeListsAreMutuallyExclusive() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.DATABASE_NAMES, "testDB1")
                        .with(SqlServerConnectorConfig.CAPTURE_INSTANCE_INCLUDE_LIST, "dbo_tablea")
                        .with(SqlServerConnectorConfig.CAPTURE_INSTANCE_EXCLUDE_LIST, "dbo_tableb")
                        .build());
        assertFalse(connectorConfig.validateAndRecord(SqlServerConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    private Predicate<String> captureInstanceFilter(String includeList, String excludeList) {
        final Configuration.Builder config = defaultConfig();
        if (includeList != null) {
            config.with(SqlServerConnectorConfig.CAPTURE_INSTANCE_INCLUDE_LIST, includeList);
        }
        if (excludeList != null) {
            config.with(SqlServerConnectorConfig.CAPTURE_INSTANCE_EXCLUDE_LIST, excludeList);
        }
        return new SqlServerConnectorConfig(config.build()).getCaptureInstanceFilter();
    }

    private Configuration.Builder defaultConfig() {
        return Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "server")
                .with(SqlServerConnectorConfig.HOSTNAME, "localhost")
                .with(SqlServerConnectorConfig.USER, "debezium")
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                .with(KafkaSchemaHistory.TOPIC, "history");
    }

    private String connectionUrl(SqlServerConnectorConfig connectorConfig) {
        SqlServerJdbcConfiguration jdbcConfig = connectorConfig.getJdbcConfig();
        return SqlServerConnection.createUrlPattern(jdbcConfig, false);
    }
}

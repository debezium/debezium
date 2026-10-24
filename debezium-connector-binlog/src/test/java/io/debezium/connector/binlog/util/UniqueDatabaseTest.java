/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.jdbc.JdbcConnection;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class UniqueDatabaseTest {

    private final String originalMetadataMode = System.getProperty(UniqueDatabase.BINLOG_METADATA_BASED_SCHEMA_PROPERTY);

    @AfterEach
    void restoreMetadataMode() {
        if (originalMetadataMode == null) {
            System.clearProperty(UniqueDatabase.BINLOG_METADATA_BASED_SCHEMA_PROPERTY);
        }
        else {
            System.setProperty(UniqueDatabase.BINLOG_METADATA_BASED_SCHEMA_PROPERTY, originalMetadataMode);
        }
    }

    @Test
    void shouldLeaveHistoryModeKeysUnchanged() {
        System.setProperty(UniqueDatabase.BINLOG_METADATA_BASED_SCHEMA_PROPERTY, "false");
        final var config = database().withBinlogMetadataKeyColumns("$DBNAME$\\.keyless:value").defaultConfig().build();
        assertThat(config.getString(BinlogConnectorConfig.MSG_KEY_COLUMNS)).isNull();
    }

    @Test
    void shouldScopeExplicitMetadataKeysToTheUniqueDatabase() {
        System.setProperty(UniqueDatabase.BINLOG_METADATA_BASED_SCHEMA_PROPERTY, "true");
        final var config = database().withBinlogMetadataKeyColumns("$DBNAME$\\.keyless:value").defaultConfig().build();
        assertThat(config.getBoolean(BinlogConnectorConfig.BINLOG_METADATA_BASED_SCHEMA)).isTrue();
        assertThat(config.getString(BinlogConnectorConfig.MSG_KEY_COLUMNS)).isEqualTo("fixture_unique\\.keyless:value");
    }

    @Test
    void shouldNotSupplyKeysUnlessTheFixtureRequestsThem() {
        System.setProperty(UniqueDatabase.BINLOG_METADATA_BASED_SCHEMA_PROPERTY, "true");
        assertThat(database().defaultConfig().build().getString(BinlogConnectorConfig.MSG_KEY_COLUMNS)).isNull();
    }

    @Test
    void shouldAllowTestsToOverrideFixtureKeys() {
        System.setProperty(UniqueDatabase.BINLOG_METADATA_BASED_SCHEMA_PROPERTY, "true");
        final var config = database().withBinlogMetadataKeyColumns("$DBNAME$\\.keyless:value").defaultConfig()
                .with(BinlogConnectorConfig.MSG_KEY_COLUMNS, "other.table:id").build();
        assertThat(config.getString(BinlogConnectorConfig.MSG_KEY_COLUMNS)).isEqualTo("other.table:id");
    }

    private UniqueDatabase database() {
        return new UniqueDatabase("server", "fixture", "unique", null) {
            @Override
            public ZoneId getTimezone() {
                return ZoneOffset.UTC;
            }

            @Override
            protected JdbcConnection forTestDatabase(String databaseName, Map<String, Object> urlProperties) {
                throw new AssertionError("Building test configuration must not open a database connection");
            }
        };
    }
}

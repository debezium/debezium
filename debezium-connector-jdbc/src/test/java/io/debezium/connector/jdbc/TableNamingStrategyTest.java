/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.naming.DefaultTableNamingStrategy;
import io.debezium.connector.jdbc.naming.TableNamingStrategy;
import io.debezium.connector.jdbc.util.DebeziumSinkRecordFactory;
import io.debezium.connector.jdbc.util.SinkRecordFactory;

/**
 * Tests for the {@link TableNamingStrategy} interface and implementations.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class TableNamingStrategyTest {
    @Test
    public void testDefaultTableNamingStrategy() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of());
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final DefaultTableNamingStrategy strategy = new DefaultTableNamingStrategy();
        assertThat(strategy.resolveTableName(config, factory.createRecord("database.schema.table"))).isEqualTo("database_schema_table");
    }

    @Test
    public void testTableNamingStrategyWithTableNameFormat() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of("table.name.format", "kafka_${topic}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final DefaultTableNamingStrategy strategy = new DefaultTableNamingStrategy();
        assertThat(strategy.resolveTableName(config, factory.createRecord("database.schema.table"))).isEqualTo("kafka_database_schema_table");
    }
}

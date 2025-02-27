/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.naming.DefaultTableNamingStrategy;
import io.debezium.connector.jdbc.util.DebeziumSinkRecordFactory;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;
import io.debezium.sink.naming.CollectionNamingStrategy;
import io.debezium.sink.naming.DefaultCollectionNamingStrategy;

/**
 * Tests for the {@link CollectionNamingStrategy} interface and implementations.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class CollectionNamingStrategyTest {

    private static final SinkRecordFactory RECORD_FACTORY = new DebeziumSinkRecordFactory();

    @Test
    public void testDefaultTableNamingStrategy() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of());
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        assertThat(strategy.resolveCollectionName(RECORD_FACTORY.createRecord("database.schema.table"), config.getCollectionNameFormat()))
                .isEqualTo("database_schema_table");
    }

    @Test
    public void testCollectionNamingStrategyWithTableNameFormat() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "kafka_${topic}"));
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        assertThat(strategy.resolveCollectionName(RECORD_FACTORY.createRecord("database.schema.table"), config.getCollectionNameFormat()))
                .isEqualTo("kafka_database_schema_table");
    }

    @Test
    public void testDeprecatedTableNamingStrategyWithTableNameFormat() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(JdbcSinkConnectorConfig.DEPRECATED_TABLE_NAME_FORMAT, "kafkadep_${topic}"));
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        assertThat(strategy.resolveCollectionName(RECORD_FACTORY.createRecord("database.schema.table"), config.getCollectionNameFormat()))
                .isEqualTo("kafkadep_database_schema_table");
    }

    @Test
    public void testDeprecatedDefaultTableNamingStrategyWithTableNameFormat() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(JdbcSinkConnectorConfig.DEPRECATED_TABLE_NAME_FORMAT, "kafkadep_${topic}"));
        final DefaultCollectionNamingStrategy strategy = new DefaultTableNamingStrategy();
        assertThat(strategy.resolveCollectionName(RECORD_FACTORY.createRecord("database.schema.table"), config.getCollectionNameFormat()))
                .isEqualTo("kafkadep_database_schema_table");
    }

    @Test
    @FixFor("DBZ-6491")
    public void testCollectionNamingStrategyWithPrependedSchema() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "SYS.${topic}"));
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        assertThat(strategy.resolveCollectionName(RECORD_FACTORY.createRecord("database.schema.table"), config.getCollectionNameFormat()))
                .isEqualTo("SYS.database_schema_table");
    }

    @Test
    @FixFor("DBZ-6491")
    public void testDeprecatedTableNamingStrategyWithPrependedSchema() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(JdbcSinkConnectorConfig.DEPRECATED_TABLE_NAME_FORMAT, "SYSDEP.${topic}"));
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        assertThat(strategy.resolveCollectionName(RECORD_FACTORY.createRecord("database.schema.table"), config.getCollectionNameFormat()))
                .isEqualTo("SYSDEP.database_schema_table");
    }

    @Test
    public void testDefaultCollectionNamingStrategyWithDebeziumSource() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(
                Map.of(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "source_${source.db}_${source.schema}_${source.table}"));
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        assertThat(strategy.resolveCollectionName(RECORD_FACTORY.createRecord("database.schema.table", (byte) 1, "database1", "schema1", "table1"),
                config.getCollectionNameFormat()))
                .isEqualTo("source_database1_schema1_table1");
    }

    @Test
    public void testDeprecatedDefaultTableNamingStrategyWithDebeziumSource() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(
                Map.of(JdbcSinkConnectorConfig.DEPRECATED_TABLE_NAME_FORMAT, "sourcedep_${source.db}_${source.schema}_${source.table}"));
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        assertThat(strategy.resolveCollectionName(RECORD_FACTORY.createRecord("database.schema.table", (byte) 1, "database1", "schema1", "table1"),
                config.getCollectionNameFormat()))
                .isEqualTo("sourcedep_database1_schema1_table1");
    }

    @Test
    public void testDefaultTableNamingStrategyWithInvalidSourceField() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "source_${source.invalid}"));
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        Assertions.assertThrows(DataException.class, () -> strategy
                .resolveCollectionName(RECORD_FACTORY.createRecord("database.schema.table", (byte) 1, "database1", "schema1", "table1"),
                        config.getCollectionNameFormat()));
    }

    @Test
    public void testDefaultTableNamingStrategyWithDebeziumSourceAndTombstone() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(
                Map.of(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "source_${source.db}_${source.schema}_${source.table}"));
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        assertThat(strategy.resolveCollectionName(RECORD_FACTORY.tombstoneRecord("database.schema.table"), config.getCollectionNameFormat())).isNull();
    }

    @Test
    public void testDefaultCollectionNamingStrategyWithTopicAndTombstone() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "kafka_${topic}"));
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        assertThat(strategy.resolveCollectionName(RECORD_FACTORY.tombstoneRecord("database.schema.table"), config.getCollectionNameFormat()))
                .isEqualTo("kafka_database_schema_table");
    }

    @Test
    public void testDeprecatedDefaultTableNamingStrategyWithTopicAndTombstone() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(JdbcSinkConnectorConfig.DEPRECATED_TABLE_NAME_FORMAT, "kafkadep_${topic}"));
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        assertThat(strategy.resolveCollectionName(RECORD_FACTORY.tombstoneRecord("database.schema.table"), config.getCollectionNameFormat()))
                .isEqualTo("kafkadep_database_schema_table");
    }

}

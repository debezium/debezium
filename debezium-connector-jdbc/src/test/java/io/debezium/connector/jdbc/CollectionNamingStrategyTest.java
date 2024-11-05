/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
    @Test
    public void testDefaultTableNamingStrategy() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of());
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        assertThat(strategy.resolveCollectionName(config, factory.createRecord("database.schema.table")))
                .isEqualTo("database_schema_table");
    }

    @Test
    public void testTableNamingStrategyWithTableNameFormat() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "kafka_${topic}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        assertThat(strategy.resolveCollectionName(config, factory.createRecord("database.schema.table")))
                .isEqualTo("kafka_database_schema_table");
    }

    @Test
    @FixFor("DBZ-6491")
    public void testTableNamingStrategyWithPrependedSchema() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "SYS.${topic}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        assertThat(strategy.resolveCollectionName(config, factory.createRecord("database.schema.table")))
                .isEqualTo("SYS.database_schema_table");
    }

    @Test
    public void testDefaultTableNamingStrategyWithDebeziumSource() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(
                Map.of(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "source_${source.db}_${source.schema}_${source.table}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        SinkRecord record = factory.createRecord("database.schema.table", (byte) 1, "database1", "schema1", "table1");
        assertThat(strategy.resolveCollectionName(config, record))
                .isEqualTo("source_database1_schema1_table1");
    }

    @Test
    public void testDefaultTableNamingStrategyWithInvalidSourceField() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "source_${source.invalid}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        SinkRecord record = factory.createRecord("database.schema.table", (byte) 1, "database1", "schema1", "table1");
        Assertions.assertThrows(DataException.class, () -> strategy.resolveCollectionName(config, record));
    }

    @Test
    public void testDefaultTableNamingStrategyWithDebeziumSourceAndTombstone() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(
                Map.of(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "source_${source.db}_${source.schema}_${source.table}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        SinkRecord record = factory.tombstoneRecord("database.schema.table");
        assertThat(strategy.resolveCollectionName(config, record)).isNull();
    }

    @Test
    public void testDefaultTableNamingStrategyWithTopicAndTombstone() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "kafka_${topic}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final DefaultCollectionNamingStrategy strategy = new DefaultCollectionNamingStrategy();
        SinkRecord record = factory.tombstoneRecord("database.schema.table");
        assertThat(strategy.resolveCollectionName(config, record)).isEqualTo("kafka_database_schema_table");
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.jdbc;

import org.jetbrains.annotations.NotNull;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.util.SinkRecordFactory;

class AbstractRecordBufferTest {

    protected DatabaseDialect dialect;

    protected @NotNull JdbcKafkaSinkRecord createRecord(KafkaDebeziumSinkRecord record, JdbcSinkConnectorConfig config) {
        return new JdbcKafkaSinkRecord(
                record.getOriginalKafkaRecord(),
                config.getPrimaryKeyMode(),
                config.getPrimaryKeyFields(),
                config.getFieldFilter(),
                config.cloudEventsSchemaNamePattern(),
                dialect);
    }

    protected @NotNull JdbcKafkaSinkRecord createRecordNoPkFields(SinkRecordFactory factory, byte i, JdbcSinkConnectorConfig config) {
        return createRecord(factory.createRecord("topic", i), config);
    }

    protected @NotNull JdbcKafkaSinkRecord createRecordPkFieldId(SinkRecordFactory factory, byte i, JdbcSinkConnectorConfig config) {
        return createRecord(factory.createRecord("topic", i), config);
    }

}

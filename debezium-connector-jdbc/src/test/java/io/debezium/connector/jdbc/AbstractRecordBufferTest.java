/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.jdbc;

import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.util.SinkRecordFactory;

class AbstractRecordBufferTest {

    protected DatabaseDialect dialect;

    protected @NotNull JdbcKafkaSinkRecord createRecord(SinkRecord kafkaRecord, JdbcSinkConnectorConfig config) {
        return new JdbcKafkaSinkRecord(
                kafkaRecord,
                config.getPrimaryKeyMode(),
                config.getPrimaryKeyFields(),
                config.getFieldFilter(),
                dialect);
    }

    protected @NotNull JdbcKafkaSinkRecord createRecordNoPkFields(SinkRecordFactory factory, byte i, JdbcSinkConnectorConfig config) {
        return createRecord(factory.createRecord("topic", i), config);
    }

    protected @NotNull JdbcKafkaSinkRecord createRecordPkFieldId(SinkRecordFactory factory, byte i, JdbcSinkConnectorConfig config) {
        return createRecord(factory.createRecord("topic", i), config);
    }

}

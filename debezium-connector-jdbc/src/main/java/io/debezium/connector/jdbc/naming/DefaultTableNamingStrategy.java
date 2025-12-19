/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.naming;

import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.sink.naming.DefaultCollectionNamingStrategy;

/**
 * Default implementation of the {@link TableNamingStrategy} where the table name is driven
 * directly from the topic name, replacing any {@code dot} characters with {@code underscore}
 * and source field in topic.
 *
 * @author Chris Cranford
 */
@Deprecated
public class DefaultTableNamingStrategy extends DefaultCollectionNamingStrategy implements TableNamingStrategy {

    @Override
    public String resolveTableName(JdbcSinkConnectorConfig config, SinkRecord record) {
        return super.resolveCollectionName(new KafkaDebeziumSinkRecord(record, config.cloudEventsSchemaNamePattern()), config.getCollectionNameFormat());
    }

}

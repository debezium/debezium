/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.naming;

import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;

/**
 * Default implementation of the {@link TableNamingStrategy} where the table name is driven
 * directly from the topic name, replacing any {@code dot} characters with {@code underscore}.
 *
 * @author Chris Cranford
 */
public class DefaultTableNamingStrategy implements TableNamingStrategy {
    @Override
    public String resolveTableName(JdbcSinkConnectorConfig config, SinkRecord record) {
        // Default behavior is to replace dots with underscores
        final String topicName = record.topic().replace(".", "_");
        return config.getTableNameFormat().replace("${topic}", topicName);
    }
}

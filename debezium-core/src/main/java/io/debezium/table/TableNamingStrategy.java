/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.table;

import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.connector.SinkConnectorConfig;

/**
 * A pluggable strategy contract for defining how table names are resolved from kafka records.
 *
 * @author Chris Cranford
 */
public interface TableNamingStrategy {
    /**
     * Resolves the logical table name from the sink record.
     *
     * @param config sink connector configuration, should not be {@code null}
     * @param record Kafka sink record, should not be {@code null}
     * @return the resolved logical table name; if {@code null} the record should not be processed
     */
    String resolveTableName(SinkConnectorConfig config, SinkRecord record);
}

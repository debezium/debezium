/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.naming;

import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.sink.SinkConnectorConfig;

/**
 * A pluggable strategy contract for defining how table names are resolved from kafka records.
 *
 * @author Chris Cranford
 */
public interface CollectionNamingStrategy {
    /**
     * Resolves the logical table name from the sink record.
     *
     * @param config the sink connector configuration defining table name format
     * @param record Debezium sink record, should not be {@code null}
     * @return the resolved logical table name; if {@code null} the record should not be processed
     */
    String resolveCollectionName(SinkConnectorConfig config, SinkRecord record);
}

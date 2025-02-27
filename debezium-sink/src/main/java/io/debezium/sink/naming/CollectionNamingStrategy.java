/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.naming;

import io.debezium.sink.DebeziumSinkRecord;

/**
 * A pluggable strategy contract for defining how table names are resolved from kafka records.
 *
 * @author Chris Cranford
 */
public interface CollectionNamingStrategy {

    /**
     * Resolves the logical collection name from the Debezium sink record.
     *
     * @param record Debezium sink record, should not be {@code null}
     * @param collectionNameFormat the format string for the collection name (mapped from the topic name)
     * @return the resolved logical collection name; if {@code null} the record should not be processed
     */
    String resolveCollectionName(DebeziumSinkRecord record, String collectionNameFormat);

}

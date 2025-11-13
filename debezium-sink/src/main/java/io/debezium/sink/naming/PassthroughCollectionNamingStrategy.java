/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.naming;

import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.util.Strings;

/**
 * A simple passthrough implementation that uses the topic name, without any changes, as the target
 * collection name for writing changes by a Debezium sink.
 *
 * @author Chris Cranford
 */
public class PassthroughCollectionNamingStrategy implements CollectionNamingStrategy {
    @Override
    public String resolveCollectionName(DebeziumSinkRecord record, String collectionNameFormat) {
        if (!Strings.isNullOrBlank(collectionNameFormat)) {
            return collectionNameFormat.replace("${topic}", record.topicName());
        }
        return record.topicName();
    }
}

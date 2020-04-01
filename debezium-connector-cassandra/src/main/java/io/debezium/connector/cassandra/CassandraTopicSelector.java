/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import io.debezium.schema.TopicSelector;

/**
 * Responsible for selecting the Kafka topic that the record will get send to.
 */
public class CassandraTopicSelector {
    public static TopicSelector<KeyspaceTable> defaultSelector(String prefix, String heartbeatPrefix) {
        return TopicSelector.defaultSelector(prefix, heartbeatPrefix, ".",
                (keyspaceTable, pref, delimiter) -> String.join(delimiter, pref, keyspaceTable.keyspace, keyspaceTable.table));
    }
}

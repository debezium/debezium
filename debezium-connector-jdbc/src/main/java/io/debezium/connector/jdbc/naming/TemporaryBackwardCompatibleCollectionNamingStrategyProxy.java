/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.naming;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.naming.CollectionNamingStrategy;

/**
 * This proxy is only a temporary wrapper for the {@link CollectionNamingStrategy} in order to make TableNamingStrategy backward compatible.
 * It will be removed in the Debezium 3.1 release!
 *
 * @author rk3rn3r
 */
@Deprecated
public class TemporaryBackwardCompatibleCollectionNamingStrategyProxy implements CollectionNamingStrategy {

    private final JdbcSinkConnectorConfig config;
    private final CollectionNamingStrategy originalStrategy;

    public TemporaryBackwardCompatibleCollectionNamingStrategyProxy(CollectionNamingStrategy strategy, JdbcSinkConnectorConfig config) {
        this.config = config;
        this.originalStrategy = strategy;
    }

    /**
     * Wrapper code to resolve the collection name from the original implementation.
     */
    public String resolveCollectionName(DebeziumSinkRecord record, String collectionNameFormat) {
        if (originalStrategy instanceof TableNamingStrategy) {
            if (record instanceof KafkaDebeziumSinkRecord) {
                return ((TableNamingStrategy) originalStrategy).resolveTableName(config, ((KafkaDebeziumSinkRecord) record).getOriginalKafkaRecord());
            }
            throw new IllegalArgumentException("Unsupported Debezium sink record type: " + record.getClass());
        }
        else {
            return originalStrategy.resolveCollectionName(record, collectionNameFormat);
        }
    }

    public CollectionNamingStrategy getOriginalStrategy() {
        return originalStrategy;
    }
}

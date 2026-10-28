/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import io.debezium.DebeziumException;
import io.debezium.common.annotation.Incubating;
import io.debezium.connector.oracle.OracleConnectorConfig;

/**
 * SPI to customize the LogMiner query. Implementations are discovered with {@link ServiceLoader},
 * so registering one in {@code META-INF/services} of a jar on the connector classpath is enough
 * to activate it. Without any implementation, the default query is used.
 *
 * <p>The returned query must keep the column order expected by {@link LogMinerColumnIndexes}
 * and the same bind parameters as the default query.
 */
@Incubating
public interface LogMinerQueryBuilderProvider {

    /**
     * @param connectorConfig the connector configuration
     * @param defaultQueryBuilder the builder Debezium would use otherwise, either to delegate to or to ignore
     * @return the query builder to use
     */
    LogMinerQueryBuilder create(OracleConnectorConfig connectorConfig, LogMinerQueryBuilder defaultQueryBuilder);

    static LogMinerQueryBuilder resolve(OracleConnectorConfig connectorConfig, LogMinerQueryBuilder defaultQueryBuilder) {
        final List<LogMinerQueryBuilderProvider> providers = StreamSupport
                .stream(ServiceLoader.load(LogMinerQueryBuilderProvider.class).spliterator(), false)
                .collect(Collectors.toList());
        if (providers.isEmpty()) {
            return defaultQueryBuilder;
        }
        if (providers.size() > 1) {
            throw new DebeziumException("Found multiple LogMinerQueryBuilderProvider implementations: " + providers);
        }
        return providers.get(0).create(connectorConfig, defaultQueryBuilder);
    }
}

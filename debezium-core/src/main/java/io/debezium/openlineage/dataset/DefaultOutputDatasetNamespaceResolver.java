/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import java.util.function.Function;

import io.debezium.config.Configuration;

public class DefaultOutputDatasetNamespaceResolver implements OutputDatasetNamespaceResolver {

    private static final String LIST_SEPARATOR = ",";
    private static final String KAFKA_NAMESPACE_FORMAT = "kafka://%s";
    private static final String SCHEMA_HISTORY_INTERNAL_KAFKA_BOOTSTRAP_SERVERS_PROPERTY = "schema.history.internal.kafka.bootstrap.servers";

    @Override
    public String resolve(Configuration configuration) {

        String hostPort = configuration.getList(SCHEMA_HISTORY_INTERNAL_KAFKA_BOOTSTRAP_SERVERS_PROPERTY, LIST_SEPARATOR, Function.identity())
                .stream().findFirst()
                .orElse("unknown:unknown");

        return String.format(KAFKA_NAMESPACE_FORMAT, hostPort);
    }
}

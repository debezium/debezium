/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import io.debezium.common.annotation.Incubating;

/**
 * {@link DebeziumSourceConnector} represents source of change data capture (CDC) for given resource.
 * CDC implementation itself is done in {@link DebeziumSourceTask}, which is self-contained unit of work
 * created and managed by {@link DebeziumSourceConnector}. {@link DebeziumSourceConnector} implementations
 * may create one or multiple tasks, based on the capabilities of the CDC resource.
 *
 * As {@link io.debezium.engine.DebeziumEngine} currently supports only one connector per its instance,
 * the connector configuration is hold in engine itself.
 *
 * @author vjuranek
 */
@Incubating
public interface DebeziumSourceConnector {

    /**
     * Returns the {@link DebeziumSourceConnectorContext} for this DebeziumSourceConnector.
     * @return the DebeziumSourceConnectorContext for this connector
     */
    DebeziumSourceConnectorContext context();

    /**
     * Initialize the connector with its {@link DebeziumSourceConnectorContext} context.
     * @param context {@link DebeziumSourceConnectorContext} containing references to auxiliary objects.
     */
    void initialize(DebeziumSourceConnectorContext context);
}

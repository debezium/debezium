/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import java.util.List;
import java.util.Map;

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

    /**
     * Start the connector with the given configuration.
     * @param config connector configuration
     */
    void start(Map<String, String> config);

    /**
     * Stop the connector.
     */
    void stop();

    /**
     * Returns the task configurations for the requested number of tasks.
     * @param maxTasks maximum number of task configurations to generate
     * @return list of task configurations
     */
    List<Map<String, String>> taskConfigs(int maxTasks);

    /**
     * Creates and initializes a new {@link DebeziumSourceTask} with the given context.
     * @param context task context containing configuration and offset management objects
     * @return a new initialized source task
     * @throws Exception if the task cannot be created or initialized
     */
    DebeziumSourceTask createTask(DebeziumSourceTaskContext context) throws Exception;
}

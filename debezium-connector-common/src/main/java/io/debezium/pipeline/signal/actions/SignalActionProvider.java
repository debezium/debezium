/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.actions;

import java.util.Map;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * This interface is used to provide custom signal actions:
 * Implementations must:
 *
 * provide a map of signal action in the {@link #createActions(EventDispatcher, ChangeEventSourceCoordinator, CommonConnectorConfig)} method.
 *
 * @author Mario Fiore Vitale
 */
public interface SignalActionProvider {

    /**
     * Create a map of signal action where the key is the name of the action.
     *
     * @param dispatcher the event dispatcher instance
     * @param connectorConfig the connector config
     * @return a concrete action
     */

    <P extends Partition> Map<String, SignalAction<P>> createActions(EventDispatcher<P, ? extends DataCollectionId> dispatcher,
                                                                     ChangeEventSourceCoordinator<P, ?> changeEventSourceCoordinator,
                                                                     CommonConnectorConfig connectorConfig);
}

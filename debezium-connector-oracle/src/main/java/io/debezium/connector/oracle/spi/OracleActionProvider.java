/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.spi;

import java.util.Map;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.actions.SignalAction;
import io.debezium.pipeline.signal.actions.SignalActionProvider;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Provides Oracle-specific signal actions.
 *
 * @author Debezium Community
 */
public class OracleActionProvider implements SignalActionProvider {

    @Override
    public <P extends Partition> Map<String, SignalAction<P>> createActions(
                                                                            EventDispatcher<P, ? extends DataCollectionId> dispatcher,
                                                                            ChangeEventSourceCoordinator<P, ?> changeEventSourceCoordinator,
                                                                            CommonConnectorConfig connectorConfig) {
        return Map.of(DropTransactionAction.NAME, new DropTransactionAction<>(changeEventSourceCoordinator));
    }
}

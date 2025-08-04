/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.signal;

import java.util.HashMap;
import java.util.Map;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.actions.SignalAction;
import io.debezium.pipeline.signal.actions.SignalActionProvider;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Provider for MySQL-specific signal actions.
 *
 * @author Debezium Authors
 */
public class MySqlSignalActionProvider implements SignalActionProvider {

    @Override
    public <P extends Partition> Map<String, SignalAction<P>> createActions(
                                                                            EventDispatcher<P, ? extends DataCollectionId> dispatcher,
                                                                            ChangeEventSourceCoordinator<P, ?> changeEventSourceCoordinator,
                                                                            CommonConnectorConfig connectorConfig) {

        Map<String, SignalAction<P>> actions = new HashMap<>();

        // Add MySQL-specific signal actions
        if (connectorConfig instanceof MySqlConnectorConfig) {
            actions.put(SetBinlogPositionSignal.NAME,
                    new SetBinlogPositionSignal<>(dispatcher, changeEventSourceCoordinator,
                            (MySqlConnectorConfig) connectorConfig));
        }

        return actions;
    }
}
/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.spi;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.SignalAction;
import io.debezium.pipeline.signal.actions.SignalActionProvider;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

public class CustomActionProvider implements SignalActionProvider {

    public CustomActionProvider() {
    }

    @Override
    public <P extends Partition> Map<String, SignalAction<P>> createActions(EventDispatcher<P, ? extends DataCollectionId> dispatcher,
                                                                            CommonConnectorConfig connectorConfig) {
        return Map.of("customLog", new CustomAction());
    }

    public class CustomAction implements SignalAction {

        public CustomAction() {
        }

        private final Logger LOGGER = LoggerFactory.getLogger(CustomAction.class);

        @Override
        public boolean arrived(SignalPayload signalPayload) throws InterruptedException {

            LOGGER.info("[CustomLog] " + signalPayload.toString());
            return true;
        }
    }
}

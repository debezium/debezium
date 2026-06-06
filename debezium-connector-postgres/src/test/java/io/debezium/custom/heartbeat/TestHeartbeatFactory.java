/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.custom.heartbeat;

import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.heartbeat.DebeziumHeartbeatFactory;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.HeartbeatConnectionProvider;
import io.debezium.heartbeat.HeartbeatErrorHandler;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.spi.OffsetContext;

public class TestHeartbeatFactory implements DebeziumHeartbeatFactory {

    private final Logger logger = LoggerFactory.getLogger(TestHeartbeatFactory.class);

    @Override
    public Optional<Heartbeat> getHeartbeat(CommonConnectorConfig connectorConfig,
                                            HeartbeatConnectionProvider connectionProvider,
                                            HeartbeatErrorHandler errorHandler,
                                            ChangeEventQueue<DataChangeEvent> queue) {
        return Optional.of(new Heartbeat() {

            @Override
            public void emit(Map<String, ?> partition, OffsetContext offset) {
                logger.info("emitting heartbeat");
            }

            @Override
            public boolean isEnabled() {
                return true;
            }
        });
    }

}

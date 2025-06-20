/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0,
 * available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.util.Clock;

/**
 * Coordinates the lifecycle of CockroachDB streaming changefeed and dispatches change events.
 *
 * This class is used internally by the {@link CockroachDBConnectorTask} to wire together
 * the connector configuration, schema, dispatcher, and changefeed polling logic.
 *
 * @author Virag Tripathi
 */
public class CockroachDBChangefeedCoordinator extends ChangeEventSourceCoordinator<CockroachDBPartition, CockroachDBOffsetContext> {

    public CockroachDBChangefeedCoordinator(
                                            CockroachDBConnectorConfig connectorConfig,
                                            CockroachDBOffsetContext offsetContext,
                                            SourceTaskContext context,
                                            Clock clock) {

        super(
                new CockroachDBChangefeedSourceFactory(connectorConfig),
                new CockroachDBEventDispatcher(
                        connectorConfig,
                        connectorConfig.getTopicNamingStrategy(connectorConfig.getLogicalName(), true),
                        new CockroachDBSchema(
                                connectorConfig,
                                new CockroachDBValueConverter(connectorConfig),
                                null,
                                SchemaNameAdjuster.create(null)),
                        new ChangeEventQueue.Builder<DataChangeEvent>()
                                .pollInterval(connectorConfig.getPollInterval())
                                .maxQueueSize(connectorConfig.getMaxQueueSize())
                                .maxBatchSize(connectorConfig.getMaxBatchSize())
                                .loggingContextSupplier(() -> "CockroachDB")
                                .build(),
                        dataCollectionId -> true,
                        (id, op, key, value, offset, headers) -> {
                        }, // No-op change event creator
                        (partition, id) -> {
                        }, // No-op inconsistent schema handler
                        offset -> new CockroachDBSourceInfoStructMaker(connectorConfig).sourceInfo(offset),
                        null, // No heartbeat
                        SchemaNameAdjuster.create(null),
                        (c, o) -> {
                        } // No signal processor
                ),
                context,
                offsetContext,
                clock);
    }
}

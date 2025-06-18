/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import java.util.Map;

import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.util.Clock;

/**
 * The Kafka Connect task for the CockroachDB connector. This class runs inside a Kafka Connect worker and
 * coordinates streaming and snapshot event sources using Debezium's {@link ChangeEventSourceCoordinator}.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnectorTask extends SourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBConnectorTask.class);

    private ChangeEventSourceCoordinator<CockroachDBPartition, CockroachDBOffsetContext> coordinator;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("Starting CockroachDB connector task");

        final Configuration config = Configuration.from(props);
        final CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        final SchemaNameAdjuster nameAdjuster = SchemaNameAdjuster.create(LOGGER);
        final CockroachDBOffsetContext offsetContext = new CockroachDBOffsetContext(connectorConfig);

        final ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .loggingContextSupplier(() -> getClass().getSimpleName())
                .build();

        final CockroachDBSchema schema = new CockroachDBSchema(
                connectorConfig,
                new CockroachDBValueConverter(connectorConfig),
                null, // JDBC connection is not needed for enriched changefeed parsing
                nameAdjuster);

        final CockroachDBSourceInfoStructMaker structMaker = new CockroachDBSourceInfoStructMaker(connectorConfig);
        final CockroachDBEventDispatcher dispatcher = new CockroachDBEventDispatcher(
                connectorConfig,
                connectorConfig.getTopicNamingStrategy(connectorConfig.getLogicalName(), true),
                schema,
                queue,
                (id) -> true,
                (id, op, key, value, offset, headers) -> {
                }, // changeEventCreator — not used for JSON polling
                (partition, id) -> {
                }, // no inconsistent schema handler
                (offsetContext) -> structMaker.sourceInfo((CockroachDBOffsetContext) offsetContext),
                null,
                nameAdjuster,
                (c, o) -> {
                } // no signal processor yet
        );

        final ChangeEventSourceFactory<CockroachDBPartition, CockroachDBOffsetContext> sourceFactory = new CockroachDBChangefeedSourceFactory(connectorConfig);

        this.coordinator = new ChangeEventSourceCoordinator<>(
                sourceFactory,
                dispatcher,
                context,
                offsetContext,
                Clock.system());

        coordinator.start();
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping CockroachDB connector task");
        if (coordinator != null) {
            coordinator.stop();
        }
    }

    @Override
    public void commit() {
        if (coordinator != null) {
            coordinator.commit();
        }
    }
}

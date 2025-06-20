/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0,
 * available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.EventMetadataProvider;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * A specialized {@link EventDispatcher} that handles CockroachDB-specific dispatching
 * of change records into the change event queue.
 *
 * This class coordinates the translation of changefeed messages into Kafka {@link org.apache.kafka.connect.source.SourceRecord}
 * events using the schema and mapping logic from {@link CockroachDBSchema}.
 *
 * @author Virag Tripathi
 */
public class CockroachDBEventDispatcher extends EventDispatcher<CockroachDBPartition, DataCollectionId> {

    public CockroachDBEventDispatcher(
                                      CockroachDBConnectorConfig connectorConfig,
                                      TopicNamingStrategy<DataCollectionId> topicNamingStrategy,
                                      CockroachDBSchema schema,
                                      ChangeEventQueue<DataChangeEvent> queue,
                                      DataCollectionFilter<DataCollectionId> filter,
                                      ChangeRecordEmitter.ChangeEventCreator<DataCollectionId> changeEventCreator,
                                      InconsistentSchemaHandler<CockroachDBPartition, DataCollectionId> inconsistentSchemaHandler,
                                      EventMetadataProvider metadataProvider,
                                      Heartbeat heartbeat,
                                      SchemaNameAdjuster nameAdjuster,
                                      SignalProcessor<CockroachDBPartition, CockroachDBOffsetContext> signalProcessor) {

        super(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                filter,
                changeEventCreator,
                inconsistentSchemaHandler,
                metadataProvider,
                heartbeat,
                nameAdjuster,
                signalProcessor);
    }
}

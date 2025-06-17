/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.postgresql.connection.LogicalDecodingMessage;
import io.debezium.connector.postgresql.pipeline.txmetadata.PostgresTransactionMonitor;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Custom extension of the {@link EventDispatcher} to accommodate routing {@link LogicalDecodingMessage} events to the change event queue.
 *
 * @author Lairen Hightower
 */
public class PostgresEventDispatcher<T extends DataCollectionId> extends EventDispatcher<PostgresPartition, T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresEventDispatcher.class);
    private final ChangeEventQueue<DataChangeEvent> queue;
    private final LogicalDecodingMessageMonitor logicalDecodingMessageMonitor;
    private final LogicalDecodingMessageFilter messageFilter;

    public PostgresEventDispatcher(PostgresConnectorConfig connectorConfig, TopicNamingStrategy<T> topicNamingStrategy,
                                   DatabaseSchema<T> schema, ChangeEventQueue<DataChangeEvent> queue, DataCollectionFilters.DataCollectionFilter<T> filter,
                                   ChangeEventCreator changeEventCreator, InconsistentSchemaHandler<PostgresPartition, T> inconsistentSchemaHandler,
                                   EventMetadataProvider metadataProvider, Heartbeat heartbeat, SchemaNameAdjuster schemaNameAdjuster,
                                   SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor, DebeziumHeaderProducer debeziumHeaderProducer) {
        super(connectorConfig, topicNamingStrategy, schema, queue, filter, changeEventCreator, inconsistentSchemaHandler, heartbeat, schemaNameAdjuster,
                new PostgresTransactionMonitor(
                        connectorConfig,
                        metadataProvider,
                        schemaNameAdjuster,
                        (record) -> {
                            queue.enqueue(new DataChangeEvent(record));
                        },
                        topicNamingStrategy.transactionTopic()),
                signalProcessor,
                debeziumHeaderProducer);
        this.queue = queue;
        this.logicalDecodingMessageMonitor = new LogicalDecodingMessageMonitor(connectorConfig, this::enqueueLogicalDecodingMessage);
        this.messageFilter = connectorConfig.getMessageFilter();
    }

    public void dispatchLogicalDecodingMessage(Partition partition, OffsetContext offset, Long decodeTimestamp,
                                               LogicalDecodingMessage message)
            throws InterruptedException {
        if (messageFilter.isIncluded(message.getPrefix())) {
            logicalDecodingMessageMonitor.logicalDecodingMessageEvent(partition, offset, decodeTimestamp, message, transactionMonitor);
        }
        else {
            LOGGER.trace("Filtered data change event for logical decoding message with prefix{}", message.getPrefix());
        }
    }

    private void enqueueLogicalDecodingMessage(SourceRecord record) throws InterruptedException {
        queue.enqueue(new DataChangeEvent(record));
    }
}

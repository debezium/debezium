/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.ChangeRecordEmitter.Receiver;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.schema.DataCollectionFilters.DataCollectionFilter;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.HistorizedDatabaseSchema;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;

/**
 * Central dispatcher for data change and schema change events. The former will be routed to the change event queue, the
 * latter will be routed to the {@link DatabaseSchema}. But based on the applying whitelist/blacklist configuration,
 * events may be not be dispatched at all.
 * <p>
 * This router is also in charge of emitting heartbeat messages, exposing of metrics via JMX etc.
 *
 * @author Gunnar Morling
 */
public class EventDispatcher<T extends DataCollectionId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventDispatcher.class);

    private final TopicSelector<T> topicSelector;
    private final DatabaseSchema<T> schema;
    private final HistorizedDatabaseSchema<T> historizedSchema;
    private final ChangeEventQueue<DataChangeEvent> queue;
    private final DataCollectionFilter<T> filter;
    private final ChangeEventCreator changeEventCreator;
    private final Heartbeat heartbeat;
    private DataChangeEventListener eventListener = DataChangeEventListener.NO_OP;
    private final boolean emitTombstonesOnDelete;
    private final InconsistentSchemaHandler<T> inconsistentSchemaHandler;
    private final TransactionMonitor transactionMonitor;
    private final CommonConnectorConfig connectorConfig;

    /**
     * Change event receiver for events dispatched from a streaming change event source.
     */
    private final StreamingChangeRecordReceiver streamingReceiver;

    public EventDispatcher(CommonConnectorConfig connectorConfig, TopicSelector<T> topicSelector,
                           DatabaseSchema<T> schema, ChangeEventQueue<DataChangeEvent> queue, DataCollectionFilter<T> filter,
                           ChangeEventCreator changeEventCreator, EventMetadataProvider metadataProvider) {
        this(connectorConfig, topicSelector, schema, queue, filter, changeEventCreator, null, metadataProvider, null);
    }

    public EventDispatcher(CommonConnectorConfig connectorConfig, TopicSelector<T> topicSelector,
                           DatabaseSchema<T> schema, ChangeEventQueue<DataChangeEvent> queue, DataCollectionFilter<T> filter,
                           ChangeEventCreator changeEventCreator, EventMetadataProvider metadataProvider,
                           Heartbeat heartbeat) {
        this(connectorConfig, topicSelector, schema, queue, filter, changeEventCreator, null, metadataProvider, heartbeat);
    }

    public EventDispatcher(CommonConnectorConfig connectorConfig, TopicSelector<T> topicSelector,
                           DatabaseSchema<T> schema, ChangeEventQueue<DataChangeEvent> queue, DataCollectionFilter<T> filter,
                           ChangeEventCreator changeEventCreator, InconsistentSchemaHandler<T> inconsistentSchemaHandler,
                           EventMetadataProvider metadataProvider, Heartbeat customHeartbeat) {
        this.connectorConfig = connectorConfig;
        this.topicSelector = topicSelector;
        this.schema = schema;
        this.historizedSchema = schema instanceof HistorizedDatabaseSchema
                ? (HistorizedDatabaseSchema<T>) schema
                : null;
        this.queue = queue;
        this.filter = filter;
        this.changeEventCreator = changeEventCreator;
        this.streamingReceiver = new StreamingChangeRecordReceiver();
        this.emitTombstonesOnDelete = connectorConfig.isEmitTombstoneOnDelete();
        this.inconsistentSchemaHandler = inconsistentSchemaHandler != null ? inconsistentSchemaHandler : this::errorOnMissingSchema;

        this.transactionMonitor = new TransactionMonitor(connectorConfig, metadataProvider, this::dispatchTransactionMessage);
        if (customHeartbeat != null) {
            heartbeat = customHeartbeat;
        }
        else {
            heartbeat = Heartbeat.create(connectorConfig.getConfig(), topicSelector.getHeartbeatTopic(),
                    connectorConfig.getLogicalName());
        }

    }

    public void dispatchSnapshotEvent(T dataCollectionId, ChangeRecordEmitter changeRecordEmitter, SnapshotReceiver receiver) throws InterruptedException {
        // TODO Handle Heartbeat

        DataCollectionSchema dataCollectionSchema = schema.schemaFor(dataCollectionId);

        // TODO handle as per inconsistent schema info option
        if (dataCollectionSchema == null) {
            errorOnMissingSchema(dataCollectionId, changeRecordEmitter);
        }

        changeRecordEmitter.emitChangeRecords(dataCollectionSchema, new Receiver() {

            @Override
            public void changeRecord(DataCollectionSchema schema,
                                     Operation operation,
                                     Object key, Struct value,
                                     OffsetContext offset,
                                     ConnectHeaders headers)
                    throws InterruptedException {
                eventListener.onEvent(dataCollectionSchema.id(), offset, key, value);
                receiver.changeRecord(dataCollectionSchema, operation, key, value, offset, headers);
            }
        });
    }

    public SnapshotReceiver getSnapshotChangeEventReceiver() {
        return new BufferingSnapshotChangeRecordReceiver();
    }

    /**
     * Dispatches one or more {@link DataChangeEvent}s. If the given data collection is included in the currently
     * captured set of collections, the given emitter will be invoked, so it can emit one or more events (in the common
     * case, one event will be emitted, but e.g. in case of PK updates, it may be a deletion and a creation event). The
     * receiving coordinator creates {@link SourceRecord}s for all emitted events and passes them to this dispatcher's
     * {@link ChangeEventCreator} for converting them into data change events.
     *
     * @return {@code true} if an event was dispatched (i.e. sent to the message broker), {@code false} otherwise.
     */
    public boolean dispatchDataChangeEvent(T dataCollectionId, ChangeRecordEmitter changeRecordEmitter) throws InterruptedException {
        try {
            boolean handled = false;
            if (!filter.isIncluded(dataCollectionId)) {
                LOGGER.trace("Filtered data change event for {}", dataCollectionId);
                eventListener.onFilteredEvent("source = " + dataCollectionId);
            }
            else {
                DataCollectionSchema dataCollectionSchema = schema.schemaFor(dataCollectionId);

                // TODO handle as per inconsistent schema info option
                if (dataCollectionSchema == null) {
                    final Optional<DataCollectionSchema> replacementSchema = inconsistentSchemaHandler.handle(dataCollectionId, changeRecordEmitter);
                    if (!replacementSchema.isPresent()) {
                        return false;
                    }
                    dataCollectionSchema = replacementSchema.get();
                }

                changeRecordEmitter.emitChangeRecords(dataCollectionSchema, new Receiver() {

                    @Override
                    public void changeRecord(DataCollectionSchema schema,
                                             Operation operation,
                                             Object key, Struct value,
                                             OffsetContext offset,
                                             ConnectHeaders headers)
                            throws InterruptedException {
                        transactionMonitor.dataEvent(dataCollectionId, offset, key, value);
                        eventListener.onEvent(dataCollectionId, offset, key, value);
                        streamingReceiver.changeRecord(schema, operation, key, value, offset, headers);
                    }
                });
                handled = true;
            }

            heartbeat.heartbeat(
                    changeRecordEmitter.getOffset().getPartition(),
                    changeRecordEmitter.getOffset().getOffset(),
                    this::enqueueHeartbeat);

            return handled;
        }
        catch (Exception e) {
            switch (connectorConfig.getEventProcessingFailureHandlingMode()) {
                case FAIL:
                    throw new ConnectException("Error while processing event at offset " + changeRecordEmitter.getOffset().getOffset(), e);
                case WARN:
                    LOGGER.warn(
                            "Error while processing event at offset {}",
                            changeRecordEmitter.getOffset().getOffset());
                    break;
                case SKIP:
                    LOGGER.debug(
                            "Error while processing event at offset {}",
                            changeRecordEmitter.getOffset().getOffset());
                    break;
            }
            return false;
        }
    }

    public void dispatchTransactionCommittedEvent(OffsetContext offset) throws InterruptedException {
        transactionMonitor.transactionComittedEvent(offset);
    }

    public void dispatchTransactionStartedEvent(String transactionId, OffsetContext offset) throws InterruptedException {
        transactionMonitor.transactionStartedEvent(transactionId, offset);
    }

    public Optional<DataCollectionSchema> errorOnMissingSchema(T dataCollectionId, ChangeRecordEmitter changeRecordEmitter) {
        eventListener.onErroneousEvent("source = " + dataCollectionId);
        throw new IllegalArgumentException("No metadata registered for captured table " + dataCollectionId);
    }

    public Optional<DataCollectionSchema> ignoreMissingSchema(T dataCollectionId, ChangeRecordEmitter changeRecordEmitter) {
        return Optional.empty();
    }

    public void dispatchSchemaChangeEvent(T dataCollectionId, SchemaChangeEventEmitter schemaChangeEventEmitter) throws InterruptedException {
        if (!filter.isIncluded(dataCollectionId)) {
            LOGGER.trace("Filtering schema change event for {}", dataCollectionId);
            return;
        }

        schemaChangeEventEmitter.emitSchemaChangeEvent(new SchemaChangeEventReceiver());
    }

    public void alwaysDispatchHeartbeatEvent(OffsetContext offset) throws InterruptedException {
        heartbeat.forcedBeat(
                offset.getPartition(),
                offset.getOffset(),
                this::enqueueHeartbeat);
    }

    public void dispatchHeartbeatEvent(OffsetContext offset) throws InterruptedException {
        heartbeat.heartbeat(
                offset.getPartition(),
                offset.getOffset(),
                this::enqueueHeartbeat);
    }

    public boolean heartbeatsEnabled() {
        return heartbeat.isEnabled();
    }

    private void enqueueHeartbeat(SourceRecord record) throws InterruptedException {
        queue.enqueue(new DataChangeEvent(record));
    }

    public void dispatchTransactionMessage(SourceRecord record) throws InterruptedException {
        queue.enqueue(new DataChangeEvent(record));
    }

    /**
     * Change record receiver used during snapshotting. Allows for a deferred submission of records, which is needed in
     * order to set the "snapshot completed" offset field, which we can't send to Kafka Connect without sending an
     * actual record
     */
    public interface SnapshotReceiver extends ChangeRecordEmitter.Receiver {
        void completeSnapshot() throws InterruptedException;
    }

    private final class StreamingChangeRecordReceiver implements ChangeRecordEmitter.Receiver {

        @Override
        public void changeRecord(DataCollectionSchema dataCollectionSchema,
                                 Operation operation,
                                 Object key, Struct value,
                                 OffsetContext offsetContext,
                                 ConnectHeaders headers)
                throws InterruptedException {

            Objects.requireNonNull(value, "value must not be null");

            LOGGER.trace("Received change record for {} operation on key {}", operation, key);

            Schema keySchema = dataCollectionSchema.keySchema();
            String topicName = topicSelector.topicNameFor((T) dataCollectionSchema.id());

            SourceRecord record = new SourceRecord(offsetContext.getPartition(),
                    offsetContext.getOffset(), topicName, null,
                    keySchema, key,
                    dataCollectionSchema.getEnvelopeSchema().schema(),
                    value,
                    null,
                    headers);

            queue.enqueue(changeEventCreator.createDataChangeEvent(record));

            if (emitTombstonesOnDelete && operation == Operation.DELETE) {
                SourceRecord tombStone = record.newRecord(
                        record.topic(),
                        record.kafkaPartition(),
                        record.keySchema(),
                        record.key(),
                        null, // value schema
                        null, // value
                        record.timestamp(),
                        record.headers());

                queue.enqueue(changeEventCreator.createDataChangeEvent(tombStone));
            }
        }
    }

    private final class BufferingSnapshotChangeRecordReceiver implements SnapshotReceiver {

        private Supplier<DataChangeEvent> bufferedEvent;

        @Override
        public void changeRecord(DataCollectionSchema dataCollectionSchema,
                                 Operation operation,
                                 Object key, Struct value,
                                 OffsetContext offsetContext,
                                 ConnectHeaders headers)
                throws InterruptedException {
            Objects.requireNonNull(value, "value must not be null");

            LOGGER.trace("Received change record for {} operation on key {}", operation, key);

            if (bufferedEvent != null) {
                queue.enqueue(bufferedEvent.get());
            }

            Schema keySchema = dataCollectionSchema.keySchema();
            String topicName = topicSelector.topicNameFor((T) dataCollectionSchema.id());

            // the record is produced lazily, so to have the correct offset as per the pre/post completion callbacks
            bufferedEvent = () -> {
                SourceRecord record = new SourceRecord(
                        offsetContext.getPartition(),
                        offsetContext.getOffset(),
                        topicName, null,
                        keySchema, key,
                        dataCollectionSchema.getEnvelopeSchema().schema(), value,
                        null, headers);
                return changeEventCreator.createDataChangeEvent(record);
            };
        }

        @Override
        public void completeSnapshot() throws InterruptedException {
            if (bufferedEvent != null) {
                // It is possible that the last snapshotted table was empty
                // this way we ensure that the last event is always marked as last
                // even if it originates form non-last table
                final DataChangeEvent event = bufferedEvent.get();
                final Struct envelope = (Struct) event.getRecord().value();
                if (envelope.schema().field(Envelope.FieldName.SOURCE) != null) {
                    final Struct source = envelope.getStruct(Envelope.FieldName.SOURCE);
                    final SnapshotRecord snapshot = SnapshotRecord.fromSource(source);
                    if (snapshot == SnapshotRecord.TRUE) {
                        SnapshotRecord.LAST.toSource(source);
                    }
                }
                queue.enqueue(event);
                bufferedEvent = null;
            }
        }
    }

    private final class SchemaChangeEventReceiver implements SchemaChangeEventEmitter.Receiver {

        @Override
        public void schemaChangeEvent(SchemaChangeEvent event) throws InterruptedException {
            historizedSchema.applySchemaChange(event);
        }
    }

    /**
     * Provide a listener that is invoked for every incoming event to be processed.
     *
     * @param eventListener
     */
    public void setEventListener(DataChangeEventListener eventListener) {
        this.eventListener = eventListener;
    }

    /**
     * Reaction to an incoming change event for which schema is not found
     */
    @FunctionalInterface
    public static interface InconsistentSchemaHandler<T extends DataCollectionId> {

        /**
         * @return collection schema if the schema was updated and event can be processed, {@code empty} to skip the processing
         */
        Optional<DataCollectionSchema> handle(T dataCollectionId, ChangeRecordEmitter changeRecordEmitter);
    }

}

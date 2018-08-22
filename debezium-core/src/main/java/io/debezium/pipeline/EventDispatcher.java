/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.util.Objects;
import java.util.function.Supplier;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.data.Envelope.Operation;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
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

    /**
     * Change event receiver for events dispatched from a streaming change event source.
     */
    private final StreamingChangeRecordReceiver streamingReceiver;

    public EventDispatcher(CommonConnectorConfig connectorConfig, TopicSelector<T> topicSelector,
            DatabaseSchema<T> schema, ChangeEventQueue<DataChangeEvent> queue, DataCollectionFilter<T> filter,
            ChangeEventCreator changeEventCreator) {
        this.topicSelector = topicSelector;
        this.schema = schema;
        this.historizedSchema = schema instanceof HistorizedDatabaseSchema
                ? (HistorizedDatabaseSchema<T>) schema
                : null;
        this.queue = queue;
        this.filter = filter;
        this.changeEventCreator = changeEventCreator;
        this.streamingReceiver = new StreamingChangeRecordReceiver();

        heartbeat = Heartbeat.create(connectorConfig.getConfig(), topicSelector.getHeartbeatTopic(),
                connectorConfig.getLogicalName());
    }

    // TODO One could argue that snapshot events shouldn't have to go through the dispatcher but rather to the queue
    // directly; It's done though in order to handle heartbeat, JMX and other things consistently with streaming, so
    // it might be beneficial eventually
    public void dispatchSnapshotEvent(T dataCollectionId, ChangeRecordEmitter changeRecordEmitter, SnapshotReceiver receiver) throws InterruptedException {
        // TODO Handle Heartbeat

        // TODO Handle JMX

        DataCollectionSchema dataCollectionSchema = schema.schemaFor(dataCollectionId);

        // TODO handle as per inconsistent schema info option
        if(dataCollectionSchema == null) {
            throw new IllegalArgumentException("No metadata registered for captured table " + dataCollectionId);
        }

        changeRecordEmitter.emitChangeRecords(dataCollectionSchema, receiver);
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
     */
    public void dispatchDataChangeEvent(T dataCollectionId, ChangeRecordEmitter changeRecordEmitter) throws InterruptedException {
        // TODO Handle JMX

        if(!filter.isIncluded(dataCollectionId)) {
            LOGGER.trace("Skipping data change event for {}", dataCollectionId);
        }
        else {
            DataCollectionSchema dataCollectionSchema = schema.schemaFor(dataCollectionId);

            // TODO handle as per inconsistent schema info option
            if(dataCollectionSchema == null) {
                throw new IllegalArgumentException("No metadata registered for captured table " + dataCollectionId);
            }

            changeRecordEmitter.emitChangeRecords(dataCollectionSchema, streamingReceiver);
        }

        heartbeat.heartbeat(
                changeRecordEmitter.getOffset().getPartition(),
                changeRecordEmitter.getOffset().getOffset(),
                this::enqueueHeartbeat
        );
    }

    public void dispatchSchemaChangeEvent(T dataCollectionId, SchemaChangeEventEmitter schemaChangeEventEmitter) throws InterruptedException {
        if(!filter.isIncluded(dataCollectionId)) {
            LOGGER.trace("Skipping data change event for {}", dataCollectionId);
            return;
        }

        schemaChangeEventEmitter.emitSchemaChangeEvent(new SchemaChangeEventReceiver());
    }

    public void dispatchHeartbeatEvent(OffsetContext offset) throws InterruptedException {
        heartbeat.forcedBeat(
                offset.getPartition(),
                offset.getOffset(),
                this::enqueueHeartbeat
        );
    }

    private void enqueueHeartbeat(SourceRecord record) throws InterruptedException {
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
        public void changeRecord(DataCollectionSchema dataCollectionSchema, Operation operation, Object key, Struct value, OffsetContext offsetContext) throws InterruptedException {
            Objects.requireNonNull(key, "key must not be null");
            Objects.requireNonNull(value, "key must not be null");

            LOGGER.trace( "Received change record for {} operation on key {}", operation, key);

            Schema keySchema = dataCollectionSchema.keySchema();
            String topicName = topicSelector.topicNameFor((T) dataCollectionSchema.id());

            SourceRecord record = new SourceRecord(offsetContext.getPartition(), offsetContext.getOffset(),
                    topicName, null, keySchema, key, dataCollectionSchema.getEnvelopeSchema().schema(), value);

            queue.enqueue(changeEventCreator.createDataChangeEvent(record));

            // TODO handle option
            boolean emitTombstonesOnDelete = true;

            if (emitTombstonesOnDelete && operation == Operation.DELETE) {
                SourceRecord tombStone = record.newRecord(
                        record.topic(),
                        record.kafkaPartition(),
                        record.keySchema(),
                        record.key(),
                        null, // value schema
                        null, // value
                        record.timestamp()
                );

                queue.enqueue(changeEventCreator.createDataChangeEvent(tombStone));
            }
        }
    }

    private final class BufferingSnapshotChangeRecordReceiver implements SnapshotReceiver {

        private Supplier<DataChangeEvent> bufferedEvent;

        @Override
        public void changeRecord(DataCollectionSchema dataCollectionSchema, Operation operation, Object key, Struct value, OffsetContext offsetContext) throws InterruptedException {
            Objects.requireNonNull(key, "key must not be null");
            Objects.requireNonNull(value, "key must not be null");

            LOGGER.trace( "Received change record for {} operation on key {}", operation, key);

            if(bufferedEvent != null) {
                queue.enqueue(bufferedEvent.get());
            }

            Schema keySchema = dataCollectionSchema.keySchema();
            String topicName = topicSelector.topicNameFor((T) dataCollectionSchema.id());

            // the record is produced lazily, so to have the correct offset as per the pre/post completion callbacks
            bufferedEvent = () -> {
                SourceRecord record = new SourceRecord(offsetContext.getPartition(), offsetContext.getOffset(),
                        topicName, null, keySchema, key, dataCollectionSchema.getEnvelopeSchema().schema(), value);

                return changeEventCreator.createDataChangeEvent(record);
            };
        }

        @Override
        public void completeSnapshot() throws InterruptedException {
            if(bufferedEvent != null) {
                queue.enqueue(bufferedEvent.get());
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
}

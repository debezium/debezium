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

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.data.Envelope.Operation;
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
    private final ChangeEventQueue<Object> queue;
    private final DataCollectionFilter<T> filter;

    public EventDispatcher(TopicSelector<T> topicSelector, DatabaseSchema<T> schema,
            ChangeEventQueue<Object> queue,
            DataCollectionFilter<T> filter) {
        this.topicSelector = topicSelector;
        this.schema = schema;
        this.historizedSchema = schema instanceof HistorizedDatabaseSchema
                ? (HistorizedDatabaseSchema<T>) schema
                : null;
        this.queue = queue;
        this.filter = filter;
    }

    /**
     * Dispatches one or more {@link DataChangeEvent}s. If the given data collection is included in the currently
     * captured set of collections, the given emitter will be invoked, so it can emit one or more events (in the common
     * case, one event will be emitted, but e.g. in case of PK updates, it may be a deletion and a creation event). The
     * receiving coordinator creates {@link SourceRecord}s for all emitted events and passes them to the given
     * {@link ChangeEventCreator} for converting them into data change events.
     */
    public void dispatchDataChangeEvent(T dataCollectionId, Supplier<ChangeRecordEmitter> changeRecordEmitter, ChangeEventCreator changeEventCreator) throws InterruptedException {
        // TODO Handle Heartbeat

        // TODO Handle JMX

        if(!filter.isIncluded(dataCollectionId)) {
            LOGGER.trace("Skipping data change event for {}", dataCollectionId);
            return;
        }

        DataCollectionSchema dataCollectionSchema = schema.schemaFor(dataCollectionId);

        // TODO handle as per inconsistent schema info option
        if(dataCollectionSchema == null) {
            throw new IllegalArgumentException("No metadata registered for captured table " + dataCollectionId);
        }

        changeRecordEmitter.get().emitChangeRecords(
            dataCollectionSchema,
            new ChangeRecordReceiver(dataCollectionId, changeEventCreator, dataCollectionSchema)
        );
    }

    public void dispatchSchemaChangeEvent(T dataCollectionId, Supplier<SchemaChangeEventEmitter> schemaChangeEventEmitter) throws InterruptedException {
        if(!filter.isIncluded(dataCollectionId)) {
            LOGGER.trace("Skipping data change event for {}", dataCollectionId);
            return;
        }

        schemaChangeEventEmitter.get().emitSchemaChangeEvent(new SchemaChangeEventReceiver());
    }

    private final class ChangeRecordReceiver implements ChangeRecordEmitter.Receiver {

        private final T dataCollectionId;
        private final ChangeEventCreator changeEventCreator;
        private final DataCollectionSchema dataCollectionSchema;

        private ChangeRecordReceiver(T dataCollectionId, ChangeEventCreator changeEventCreator,
                DataCollectionSchema dataCollectionSchema) {
            this.dataCollectionId = dataCollectionId;
            this.changeEventCreator = changeEventCreator;
            this.dataCollectionSchema = dataCollectionSchema;
        }

        @Override
        public void changeRecord(Operation operation, Object key, Struct value, OffsetContext offsetContext) throws InterruptedException {
            Objects.requireNonNull(key, "key must not be null");
            Objects.requireNonNull(value, "key must not be null");

            LOGGER.trace( "Received change record for {} operation on key {}", operation, key);

            Schema keySchema = dataCollectionSchema.keySchema();
            String topicName = topicSelector.topicNameFor(dataCollectionId);

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

    private final class SchemaChangeEventReceiver implements SchemaChangeEventEmitter.Receiver {

        @Override
        public void schemaChangeEvent(SchemaChangeEvent event) throws InterruptedException {
            historizedSchema.applySchemaChange(event);
        }
    }
}

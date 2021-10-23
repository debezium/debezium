/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.signal.Signal;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.ChangeRecordEmitter.Receiver;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.relational.history.ConnectTableChangeSerializer;
import io.debezium.relational.history.HistoryRecord.Fields;
import io.debezium.relational.history.TableChanges.TableChangesSerializer;
import io.debezium.schema.DataCollectionFilters.DataCollectionFilter;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.HistorizedDatabaseSchema;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Central dispatcher for data change and schema change events. The former will be routed to the change event queue, the
 * latter will be routed to the {@link DatabaseSchema}. But based on the applying include/exclude list configuration,
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
    private final EnumSet<Operation> skippedOperations;
    private final boolean neverSkip;

    private final Schema schemaChangeKeySchema;
    private final Schema schemaChangeValueSchema;
    private final TableChangesSerializer<List<Struct>> tableChangesSerializer = new ConnectTableChangeSerializer();
    private final Signal signal;
    private IncrementalSnapshotChangeEventSource<T> incrementalSnapshotChangeEventSource;

    /**
     * Change event receiver for events dispatched from a streaming change event source.
     */
    private final StreamingChangeRecordReceiver streamingReceiver;

    public EventDispatcher(CommonConnectorConfig connectorConfig, TopicSelector<T> topicSelector,
                           DatabaseSchema<T> schema, ChangeEventQueue<DataChangeEvent> queue, DataCollectionFilter<T> filter,
                           ChangeEventCreator changeEventCreator, EventMetadataProvider metadataProvider, SchemaNameAdjuster schemaNameAdjuster) {
        this(connectorConfig, topicSelector, schema, queue, filter, changeEventCreator, null, metadataProvider,
                null, schemaNameAdjuster, null);
    }

    public EventDispatcher(CommonConnectorConfig connectorConfig, TopicSelector<T> topicSelector,
                           DatabaseSchema<T> schema, ChangeEventQueue<DataChangeEvent> queue, DataCollectionFilter<T> filter,
                           ChangeEventCreator changeEventCreator, EventMetadataProvider metadataProvider,
                           Heartbeat heartbeat, SchemaNameAdjuster schemaNameAdjuster) {
        this(connectorConfig, topicSelector, schema, queue, filter, changeEventCreator, null, metadataProvider,
                heartbeat, schemaNameAdjuster, null);
    }

    public EventDispatcher(CommonConnectorConfig connectorConfig, TopicSelector<T> topicSelector,
                           DatabaseSchema<T> schema, ChangeEventQueue<DataChangeEvent> queue, DataCollectionFilter<T> filter,
                           ChangeEventCreator changeEventCreator, InconsistentSchemaHandler<T> inconsistentSchemaHandler,
                           EventMetadataProvider metadataProvider, Heartbeat customHeartbeat, SchemaNameAdjuster schemaNameAdjuster,
                           JdbcConnection jdbcConnection) {
        this.connectorConfig = connectorConfig;
        this.topicSelector = topicSelector;
        this.schema = schema;
        this.historizedSchema = schema.isHistorized() ? (HistorizedDatabaseSchema<T>) schema : null;
        this.queue = queue;
        this.filter = filter;
        this.changeEventCreator = changeEventCreator;
        this.streamingReceiver = new StreamingChangeRecordReceiver();
        this.emitTombstonesOnDelete = connectorConfig.isEmitTombstoneOnDelete();
        this.inconsistentSchemaHandler = inconsistentSchemaHandler != null ? inconsistentSchemaHandler : this::errorOnMissingSchema;
        this.skippedOperations = connectorConfig.getSkippedOperations();
        this.neverSkip = connectorConfig.supportsOperationFiltering() || this.skippedOperations.isEmpty();

        this.transactionMonitor = new TransactionMonitor(connectorConfig, metadataProvider, this::enqueueTransactionMessage);
        this.signal = new Signal(connectorConfig, this);
        if (customHeartbeat != null) {
            heartbeat = customHeartbeat;
        }
        else {
            Configuration configuration = connectorConfig.getConfig();
            heartbeat = Heartbeat.create(
                    configuration.getDuration(Heartbeat.HEARTBEAT_INTERVAL, ChronoUnit.MILLIS),
                    topicSelector.getHeartbeatTopic(),
                    connectorConfig.getLogicalName());
        }

        schemaChangeKeySchema = SchemaBuilder.struct()
                .name(schemaNameAdjuster.adjust("io.debezium.connector." + connectorConfig.getConnectorName() + ".SchemaChangeKey"))
                .field(Fields.DATABASE_NAME, Schema.STRING_SCHEMA)
                .build();
        schemaChangeValueSchema = SchemaBuilder.struct()
                .name(schemaNameAdjuster.adjust("io.debezium.connector." + connectorConfig.getConnectorName() + ".SchemaChangeValue"))
                .field(Fields.SOURCE, connectorConfig.getSourceInfoStructMaker().schema())
                .field(Fields.DATABASE_NAME, Schema.OPTIONAL_STRING_SCHEMA)
                .field(Fields.SCHEMA_NAME, Schema.OPTIONAL_STRING_SCHEMA)
                .field(Fields.DDL_STATEMENTS, Schema.OPTIONAL_STRING_SCHEMA)
                .field(Fields.TABLE_CHANGES, SchemaBuilder.array(ConnectTableChangeSerializer.CHANGE_SCHEMA).build())
                .build();
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
            public void changeRecord(Partition partition,
                                     DataCollectionSchema schema,
                                     Operation operation,
                                     Object key, Struct value,
                                     OffsetContext offset,
                                     ConnectHeaders headers)
                    throws InterruptedException {
                eventListener.onEvent(dataCollectionSchema.id(), offset, key, value);
                receiver.changeRecord(partition, dataCollectionSchema, operation, key, value, offset, headers);
            }
        });
    }

    public SnapshotReceiver getSnapshotChangeEventReceiver() {
        return new BufferingSnapshotChangeRecordReceiver();
    }

    public SnapshotReceiver getIncrementalSnapshotChangeEventReceiver(DataChangeEventListener dataListener) {
        return new IncrementalSnapshotChangeRecordReceiver(dataListener);
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
                dispatchFilteredEvent(changeRecordEmitter.getPartition(), changeRecordEmitter.getOffset());
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
                    public void changeRecord(Partition partition,
                                             DataCollectionSchema schema,
                                             Operation operation,
                                             Object key, Struct value,
                                             OffsetContext offset,
                                             ConnectHeaders headers)
                            throws InterruptedException {
                        if (operation == Operation.CREATE && signal.isSignal(dataCollectionId)) {
                            signal.process(partition, value, offset);
                        }

                        if (neverSkip || !skippedOperations.contains(operation)) {
                            transactionMonitor.dataEvent(partition, dataCollectionId, offset, key, value);
                            eventListener.onEvent(dataCollectionId, offset, key, value);
                            if (incrementalSnapshotChangeEventSource != null) {
                                incrementalSnapshotChangeEventSource.processMessage(partition, dataCollectionId, key, offset);
                            }
                            streamingReceiver.changeRecord(partition, schema, operation, key, value, offset, headers);
                        }
                    }
                });
                handled = true;
            }

            heartbeat.heartbeat(
                    changeRecordEmitter.getPartition().getSourcePartition(),
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

    public void dispatchFilteredEvent(Partition partition, OffsetContext offset) throws InterruptedException {
        if (incrementalSnapshotChangeEventSource != null) {
            incrementalSnapshotChangeEventSource.processFilteredEvent(partition, offset);
        }
    }

    public void dispatchTransactionCommittedEvent(Partition partition, OffsetContext offset) throws InterruptedException {
        transactionMonitor.transactionComittedEvent(partition, offset);
        if (incrementalSnapshotChangeEventSource != null) {
            incrementalSnapshotChangeEventSource.processTransactionCommittedEvent(partition, offset);
        }
    }

    public void dispatchTransactionStartedEvent(Partition partition, String transactionId, OffsetContext offset) throws InterruptedException {
        transactionMonitor.transactionStartedEvent(partition, transactionId, offset);
        if (incrementalSnapshotChangeEventSource != null) {
            incrementalSnapshotChangeEventSource.processTransactionStartedEvent(partition, offset);
        }
    }

    public void dispatchConnectorEvent(ConnectorEvent event) {
        eventListener.onConnectorEvent(event);
    }

    public Optional<DataCollectionSchema> errorOnMissingSchema(T dataCollectionId, ChangeRecordEmitter changeRecordEmitter) {
        eventListener.onErroneousEvent("source = " + dataCollectionId);
        throw new IllegalArgumentException("No metadata registered for captured table " + dataCollectionId);
    }

    public Optional<DataCollectionSchema> ignoreMissingSchema(T dataCollectionId, ChangeRecordEmitter changeRecordEmitter) {
        return Optional.empty();
    }

    public void dispatchSchemaChangeEvent(T dataCollectionId, SchemaChangeEventEmitter schemaChangeEventEmitter) throws InterruptedException {
        if (dataCollectionId != null && !filter.isIncluded(dataCollectionId)) {
            if (historizedSchema == null || historizedSchema.storeOnlyCapturedTables()) {
                LOGGER.trace("Filtering schema change event for {}", dataCollectionId);
                return;
            }
        }
        schemaChangeEventEmitter.emitSchemaChangeEvent(new SchemaChangeEventReceiver());
    }

    public void dispatchSchemaChangeEvent(Collection<T> dataCollectionIds, SchemaChangeEventEmitter schemaChangeEventEmitter) throws InterruptedException {
        boolean anyNonfilteredEvent = false;
        if (dataCollectionIds == null || dataCollectionIds.isEmpty()) {
            anyNonfilteredEvent = true;
        }
        else {
            for (T dataCollectionId : dataCollectionIds) {
                if (filter.isIncluded(dataCollectionId)) {
                    anyNonfilteredEvent = true;
                    break;
                }
            }
        }
        if (!anyNonfilteredEvent) {
            if (historizedSchema == null || historizedSchema.storeOnlyCapturedTables()) {
                LOGGER.trace("Filtering schema change event for {}", dataCollectionIds);
                return;
            }
        }

        schemaChangeEventEmitter.emitSchemaChangeEvent(new SchemaChangeEventReceiver());
    }

    public void alwaysDispatchHeartbeatEvent(Partition partition, OffsetContext offset) throws InterruptedException {
        heartbeat.forcedBeat(
                partition.getSourcePartition(),
                offset.getOffset(),
                this::enqueueHeartbeat);
    }

    public void dispatchHeartbeatEvent(Partition partition, OffsetContext offset) throws InterruptedException {
        heartbeat.heartbeat(
                partition.getSourcePartition(),
                offset.getOffset(),
                this::enqueueHeartbeat);
    }

    public boolean heartbeatsEnabled() {
        return heartbeat.isEnabled();
    }

    private void enqueueHeartbeat(SourceRecord record) throws InterruptedException {
        queue.enqueue(new DataChangeEvent(record));
    }

    private void enqueueTransactionMessage(SourceRecord record) throws InterruptedException {
        queue.enqueue(new DataChangeEvent(record));
    }

    private void enqueueSchemaChangeMessage(SourceRecord record) throws InterruptedException {
        queue.enqueue(new DataChangeEvent(record));
    }

    public void dispatchServerHeartbeatEvent(Partition partition, OffsetContext offset) throws InterruptedException {
        if (incrementalSnapshotChangeEventSource != null) {
            incrementalSnapshotChangeEventSource.processHeartbeat(partition, offset);
        }
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
        public void changeRecord(Partition partition,
                                 DataCollectionSchema dataCollectionSchema,
                                 Operation operation,
                                 Object key, Struct value,
                                 OffsetContext offsetContext,
                                 ConnectHeaders headers)
                throws InterruptedException {

            Objects.requireNonNull(value, "value must not be null");

            LOGGER.trace("Received change record for {} operation on key {}", operation, key);

            // Truncate events must have null key schema as they are sent to table topics without keys
            Schema keySchema = (key == null && operation == Operation.TRUNCATE) ? null
                    : dataCollectionSchema.keySchema();
            String topicName = topicSelector.topicNameFor((T) dataCollectionSchema.id());

            SourceRecord record = new SourceRecord(partition.getSourcePartition(),
                    offsetContext.getOffset(),
                    topicName, null,
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
        public void changeRecord(Partition partition,
                                 DataCollectionSchema dataCollectionSchema,
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
                        partition.getSourcePartition(),
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

    private final class IncrementalSnapshotChangeRecordReceiver implements SnapshotReceiver {

        public final DataChangeEventListener dataListener;

        public IncrementalSnapshotChangeRecordReceiver(DataChangeEventListener dataListener) {
            this.dataListener = dataListener;
        }

        @Override
        public void changeRecord(Partition partition,
                                 DataCollectionSchema dataCollectionSchema,
                                 Operation operation,
                                 Object key, Struct value,
                                 OffsetContext offsetContext,
                                 ConnectHeaders headers)
                throws InterruptedException {
            Objects.requireNonNull(value, "value must not be null");

            LOGGER.trace("Received change record for {} operation on key {}", operation, key);

            Schema keySchema = dataCollectionSchema.keySchema();
            String topicName = topicSelector.topicNameFor((T) dataCollectionSchema.id());

            SourceRecord record = new SourceRecord(
                    partition.getSourcePartition(),
                    offsetContext.getOffset(),
                    topicName, null,
                    keySchema, key,
                    dataCollectionSchema.getEnvelopeSchema().schema(), value,
                    null, headers);
            dataListener.onEvent(dataCollectionSchema.id(), offsetContext, keySchema, value);
            queue.enqueue(changeEventCreator.createDataChangeEvent(record));
        }

        @Override
        public void completeSnapshot() throws InterruptedException {
        }
    }

    private final class SchemaChangeEventReceiver implements SchemaChangeEventEmitter.Receiver {

        private Struct schemaChangeRecordKey(SchemaChangeEvent event) {
            Struct result = new Struct(schemaChangeKeySchema);
            result.put(Fields.DATABASE_NAME, event.getDatabase());
            return result;
        }

        private Struct schemaChangeRecordValue(SchemaChangeEvent event) {
            Struct result = new Struct(schemaChangeValueSchema);
            result.put(Fields.SOURCE, event.getSource());
            result.put(Fields.DATABASE_NAME, event.getDatabase());
            result.put(Fields.SCHEMA_NAME, event.getSchema());
            result.put(Fields.DDL_STATEMENTS, event.getDdl());
            result.put(Fields.TABLE_CHANGES, tableChangesSerializer.serialize(event.getTableChanges()));
            return result;
        }

        @Override
        public void schemaChangeEvent(SchemaChangeEvent event) throws InterruptedException {
            historizedSchema.applySchemaChange(event);

            if (connectorConfig.isSchemaChangesHistoryEnabled()) {
                final String topicName = topicSelector.getPrimaryTopic();
                final Integer partition = 0;
                final Struct key = schemaChangeRecordKey(event);
                final Struct value = schemaChangeRecordValue(event);
                final SourceRecord record = new SourceRecord(event.getPartition(), event.getOffset(), topicName, partition,
                        schemaChangeKeySchema, key, schemaChangeValueSchema, value);
                enqueueSchemaChangeMessage(record);
            }
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
     * Enable support for incremental snapshotting.
     */
    public void setIncrementalSnapshotChangeEventSource(Optional<IncrementalSnapshotChangeEventSource<? extends DataCollectionId>> incrementalSnapshotChangeEventSource) {
        this.incrementalSnapshotChangeEventSource = (IncrementalSnapshotChangeEventSource<T>) incrementalSnapshotChangeEventSource.orElse(null);
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

    public DatabaseSchema<T> getSchema() {
        return schema;
    }

    public HistorizedDatabaseSchema<T> getHistorizedSchema() {
        return historizedSchema;
    }

    public IncrementalSnapshotChangeEventSource<T> getIncrementalSnapshotChangeEventSource() {
        return incrementalSnapshotChangeEventSource;
    }
}

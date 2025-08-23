/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionFilters.DataCollectionFilter;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;

public class MysqlEventDispatcher<P extends Partition, T extends DataCollectionId> extends EventDispatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlEventDispatcher.class);
    private final MySqlDatabaseSchema schema;
    private final MySqlConnectorConfig mySqlConnectorConfig;

    public MysqlEventDispatcher(CommonConnectorConfig connectorConfig, TopicNamingStrategy topicNamingStrategy,
                                MySqlDatabaseSchema schema, ChangeEventQueue queue, DataCollectionFilter filter,
                                ChangeEventCreator changeEventCreator,
                                InconsistentSchemaHandler inconsistentSchemaHandler,
                                EventMetadataProvider metadataProvider, Heartbeat heartbeat,
                                SchemaNameAdjuster schemaNameAdjuster, SignalProcessor signalProcessor,
                                DebeziumHeaderProducer debeziumHeaderProducer) {
        super(connectorConfig, topicNamingStrategy, schema, queue, filter, changeEventCreator,
                inconsistentSchemaHandler, metadataProvider, heartbeat, schemaNameAdjuster, signalProcessor, debeziumHeaderProducer);
        this.schema = schema;
        mySqlConnectorConfig = (MySqlConnectorConfig) connectorConfig;
    }

    @Override
    public SnapshotReceiver<P> getIncrementalSnapshotChangeEventReceiver(DataChangeEventListener dataListener) {
        return new IncrementalSnapshotChangeRecordReceiver(dataListener);
    }

    @Override
    public SnapshotReceiver<P> getSnapshotChangeEventReceiver() {
        return new BufferingSnapshotChangeRecordReceiver(this.getSnapshotMaxThreads() > 1);
    }

    private final class IncrementalSnapshotChangeRecordReceiver implements SnapshotReceiver<P> {

        public final DataChangeEventListener<P> dataListener;

        IncrementalSnapshotChangeRecordReceiver(DataChangeEventListener<P> dataListener) {
            this.dataListener = dataListener;
        }

        @Override
        public void changeRecord(P partition,
                                 DataCollectionSchema dataCollectionSchema,
                                 Operation operation,
                                 Object key, Struct value,
                                 OffsetContext offsetContext,
                                 ConnectHeaders headers)
                throws InterruptedException {
            Objects.requireNonNull(value, "value must not be null");

            LOGGER.trace("Received change record for {} operation on key {}", operation, key);

            Schema keySchema = dataCollectionSchema.keySchema();
            String topicName = topicNamingStrategy.dataChangeTopic((T) dataCollectionSchema.id());

            doPostProcessing(key, value);

            // TODO(hun): Marker for where to create a SourceRecord
            SourceRecord record = new SourceRecord(
                    partition.getSourcePartition(),
                    offsetContext.getOffset(),
                    topicName, null,
                    keySchema, key,
                    dataCollectionSchema.getEnvelopeSchema().schema(), value,
                    null, headers);
            dataListener.onEvent(partition, dataCollectionSchema.id(), offsetContext, keySchema, value, operation);
            queue.enqueue(changeEventCreator.createDataChangeEvent(record));
        }

        @Override
        public void completeSnapshot() throws InterruptedException {
        }
    }

    private static final class BufferedDataChangeEvent {
        private static final BufferedDataChangeEvent NULL = new BufferedDataChangeEvent();

        private DataChangeEvent dataChangeEvent;
        private OffsetContext offsetContext;

    }

    private final class BufferingSnapshotChangeRecordReceiver implements SnapshotReceiver<P> {

        private AtomicReference<BufferedDataChangeEvent> bufferedEventRef = new AtomicReference<>(BufferedDataChangeEvent.NULL);
        private final boolean threaded;

        BufferingSnapshotChangeRecordReceiver(boolean threaded) {
            this.threaded = threaded;
        }

        @Override
        public void changeRecord(P partition,
                                 DataCollectionSchema dataCollectionSchema,
                                 Operation operation,
                                 Object key, Struct value,
                                 OffsetContext offsetContext,
                                 ConnectHeaders headers)
                throws InterruptedException {
            Objects.requireNonNull(value, "value must not be null");

            LOGGER.trace("Received change record for {} operation on key {}", operation, key);

            doPostProcessing(key, value);

            applyUnavailablePlaceholdersForNoblobColumns(value, schema);

            SourceRecord record = new SourceRecord(
                    partition.getSourcePartition(),
                    offsetContext.getOffset(),
                    topicNamingStrategy.dataChangeTopic((T) dataCollectionSchema.id()),
                    null,
                    dataCollectionSchema.keySchema(),
                    key,
                    // TODO(hun): maybe it contains column
                    dataCollectionSchema.getEnvelopeSchema().schema(),
                    value,
                    null,
                    headers);

            BufferedDataChangeEvent nextBufferedEvent = new BufferedDataChangeEvent();
            nextBufferedEvent.offsetContext = offsetContext;
            nextBufferedEvent.dataChangeEvent = new DataChangeEvent(record);

            if (threaded) {
                // This entire step needs to happen atomically when using buffering with multiple threads.
                // This guarantees that the getAndSet and the enqueue do not cause a dispatch of out-of-order
                // events within a single thread.
                synchronized (queue) {
                    queue.enqueue(bufferedEventRef.getAndSet(nextBufferedEvent).dataChangeEvent);
                }
            }
            else {
                queue.enqueue(bufferedEventRef.getAndSet(nextBufferedEvent).dataChangeEvent);
            }
        }

        @Override
        public void completeSnapshot() throws InterruptedException {
            // It is possible that the last snapshotted table was empty
            // this way we ensure that the last event is always marked as last
            // even if it originates form non-last table
            final BufferedDataChangeEvent bufferedEvent = bufferedEventRef.getAndSet(BufferedDataChangeEvent.NULL);
            DataChangeEvent event = bufferedEvent.dataChangeEvent;
            if (event != null) {
                SourceRecord record = event.getRecord();
                final Struct envelope = (Struct) record.value();
                if (envelope.schema().field(Envelope.FieldName.SOURCE) != null) {
                    final Struct source = envelope.getStruct(Envelope.FieldName.SOURCE);
                    SnapshotRecord.LAST.toSource(source);
                }
                @SuppressWarnings("unchecked")
                final Map<String, Object> offset = (Map<String, Object>) record.sourceOffset();
                offset.clear();
                offset.putAll(bufferedEvent.offsetContext.getOffset());
                queue.enqueue(event);
            }
        }
    }

    private Struct applyUnavailablePlaceholders(Struct original, Set<String> fieldsToPlaceholder) {
        final String unavailableValuePlaceholderString = new String(mySqlConnectorConfig.getUnavailableValuePlaceholder());
        final ByteBuffer unavailableValuePlaceholderByteBuffer = ByteBuffer.wrap(mySqlConnectorConfig.getUnavailableValuePlaceholder());
        final Schema schema = original.schema();
        final Struct updated = new Struct(schema);
        for (Field field : schema.fields()) {
            final String fieldName = field.name();
            final Object originalValue = original.get(field);
            if (fieldsToPlaceholder.contains(fieldName)) {
                switch (field.schema().type()) {
                    case STRING:
                        updated.put(fieldName, unavailableValuePlaceholderString);
                        break;
                    case BYTES:
                        updated.put(fieldName, unavailableValuePlaceholderByteBuffer);
                        break;
                    default:
                        // For non-text/binary types, leave as-is (or extend as needed)
                        updated.put(fieldName, originalValue);
                        break;
                }
            }
            else  {
                updated.put(fieldName, originalValue);
            }
        }
        return updated;
    }

    private void applyUnavailablePlaceholdersForNoblobColumns(Struct value, MySqlDatabaseSchema schema) {
        final Set<TableId> tableIds = schema.tableIds();
        final Table table = schema.tableFor(tableIds.stream().findFirst().get());
        final List<Column> columns = table.columns();
        final Set<String> shouldSkip = new HashSet<>();
        for (Column column : columns) {
            final String typeName = column.typeName();
            final String name = column.name();
            if ("BLOB".equalsIgnoreCase(typeName) || "TEXT".equalsIgnoreCase(typeName)) {
                shouldSkip.add(name);
            }
        }

        final Struct after = value.getStruct("after");
        if (after != null) {
            value.put("after", applyUnavailablePlaceholders(after, shouldSkip));
        }

        final Struct before = value.getStruct("before");
        if (before != null) {
            value.put("before", applyUnavailablePlaceholders(before, shouldSkip));
        }
    }
}

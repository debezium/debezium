/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.batch.Batch;
import io.debezium.sink.batch.Buffer;
import io.debezium.sink.batch.DeduplicatedBuffer;
import io.debezium.sink.batch.KeylessBuffer;
import io.debezium.sink.spi.ChangeEventSink;
import io.debezium.util.Stopwatch;

/**
 * - Apply filtering fields/columns
 * - Deduplication
 * - Batching and buffering until buffer/s are full and can be emitted
 * - Flushing buffers when they are full or by timeout
 * - Handle retries
 * - Handle schema changes?
 */
public abstract class AbstractChangeEventSink implements ChangeEventSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractChangeEventSink.class);
    private static final String DETECT_SCHEMA_CHANGE_RECORD_MSG = "Schema change records are not supported by Debezium sink connectors. Please adjust `topics` or `topics.regex` to exclude schema change topic.";

    protected final SinkConnectorConfig config;
    private final Buffer buffer;

    protected AbstractChangeEventSink(SinkConnectorConfig config) {
        this.config = config;
        if (SinkConnectorConfig.PrimaryKeyMode.NONE.equals(config.getPrimaryKeyMode())) {
            buffer = new KeylessBuffer(config);
        }
        else {
            // might also require: !record.keyFieldNames().isEmpty()
            buffer = new DeduplicatedBuffer(config);
        }
    }

    public Optional<CollectionId> getCollectionIdFromRecord(DebeziumSinkRecord record) {
        String tableName = this.config.getCollectionNamingStrategy().resolveCollectionName(record, config.getCollectionNameFormat());
        if (tableName == null) {
            return Optional.empty();
        }
        return getCollectionId(tableName);
    }

    @Override
    public Batch put(Collection<SinkRecord> records) {
        // @TODO in the future a list of Debezium sink records should be passed instead of the Kafka Connect's SinkRecord instances
        final Map<CollectionId, Buffer> updateBufferByTable = new LinkedHashMap<>();
        final Map<CollectionId, Buffer> deleteBufferByTable = new LinkedHashMap<>();

        for (SinkRecord kafkaSinkRecord : records) {
            LOGGER.trace("Processing {}", kafkaSinkRecord);

            DebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaSinkRecord);

            if (record.isSchemaChange()) { // schema change records are not yet supported
                LOGGER.error(DETECT_SCHEMA_CHANGE_RECORD_MSG);
                continue;
            }

            Optional<CollectionId> optionalCollectionId = getCollectionIdFromRecord(record);
            if (optionalCollectionId.isEmpty()) {
                LOGGER.warn("Ignored to write record from topic '{}' partition '{}' offset '{}'. No resolvable table name.",
                        record.topicName(), record.partition(), record.offset());
                continue;
            }

            final CollectionId collectionId = optionalCollectionId.get();

            if (record.isTruncate()) {
                if (!config.isTruncateEnabled()) {
                    LOGGER.debug("Truncates are not enabled, skipping truncate for topic '{}'", record.topicName());
                    continue;
                }

                // clear all batches for the resolved collection/table and add the truncate event to the buffer
                buffer.truncate(collectionId, record);
                continue;
            }

            if (record.isDelete()) {
                if (!config.isDeleteEnabled()) {
                    LOGGER.debug("Deletes are not enabled, skipping delete for topic '{}'", record.topicName());
                    continue;
                }

                buffer.enqueue(collectionId, record);
            }
            else {
                // lookup if the record is already in queue

                Buffer buffer = deleteBufferByTable.get(collectionId);
                if (buffer != null && buffer.size() > 0) {
                    // When an insert arrives, delete buffer must be flushed to avoid losing an insert for the same record after its deletion.
                    // this because at the end we will always flush inserts before deletes.
                    // flushBuffer(collectionId, deleteBufferByTable.get(collectionId).flush());
                    // @TODO
                }

                Stopwatch updateBufferStopwatch = Stopwatch.reusable();
                updateBufferStopwatch.start();

                // Buffer tableIdBuffer = resolveBuffer(updateBufferByTable, collectionId, sinkRecordDescriptor);
                // List<JdbcSinkRecordDescriptor> toFlush = tableIdBuffer.add(sinkRecordDescriptor);
                updateBufferStopwatch.stop();

                LOGGER.trace("[PERF] Update buffer execution time {}", updateBufferStopwatch.durations());
                // flushBuffer(collectionId, toFlush);
            }

        }

        // flushBuffers(updateBufferByTable);
        // flushBuffers(deleteBufferByTable);
        return buffer.poll();
    }
}

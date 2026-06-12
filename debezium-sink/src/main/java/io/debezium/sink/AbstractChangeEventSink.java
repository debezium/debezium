/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink;

import java.util.Collection;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.batch.Batch;
import io.debezium.sink.batch.Buffer;
import io.debezium.sink.batch.DeduplicatingBuffer;
import io.debezium.sink.batch.KeyedPassthroughBuffer;
import io.debezium.sink.batch.KeylessPassthroughBuffer;
import io.debezium.sink.spi.ChangeEventSink;

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
    private int batchCount;
    private int totalRecordsWritten;

    protected AbstractChangeEventSink(SinkConnectorConfig config) {
        this.config = config;
        if (SinkConnectorConfig.PrimaryKeyMode.NONE.equals(config.getPrimaryKeyMode())) {
            buffer = new KeylessPassthroughBuffer(config);
        }
        else if (SinkConnectorConfig.KeyedMessageBatchMode.PASSTHROUGH.equals(config.getKeyedMessageBatchMode())) {
            buffer = new KeyedPassthroughBuffer(config);
        }
        else {
            buffer = new DeduplicatingBuffer(config);
        }
    }

    protected DebeziumSinkRecord createSinkRecord(SinkRecord kafkaSinkRecord) {
        return new KafkaDebeziumSinkRecord(kafkaSinkRecord, config.cloudEventsSchemaNamePattern());
    }

    public CollectionId getCollectionIdFromRecord(DebeziumSinkRecord record) {
        String tableName = this.config.getCollectionNamingStrategy().resolveCollectionName(record, config.getCollectionNameFormat());
        if (tableName == null) {
            return null;
        }
        return getCollectionId(tableName);
    }

    @Override
    public List<Batch> put(Collection<SinkRecord> records) {
        for (SinkRecord kafkaSinkRecord : records) {
            LOGGER.trace("Processing {}", kafkaSinkRecord);

            DebeziumSinkRecord record = createSinkRecord(kafkaSinkRecord);

            if (record.isSchemaChange()) { // schema change records are not supported
                LOGGER.error(DETECT_SCHEMA_CHANGE_RECORD_MSG);
                continue;
            }

            CollectionId collectionId = getCollectionIdFromRecord(record);
            if (null == collectionId) {
                LOGGER.warn("Ignored to write record from topic '{}' partition '{}' offset '{}'. No resolvable table name.",
                        record.topicName(), record.partition(), record.offset());
                continue;
            }

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
            }

            buffer.enqueue(collectionId, record);
        }

        return buffer.poll();
    }

    public List<Batch> forcePoll() {
        if (buffer != null) {
            return buffer.forcePoll();
        }
        return List.of();
    }

    public void flush() {
        for (Batch batch : forcePoll()) {
            writeBatch(batch);
        }
    }

    public int getBatchCount() {
        return batchCount;
    }

    public int getTotalRecordsWritten() {
        return totalRecordsWritten;
    }

    protected void writeBatch(Batch batch) {
        batchCount++;
        totalRecordsWritten += batch.size();
        doWriteBatch(batch);
    }

    protected abstract void doWriteBatch(Batch batch);
}

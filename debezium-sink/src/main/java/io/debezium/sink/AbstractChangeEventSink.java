/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink;

import java.util.Collection;

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

    public CollectionId getCollectionIdFromRecord(DebeziumSinkRecord record) {
        String tableName = this.config.getCollectionNamingStrategy().resolveCollectionName(record, config.getCollectionNameFormat());
        if (tableName == null) {
            return null;
        }
        return getCollectionId(tableName);
    }

    @Override
    public Batch put(Collection<SinkRecord> records) {
        // @TODO in the future a list of Debezium sink records should be passed instead of the Kafka Connect's SinkRecord instances
        for (SinkRecord kafkaSinkRecord : records) {
            LOGGER.trace("Processing {}", kafkaSinkRecord);

            DebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaSinkRecord);

            if (record.isSchemaChange()) { // schema change records are not yet supported
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
                buffer.truncate(record);
                continue;
            }

            if (record.isDelete()) {
                if (!config.isDeleteEnabled()) {
                    LOGGER.debug("Deletes are not enabled, skipping delete for topic '{}'", record.topicName());
                    continue;
                }
            }
            buffer.enqueue(record);

        }

        return buffer.poll();
    }
}

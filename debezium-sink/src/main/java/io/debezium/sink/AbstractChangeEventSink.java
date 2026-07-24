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
import io.debezium.dlq.ErrorReporter;
import io.debezium.dlq.ErrorReporters;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.batch.Batch;
import io.debezium.sink.batch.BatchRecord;
import io.debezium.sink.batch.Buffer;
import io.debezium.sink.batch.DeduplicatingBuffer;
import io.debezium.sink.batch.PassthroughBuffer;
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
    private final ErrorReporter errorReporter;
    private final Buffer buffer;
    private int batchCount;
    private int totalRecordsWritten;

    protected AbstractChangeEventSink(SinkConnectorConfig config) {
        this(config, ErrorReporters.nop());
    }

    protected AbstractChangeEventSink(SinkConnectorConfig config, ErrorReporter errorReporter) {
        this.config = config;
        this.errorReporter = errorReporter != null ? errorReporter : ErrorReporters.nop();
        boolean keyed = !SinkConnectorConfig.PrimaryKeyMode.NONE.equals(config.getPrimaryKeyMode());
        if (SinkConnectorConfig.KeyedMessageBatchMode.PASSTHROUGH.equals(config.getKeyedMessageBatchMode())) {
            buffer = new PassthroughBuffer(config, keyed);
        }
        else {
            buffer = new DeduplicatingBuffer(config, keyed);
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

            if (record.isSchemaChange()) {
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

    protected void forceFlush() {
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
        try {
            doWriteBatch(batch);
        }
        catch (RuntimeException e) {
            if (!ErrorReporters.isEnabled(errorReporter) || isRetriableWriteException(e)) {
                throw e;
            }
            reportFailedRecords(batch, e);
        }
    }

    /**
     * Retries the records of a failed batch one at a time to isolate the failing ones and routes
     * only those to the errant record reporter, so a single bad record does not fail the task and
     * does not discard the healthy records written alongside it.
     */
    private void reportFailedRecords(Batch batch, RuntimeException cause) {
        if (batch.size() == 1) {
            reportRecord(batch.get(0), cause);
            return;
        }
        LOGGER.warn("Batch of {} records failed to be written; retrying records individually to isolate the failing ones",
                batch.size(), cause);
        for (BatchRecord batchRecord : batch) {
            try {
                doWriteBatch(new Batch(List.of(batchRecord)));
            }
            catch (RuntimeException e) {
                if (isRetriableWriteException(e)) {
                    throw e;
                }
                reportRecord(batchRecord, e);
            }
        }
    }

    private void reportRecord(BatchRecord batchRecord, RuntimeException cause) {
        DebeziumSinkRecord record = batchRecord.record();
        LOGGER.warn("Reporting failed record from topic '{}' partition '{}' offset '{}' to the errant record reporter",
                record.topicName(), record.partition(), record.offset(), cause);
        totalRecordsWritten--;
        errorReporter.report(record, cause);
    }

    /**
     * Returns whether the given write failure is transient (e.g. caused by a communication problem
     * with the target system) and should be propagated so existing retry mechanisms apply, instead
     * of being routed to the errant record reporter. Defaults to {@code false}; subclasses should
     * override this based on the failure semantics of their target system.
     */
    protected boolean isRetriableWriteException(RuntimeException exception) {
        return false;
    }

    protected abstract void doWriteBatch(Batch batch);
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.sink.spi.ChangeEventSink;

/**
 * The main task executing streaming from sink connector.
 * Responsible for lifecycle management of the streaming code.
 *
 * @author Hossein Torabi
 */
public class JdbcSinkConnectorTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkConnectorTask.class);

    private enum State {
        RUNNING,
        STOPPED;
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.STOPPED);
    private final ReentrantLock stateLock = new ReentrantLock();

    private ChangeEventSink changeEventSink;
    private Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    private Throwable previousPutException;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        stateLock.lock();

        try {
            if (!state.compareAndSet(State.STOPPED, State.RUNNING)) {
                LOGGER.info("Connector has already been started");
                return;
            }

            // be sure to reset this state
            previousPutException = null;

            final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(props);
            config.validate();

            changeEventSink = new JdbcChangeEventSink(config);
        }
        finally {
            stateLock.unlock();
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (previousPutException != null) {
            throw new ConnectException("JDBC sink connector failure", previousPutException);
        }

        LOGGER.debug("Received {} changes.", records.size());

        for (Iterator<SinkRecord> iterator = records.iterator(); iterator.hasNext();) {
            final SinkRecord record = iterator.next();
            LOGGER.trace("Received {}", record);
            try {
                changeEventSink.execute(record);
                markProcessed(record);
            }
            catch (Throwable throwable) {
                // Stash currently failed record
                markNotProcessed(record);

                // Capture failure
                LOGGER.error("Failed to process record: {}", throwable.getMessage());
                previousPutException = throwable;

                // Stash any remaining records
                markNotProcessed(iterator);
            }
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        // Flush only up to the records processed by this sink
        LOGGER.debug("Flushing offsets: {}", offsets);
        flush(offsets);
        return offsets;
    }

    @Override
    public void stop() {
        if (changeEventSink != null) {
            try {
                changeEventSink.close();
            }
            catch (Exception e) {
                LOGGER.error("Failed to gracefully close resources.", e);
            }
        }
    }

    /**
     * Marks a sink record as processed.
     *
     * @param record sink record, should not be {@code null}
     */
    private void markProcessed(SinkRecord record) {
        final TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        final OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.kafkaOffset() + 1L);

        final OffsetAndMetadata existing = offsets.put(topicPartition, offsetAndMetadata);
        if (existing == null) {
            LOGGER.trace("Advanced topic {} to offset {}.", record.topic(), record.kafkaOffset());
        }
        else {
            LOGGER.trace("Updated topic {} from offset {} to {}.", record.topic(), existing.offset(), record.kafkaOffset());
        }
    }

    /**
     * Marks all remaining elements in the collection as not processed.
     *
     * @param records collection of sink records, should not be {@code null}
     */
    private void markNotProcessed(Iterator<SinkRecord> records) {
        while (records.hasNext()) {
            markNotProcessed(records.next());
        }
        if (context != null) {
            context.requestCommit();
        }
    }

    /**
     * Marks a single record as not processed.
     *
     * @param record sink record, should not be {@code null}
     */
    private void markNotProcessed(SinkRecord record) {
        // Sink connectors operate on batches and a batch could technically include a stream of records
        // where the same topic/partition tuple exists in the batch at various points, before and after
        // other topic/partition tuples. When marking a record as not processed, we are only interested
        // in doing this if this tuple is not already in the map as a previous entry could have been
        // added because an earlier record was either processed or marked as not processed since any
        // remaining entries in the batch call this method on failures.
        final TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        if (!offsets.containsKey(topicPartition)) {
            LOGGER.debug("Rewinding topic {} offset to {}.", record.topic(), record.kafkaOffset());
            final OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.kafkaOffset());
            offsets.put(topicPartition, offsetAndMetadata);
        }
    }

}

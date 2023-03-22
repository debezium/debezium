/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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
    private Map<TopicPartition, OffsetAndMetadata> processedOffsets = new HashMap<>();

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
        for (SinkRecord record : records) {
            LOGGER.debug("Applying {}", record);
            changeEventSink.execute(record);
            updateProcessedOffsets(record);
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        // Flush only up to the records processed by this sink
        // todo: need to check whether this allows resuming in the middle of a collection after an error
        LOGGER.debug("Flushing offsets: {}", processedOffsets);
        flush(processedOffsets);
        return processedOffsets;
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

    private void updateProcessedOffsets(SinkRecord record) {
        final TopicPartition key = new TopicPartition(record.topic(), record.kafkaPartition());
        final OffsetAndMetadata value = new OffsetAndMetadata(record.kafkaOffset());
        final OffsetAndMetadata existing = processedOffsets.put(key, value);
        if (existing != null) {
            LOGGER.debug("Updated topic '{}' offset from {} to {}", record.topic(), existing.offset(), value.offset());
        }
        else {
            LOGGER.debug("Set topic '{}' offset to {}", record.topic(), record.kafkaOffset());
        }
    }
}

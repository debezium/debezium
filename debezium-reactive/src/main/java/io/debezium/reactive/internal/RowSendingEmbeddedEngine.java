/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive.internal;

import java.util.List;
import java.util.function.Consumer;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.embedded.StopConnectorException;
import io.debezium.embedded.internal.AbstractEmbeddedEngine;
import io.debezium.embedded.spi.OffsetCommitPolicy;
import io.debezium.util.Clock;

/**
 * A an implementation of EmbeddedEngine that sends records one-by-one, not in batches
 *
 * @author Randall Hauch, Jiri Pechanec
 *
 */
@ThreadSafe
public class RowSendingEmbeddedEngine extends AbstractEmbeddedEngine {
    List<SourceRecord> changeRecords = null;

    public RowSendingEmbeddedEngine(Configuration config, ClassLoader classLoader, Clock clock, Consumer<SourceRecord> consumer,
                           CompletionCallback completionCallback, ConnectorCallback connectorCallback,
                           OffsetCommitPolicy offsetCommitPolicy) {
        super(config, classLoader, clock, consumer, completionCallback, connectorCallback, offsetCommitPolicy);
    }

    private boolean hasRecordsToProcess() {
        return changeRecords != null && !changeRecords.isEmpty();
    }

    /**
     * Tries to poll a connector for a List of events and process a single row from the list. Subsequent
     * calls process the remaining rows one by one before another call to connector is executed.
     */
    @Override
    public void run() {
        try {
            if (hasRecordsToProcess()) {
                processRecord();
                return;
            }
            changeRecords = readRecordsFromConnector();
            if (hasRecordsToProcess()) {
                logger.debug("Received {} records from the task", changeRecords.size());
                processRecord();
            }
            else {
                logger.debug("Received no records from the task");
            }
        }
        catch (InterruptedException e) {
            logger.info("Termination of polling requested");
        }
    }

    private void processRecord() throws InterruptedException {
        final SourceRecord record = changeRecords.get(0);
        try {
            consumer.accept(record);
            if (handlerError != null) {
                throw new ConnectException(handlerError);
            }
        }
        catch (StopConnectorException e) {
            // Stop processing any more but first record the offset for this record's partition
            offsetWriter.offset(record.sourcePartition(), record.sourceOffset());
            recordsSinceLastCommit += 1;
            throw e;
        }
    }

    /**
     * Commits an offset of the record into the offset store. The flush of the store happens
     * only after all records for the polled batch are processed.
     *
     * @param record to be committed
     * @throws InterruptedException
     */
    public void commitRecord(final SourceRecord record) throws InterruptedException {
        final SourceRecord recordFromHead = changeRecords.remove(0);
        assert record == recordFromHead : "Trying to commit record not from the top of batch";
        task.commitRecord(record);
        // Record the offset for this record's partition
        offsetWriter.offset(record.sourcePartition(), record.sourceOffset());
        recordsSinceLastCommit += 1;
        if (changeRecords.isEmpty()) {
            // Flush the offsets to storage if necessary ...
            maybeFlush(offsetWriter, offsetCommitPolicy, commitTimeoutMs, task);
        }
    }
}

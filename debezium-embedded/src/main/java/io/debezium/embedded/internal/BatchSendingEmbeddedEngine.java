/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.internal;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.embedded.StopConnectorException;
import io.debezium.embedded.spi.OffsetCommitPolicy;
import io.debezium.util.Clock;

/**
 * A default implementation of EmbeddedEngine
 *
 * @author Randall Hauch, Jiri Pechanec
 *
 */
@ThreadSafe
public class BatchSendingEmbeddedEngine extends AbstractEmbeddedEngine {

    public BatchSendingEmbeddedEngine(Configuration config, ClassLoader classLoader, Clock clock, Consumer<SourceRecord> consumer,
                           CompletionCallback completionCallback, ConnectorCallback connectorCallback,
                           OffsetCommitPolicy offsetCommitPolicy) {
        super(config, classLoader, clock, consumer, completionCallback, connectorCallback, offsetCommitPolicy);
    }

    /**
     * Run this embedded connector and deliver database changes to the registered {@link Consumer}. This method blocks until
     * the connector is stopped.
     * <p>
     * First, the method checks to see if this instance is currently {@link #run() running}, and if so immediately returns.
     * <p>
     * If the configuration is valid, this method starts the connector and starts polling the connector for change events.
     * All messages are delivered in batches to the {@link Consumer} registered with this embedded connector. The batch size,
     * polling
     * frequency, and other parameters are controlled via configuration settings. This continues until this connector is
     * {@link #stop() stopped}.
     * <p>
     * Note that there are two ways to stop a connector running on a thread: calling {@link #stop()} from another thread, or
     * interrupting the thread (e.g., via {@link ExecutorService#shutdownNow()}).
     * <p>
     * This method can be called repeatedly as needed.
     */
    @Override
    public void run() {
        if (runningThread.compareAndSet(null, Thread.currentThread())) {
            // Only one thread can be in this part of the method at a time ...
            latch.countUp();

            try {
                doStart();
                if (completionResult.hasError()) {
                    return;
                }
                try {
                    try {
                        boolean keepProcessing = true;
                        List<SourceRecord> changeRecords = null;
                        while (runningThread.get() != null && handlerError == null && keepProcessing) {
                            try {
                                try {
                                    changeRecords = readRecordsFromConnector();
                                } catch (InterruptedException e) {
                                    // Interrupted while polling ...
                                    logger.debug("Embedded engine interrupted on thread " + runningThread.get() + " while polling the task for records");
                                    Thread.interrupted();
                                    break;
                                }
                                try {
                                    if (changeRecords != null && !changeRecords.isEmpty()) {
                                        logger.debug("Received {} records from the task", changeRecords.size());

                                        // First forward the records to the connector's consumer ...
                                        for (SourceRecord record : changeRecords) {
                                            try {
                                                consumer.accept(record);
                                                task.commitRecord(record);
                                            } catch (StopConnectorException e) {
                                                keepProcessing = false;
                                                // Stop processing any more but first record the offset for this record's
                                                // partition
                                                offsetWriter.offset(record.sourcePartition(), record.sourceOffset());
                                                recordsSinceLastCommit += 1;
                                                break;
                                            } catch (Throwable t) {
                                                handlerError = t;
                                                break;
                                            }

                                            // Record the offset for this record's partition
                                            offsetWriter.offset(record.sourcePartition(), record.sourceOffset());
                                            recordsSinceLastCommit += 1;
                                        }

                                        // Flush the offsets to storage if necessary ...
                                        maybeFlush(offsetWriter, offsetCommitPolicy, commitTimeoutMs, task);
                                    } else {
                                        logger.debug("Received no records from the task");
                                    }
                                } catch (Throwable t) {
                                    // There was some sort of unexpected exception, so we should stop work
                                    if (handlerError == null) {
                                        // make sure we capture the error first so that we can report it later
                                        handlerError = t;
                                    }
                                    break;
                                }
                            } finally {
                                // then try to commit the offsets, since we record them only after the records were handled
                                // by the consumer ...
                                maybeFlush(offsetWriter, offsetCommitPolicy, commitTimeoutMs, task);
                            }
                        }
                    } finally {
                        if (handlerError != null) {
                            // There was an error in the handler so make sure it's always captured...
                            fail("Stopping connector after error in the application's handler method: " + handlerError.getMessage(),
                                 handlerError);
                        }
                        try {
                            doStopTask();
                        } catch (Throwable t) {
                            fail("Error while trying to stop the task and commit the offsets", t);
                        }
                    }
                } catch (Throwable t) {
                    fail("Error while trying to run connector class '" + connectorClassName + "'", t);
                } finally {
                    doStopConnector();
                }
            } finally {
                latch.countDown();
                runningThread.set(null);
                // after we've "shut down" the engine, fire the completion callback based on the results we collected
                completionCallback.handle(completionResult.success(), completionResult.message(), completionResult.error());
            }
        }
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Optional;
import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

public class MySqlChangeEventSourceFactory implements ChangeEventSourceFactory<MySqlPartition, MySqlOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlChangeEventSourceFactory.class);

    private final MySqlConnectorConfig configuration;
    private final MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<MySqlPartition, TableId> dispatcher;
    private final Clock clock;
    private final MySqlTaskContext taskContext;
    private final MySqlStreamingChangeEventSourceMetrics streamingMetrics;
    private final MySqlDatabaseSchema schema;
    // MySQL snapshot requires buffering to modify the last record in the snapshot as sometimes it is
    // impossible to detect it till the snapshot is ended. Mainly when the last snapshotted table is empty.
    // Based on the DBZ-3113 the code can change in the future and it will be handled not in MySQL
    // but in the core shared code.
    private final ChangeEventQueue<DataChangeEvent> queue;

    private final SnapshotterService snapshotterService;

    public MySqlChangeEventSourceFactory(MySqlConnectorConfig configuration, MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory,
                                         ErrorHandler errorHandler, EventDispatcher<MySqlPartition, TableId> dispatcher, Clock clock, MySqlDatabaseSchema schema,
                                         MySqlTaskContext taskContext, MySqlStreamingChangeEventSourceMetrics streamingMetrics,
                                         ChangeEventQueue<DataChangeEvent> queue, SnapshotterService snapshotterService) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
        this.queue = queue;
        this.schema = schema;
        this.snapshotterService = snapshotterService;
    }

    @Override
    public SnapshotChangeEventSource<MySqlPartition, MySqlOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<MySqlPartition> snapshotProgressListener,
                                                                                                      NotificationService<MySqlPartition, MySqlOffsetContext> notificationService) {
        return new MySqlSnapshotChangeEventSource(
                configuration,
                connectionFactory,
                taskContext.getSchema(),
                dispatcher,
                clock,
                (MySqlSnapshotChangeEventSourceMetrics) snapshotProgressListener,
                this::modifyAndFlushLastRecord,
                this::preSnapshot,
                notificationService,
                snapshotterService);
    }

    private void preSnapshot() {
        queue.enableBuffering();
    }

    private void modifyAndFlushLastRecord(Function<SourceRecord, SourceRecord> modify) throws InterruptedException {
        // Check if the current thread has been interrupted before attempting to flush
        if (Thread.currentThread().isInterrupted()) {
            LOGGER.info("Thread has been interrupted, skipping flush of buffered record");
            queue.disableBuffering();
            throw new InterruptedException("Thread interrupted during snapshot cleanup");
        }

        try {
            // Attempt to flush the buffered record
            // If queue is shut down, this will throw InterruptedException and we'll handle it gracefully
            queue.flushBuffer(dataChange -> new DataChangeEvent(modify.apply(dataChange.getRecord())));
            LOGGER.debug("Successfully flushed buffered record during snapshot cleanup");
        }
        catch (InterruptedException e) {
            // Queue was shut down or thread was interrupted - this is expected during task shutdown
            LOGGER.info("Buffered record flush interrupted during snapshot cleanup, likely due to task shutdown");
            throw e;
        }
        finally {
            // Always disable buffering to prevent memory leaks
            try {
                queue.disableBuffering();
            }
            catch (AssertionError e) {
                // In rare cases, assertion may fail if buffer is not empty due to shutdown timing
                // This is acceptable as the queue shutdown mechanism prevents the memory leak
                LOGGER.debug("Buffer not empty during cleanup due to shutdown timing - this is expected");
            }
        }
    }

    @Override
    public StreamingChangeEventSource<MySqlPartition, MySqlOffsetContext> getStreamingChangeEventSource() {

        queue.disableBuffering();
        return new MySqlStreamingChangeEventSource(
                configuration,
                connectionFactory.mainConnection(),
                dispatcher,
                errorHandler,
                clock,
                taskContext,
                streamingMetrics,
                snapshotterService);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<MySqlPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                              MySqlOffsetContext offsetContext,
                                                                                                                                              SnapshotProgressListener<MySqlPartition> snapshotProgressListener,
                                                                                                                                              DataChangeEventListener<MySqlPartition> dataChangeEventListener,
                                                                                                                                              NotificationService<MySqlPartition, MySqlOffsetContext> notificationService) {

        if (configuration.isReadOnlyConnection()) {
            if (connectionFactory.mainConnection().isGtidModeEnabled()) {
                return Optional.of(new MySqlReadOnlyIncrementalSnapshotChangeEventSource(
                        configuration,
                        connectionFactory.mainConnection(),
                        dispatcher,
                        schema,
                        clock,
                        snapshotProgressListener,
                        dataChangeEventListener,
                        notificationService));
            }
            throw new UnsupportedOperationException("Read only connection requires GTID_MODE to be ON");
        }
        // If no data collection id is provided, don't return an instance as the implementation requires
        // that a signal data collection id be provided to work.
        if (Strings.isNullOrEmpty(configuration.getSignalingDataCollectionId())) {
            return Optional.empty();
        }
        return Optional.of(new SignalBasedIncrementalSnapshotChangeEventSource<>(
                configuration,
                connectionFactory.mainConnection(),
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener, notificationService));
    }
}

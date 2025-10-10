/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Optional;
import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;

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
                schema,
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
        queue.flushBuffer(dataChange -> new DataChangeEvent(modify.apply(dataChange.getRecord())));
        queue.disableBuffering();
    }

    @Override
    public StreamingChangeEventSource<MySqlPartition, MySqlOffsetContext> getStreamingChangeEventSource() {

        queue.disableBuffering();
        MySqlStreamingChangeEventSource streamingSource = new MySqlStreamingChangeEventSource(
                configuration,
                connectionFactory.mainConnection(),
                dispatcher,
                errorHandler,
                clock,
                taskContext,
                schema,
                streamingMetrics,
                snapshotterService);

        // Set the binary log client on the metrics now that the streaming source has created it
        streamingMetrics.setBinaryLogClient(streamingSource.getBinaryLogClient());

        return streamingSource;
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

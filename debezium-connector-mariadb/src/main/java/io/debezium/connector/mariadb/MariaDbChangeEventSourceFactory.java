/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.util.Optional;
import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.mariadb.metrics.MariaDbSnapshotChangeEventSourceMetrics;
import io.debezium.connector.mariadb.metrics.MariaDbStreamingChangeEventSourceMetrics;
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

/**
 * @author Chris Cranford
 */
public class MariaDbChangeEventSourceFactory implements ChangeEventSourceFactory<MariaDbPartition, MariaDbOffsetContext> {

    private final MariaDbConnectorConfig configuration;
    private final MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<MariaDbPartition, TableId> dispatcher;
    private final Clock clock;
    private final MariaDbTaskContext taskContext;
    private final MariaDbStreamingChangeEventSourceMetrics streamingMetrics;
    private final MariaDbDatabaseSchema schema;
    // Snapshot requires buffering to modify the last record in the snapshot as sometimes it is
    // impossible to detect it till the snapshot is ended. Mainly when the last snapshotted table is empty.
    // Based on the DBZ-3113 the code can change in the future, and it will be handled in the core shared code.
    private final ChangeEventQueue<DataChangeEvent> queue;
    private final SnapshotterService snapshotterService;

    public MariaDbChangeEventSourceFactory(MariaDbConnectorConfig configuration,
                                           MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory,
                                           ErrorHandler errorHandler,
                                           EventDispatcher<MariaDbPartition, TableId> dispatcher,
                                           Clock clock,
                                           MariaDbDatabaseSchema schema,
                                           MariaDbTaskContext taskContext,
                                           MariaDbStreamingChangeEventSourceMetrics streamingMetrics,
                                           ChangeEventQueue<DataChangeEvent> queue,
                                           SnapshotterService snapshotterService) {
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
    public SnapshotChangeEventSource<MariaDbPartition, MariaDbOffsetContext> getSnapshotChangeEventSource(
                                                                                                          SnapshotProgressListener<MariaDbPartition> snapshotProgressListener,
                                                                                                          NotificationService<MariaDbPartition, MariaDbOffsetContext> notificationService) {
        return new MariaDbSnapshotChangeEventSource(
                configuration,
                connectionFactory,
                taskContext.getSchema(),
                dispatcher,
                clock,
                (MariaDbSnapshotChangeEventSourceMetrics) snapshotProgressListener,
                this::modifyAndFlushLastRecord,
                this::preSnapshot,
                notificationService,
                snapshotterService);
    }

    @Override
    public StreamingChangeEventSource<MariaDbPartition, MariaDbOffsetContext> getStreamingChangeEventSource() {
        queue.disableBuffering();
        return new MariaDbStreamingChangeEventSource(
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
    public Optional<IncrementalSnapshotChangeEventSource<MariaDbPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                                MariaDbOffsetContext offsetContext,
                                                                                                                                                SnapshotProgressListener<MariaDbPartition> snapshotProgressListener,
                                                                                                                                                DataChangeEventListener<MariaDbPartition> dataChangeEventListener,
                                                                                                                                                NotificationService<MariaDbPartition, MariaDbOffsetContext> notificationService) {
        if (configuration.isReadOnlyConnection()) {
            if (connectionFactory.mainConnection().isGtidModeEnabled()) {
                return Optional.of(new MariaDbReadOnlyIncrementalSnapshotChangeEventSource(
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

        // If no data collection id is provided, don't return an instance as the implement requires the table
        if (Strings.isNullOrBlank(configuration.getSignalingDataCollectionId())) {
            return Optional.empty();
        }

        return Optional.of(new SignalBasedIncrementalSnapshotChangeEventSource<>(
                configuration,
                connectionFactory.mainConnection(),
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener,
                notificationService));
    }

    private void preSnapshot() {
        queue.enableBuffering();
    }

    private void modifyAndFlushLastRecord(Function<SourceRecord, SourceRecord> modify) throws InterruptedException {
        queue.flushBuffer(dataChange -> new DataChangeEvent(modify.apply(dataChange.getRecord())));
        queue.disableBuffering();
    }
}

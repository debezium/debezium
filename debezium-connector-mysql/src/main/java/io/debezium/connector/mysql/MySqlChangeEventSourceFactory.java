/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class MySqlChangeEventSourceFactory implements ChangeEventSourceFactory {

    private final MySqlConnectorConfig configuration;
    private final MySqlConnection connection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final MySqlTaskContext taskContext;
    private final MySqlStreamingChangeEventSourceMetrics streamingMetrics;
    // MySQL snapshot requires buffering to modify the last record in the snapshot as sometimes it is
    // impossible to detect it till the snapshot is ended. Mainly when the last snapshotted table is empty.
    // Based on the DBZ-3113 the code can change in the future and it will be handled not in MySQL
    // but in the core shared code.
    private final ChangeEventQueue<DataChangeEvent> queue;

    public MySqlChangeEventSourceFactory(MySqlConnectorConfig configuration, MySqlConnection connection,
                                         ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock, MySqlDatabaseSchema schema,
                                         MySqlTaskContext taskContext, MySqlStreamingChangeEventSourceMetrics streamingMetrics,
                                         ChangeEventQueue<DataChangeEvent> queue) {
        this.configuration = configuration;
        this.connection = connection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
        this.queue = queue;
    }

    @Override
    public SnapshotChangeEventSource getSnapshotChangeEventSource(OffsetContext offsetContext, SnapshotProgressListener snapshotProgressListener) {
        return new MySqlSnapshotChangeEventSource(configuration, (MySqlOffsetContext) offsetContext, connection, taskContext.getSchema(), dispatcher, clock,
                (MySqlSnapshotChangeEventSourceMetrics) snapshotProgressListener, record -> modifyAndFlushLastRecord(record));
    }

    private void modifyAndFlushLastRecord(Function<SourceRecord, SourceRecord> modify) throws InterruptedException {
        queue.flushBuffer(dataChange -> new DataChangeEvent(modify.apply(dataChange.getRecord())));
        queue.disableBuffering();
    }

    @Override
    public StreamingChangeEventSource getStreamingChangeEventSource(OffsetContext offsetContext) {
        queue.disableBuffering();
        return new MySqlStreamingChangeEventSource(
                configuration,
                (MySqlOffsetContext) offsetContext,
                connection,
                dispatcher,
                errorHandler,
                clock,
                taskContext,
                streamingMetrics);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.Optional;

import io.debezium.config.Configuration;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

public class OracleChangeEventSourceFactory implements ChangeEventSourceFactory<OraclePartition, OracleOffsetContext> {

    private final OracleConnectorConfig configuration;
    private final MainConnectionProvidingConnectionFactory<OracleConnection> connectionFactory;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<OraclePartition, TableId> dispatcher;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final Configuration jdbcConfig;
    private final OracleTaskContext taskContext;
    private final AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics;

    public OracleChangeEventSourceFactory(OracleConnectorConfig configuration, MainConnectionProvidingConnectionFactory<OracleConnection> connectionFactory,
                                          ErrorHandler errorHandler, EventDispatcher<OraclePartition, TableId> dispatcher, Clock clock, OracleDatabaseSchema schema,
                                          Configuration jdbcConfig, OracleTaskContext taskContext,
                                          AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.jdbcConfig = jdbcConfig;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public SnapshotChangeEventSource<OraclePartition, OracleOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<OraclePartition> snapshotProgressListener,
                                                                                                        NotificationService<OraclePartition, OracleOffsetContext> notificationService) {
        return new OracleSnapshotChangeEventSource(configuration, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService);
    }

    @Override
    public StreamingChangeEventSource<OraclePartition, OracleOffsetContext> getStreamingChangeEventSource() {
        return configuration.getAdapter().getSource(
                connectionFactory.mainConnection(),
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext,
                jdbcConfig,
                streamingMetrics);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<OraclePartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                               OracleOffsetContext offsetContext,
                                                                                                                                               SnapshotProgressListener<OraclePartition> snapshotProgressListener,
                                                                                                                                               DataChangeEventListener<OraclePartition> dataChangeEventListener,
                                                                                                                                               NotificationService<OraclePartition, OracleOffsetContext> notificationService) {
        // If no data collection id is provided, don't return an instance as the implementation requires
        // that a signal data collection id be provided to work.
        if (Strings.isNullOrEmpty(configuration.getSignalingDataCollectionId())) {
            return Optional.empty();
        }

        // Incremental snapshots requires a secondary database connection
        // This is because Xstream does not allow any work on the connection while the LCR handler may be invoked
        // and LogMiner streams results from the CDB$ROOT container but we will need to stream changes from the
        // PDB when reading snapshot records.
        return Optional.of(new OracleSignalBasedIncrementalSnapshotChangeEventSource(
                configuration,
                new OracleConnection(connectionFactory.mainConnection().config()),
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener,
                notificationService));
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.oracle.AbstractStreamingAdapter;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
import io.debezium.document.Document;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.Clock;

/**
 * The streaming adapter for OpenLogReplicator.
 *
 * @author Chris Cranford
 */
public class OpenLogReplicatorAdapter extends AbstractStreamingAdapter<OpenLogReplicatorStreamingChangeEventSourceMetrics> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLogReplicatorAdapter.class);
    private static final String TYPE = "olr";

    public OpenLogReplicatorAdapter(OracleConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                return resolveScn(recorded).compareTo(resolveScn(desired)) < 1;
            }
        };
    }

    @Override
    public OffsetContext.Loader<OracleOffsetContext> getOffsetContextLoader() {
        return new OpenLogReplicatorOracleOffsetContextLoader(connectorConfig);
    }

    @Override
    public StreamingChangeEventSource<OraclePartition, OracleOffsetContext> getSource(OracleConnection connection,
                                                                                      EventDispatcher<OraclePartition, TableId> dispatcher,
                                                                                      ErrorHandler errorHandler,
                                                                                      Clock clock,
                                                                                      OracleDatabaseSchema schema,
                                                                                      OracleTaskContext taskContext,
                                                                                      Configuration jdbcConfig,
                                                                                      OpenLogReplicatorStreamingChangeEventSourceMetrics streamingMetrics) {
        return new OpenLogReplicatorStreamingChangeEventSource(
                connectorConfig,
                connection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                streamingMetrics);
    }

    @Override
    public OpenLogReplicatorStreamingChangeEventSourceMetrics getStreamingMetrics(OracleTaskContext taskContext,
                                                                                  ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                  EventMetadataProvider metadataProvider,
                                                                                  OracleConnectorConfig connectorConfig) {
        return new OpenLogReplicatorStreamingChangeEventSourceMetrics(taskContext, changeEventQueueMetrics, metadataProvider);
    }

    @Override
    public OracleOffsetContext determineSnapshotOffset(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> ctx,
                                                       OracleConnectorConfig connectorConfig, OracleConnection connection)
            throws SQLException {
        final Optional<Scn> latestTableDdlScn = getLatestTableDdlScn(ctx, connection);

        // we must use an SCN for taking the snapshot that represents a later timestamp than the latest DDL change than
        // any of the captured tables; this will not be a problem in practice, but during testing it may happen that the
        // SCN of "now" represents the same timestamp as a newly created table that should be captured; in that case
        // we'd get a ORA-01466 when running the flashback query for doing the snapshot
        Scn currentScn = null;
        do {
            currentScn = connection.getCurrentScn();
        } while (areSameTimestamp(latestTableDdlScn.orElse(null), currentScn, connection));

        LOGGER.info("\tCurrent SCN resolved as {}", currentScn);

        return OracleOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(currentScn)
                .snapshotScn(currentScn)
                .snapshotPendingTransactions(Collections.emptyMap())
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>())
                .build();
    }

    @Override
    public OracleValueConverters getValueConverter(OracleConnectorConfig connectorConfig, OracleConnection connection) {
        return new OpenLogReplicatorValueConverter(connectorConfig, connection);
    }

    @Override
    public OracleOffsetContext copyOffset(OracleConnectorConfig connectorConfig, OracleOffsetContext offsetContext) {
        return new OpenLogReplicatorOracleOffsetContextLoader(connectorConfig).load(offsetContext.getOffset());
    }
}

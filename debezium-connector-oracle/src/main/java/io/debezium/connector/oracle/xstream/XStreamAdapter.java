/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

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
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.SourceInfo;
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
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

import oracle.streams.StreamsException;
import oracle.streams.XStreamUtility;

/**
 * The streaming adapter implementation for Oracle XStream.
 *
 * @author Chris Cranford
 */
public class XStreamAdapter extends AbstractStreamingAdapter<XStreamStreamingChangeEventSourceMetrics> {

    private static final Logger LOGGER = LoggerFactory.getLogger(XStreamAdapter.class);

    public static final String TYPE = "xstream";

    public XStreamAdapter(OracleConnectorConfig connectorConfig) {
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
            public boolean isPositionAtOrBefore(Document recorded, Document desired) {
                final LcrPosition recordedPosition = LcrPosition.valueOf(recorded.getString(SourceInfo.LCR_POSITION_KEY));
                final LcrPosition desiredPosition = LcrPosition.valueOf(desired.getString(SourceInfo.LCR_POSITION_KEY));
                final Scn recordedScn = recordedPosition != null ? recordedPosition.getScn() : resolveScn(recorded);
                final Scn desiredScn = desiredPosition != null ? desiredPosition.getScn() : resolveScn(desired);
                if (recordedPosition != null && desiredPosition != null) {
                    return recordedPosition.compareTo(desiredPosition) < 1;
                }
                return recordedScn.compareTo(desiredScn) < 1;
            }
        };
    }

    @Override
    public OffsetContext.Loader<OracleOffsetContext> getOffsetContextLoader() {
        return new XStreamOracleOffsetContextLoader(connectorConfig);
    }

    @Override
    public StreamingChangeEventSource<OraclePartition, OracleOffsetContext> getSource(OracleConnection connection,
                                                                                      EventDispatcher<OraclePartition, TableId> dispatcher,
                                                                                      ErrorHandler errorHandler,
                                                                                      Clock clock,
                                                                                      OracleDatabaseSchema schema,
                                                                                      OracleTaskContext taskContext,
                                                                                      Configuration jdbcConfig,
                                                                                      XStreamStreamingChangeEventSourceMetrics streamingMetrics,
                                                                                      SnapshotterService snapshotterService) {
        return new XstreamStreamingChangeEventSource(
                connectorConfig,
                connection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                streamingMetrics);
    }

    @Override
    public XStreamStreamingChangeEventSourceMetrics getStreamingMetrics(OracleTaskContext taskContext,
                                                                        ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                        EventMetadataProvider metadataProvider,
                                                                        OracleConnectorConfig connectorConfig) {
        return new XStreamStreamingChangeEventSourceMetrics(taskContext, changeEventQueueMetrics, metadataProvider);
    }

    @Override
    public TableNameCaseSensitivity getTableNameCaseSensitivity(OracleConnection connection) {
        // Always use tablename case insensitivity true when on Oracle 11, otherwise false.
        if (connection.getOracleVersion().getMajor() == 11) {
            return TableNameCaseSensitivity.SENSITIVE;
        }
        return super.getTableNameCaseSensitivity(connection);
    }

    @Override
    public OracleOffsetContext determineSnapshotOffset(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> ctx,
                                                       OracleConnectorConfig connectorConfig,
                                                       OracleConnection connection)
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
    public Scn getOffsetScn(OracleOffsetContext offsetContext) {

        final byte[] startPosition;
        String lcrPosition = offsetContext.getLcrPosition();
        if (lcrPosition != null) {
            startPosition = LcrPosition.valueOf(lcrPosition).getRawPosition();
            getScn(startPosition);
        }
        return offsetContext.getScn();
    }

    private static void getScn(byte[] startPosition) {
        try {
            XStreamUtility.getSCNFromPosition(startPosition);
        }
        catch (StreamsException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OracleOffsetContext copyOffset(OracleConnectorConfig connectorConfig, OracleOffsetContext offsetContext) {
        return new XStreamOracleOffsetContextLoader(connectorConfig).load(offsetContext.getOffset());
    }

}

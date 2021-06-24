/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.AbstractStreamingAdapter;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.document.Document;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.Clock;

/**
 * The streaming adapter implementation for Oracle XStream.
 *
 * @author Chris Cranford
 */
public class XStreamAdapter extends AbstractStreamingAdapter {

    private static final String TYPE = "xstream";

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
    public StreamingChangeEventSource<OracleOffsetContext> getSource(OracleConnection connection,
                                                                     EventDispatcher<TableId> dispatcher,
                                                                     ErrorHandler errorHandler,
                                                                     Clock clock,
                                                                     OracleDatabaseSchema schema,
                                                                     OracleTaskContext taskContext,
                                                                     Configuration jdbcConfig,
                                                                     OracleStreamingChangeEventSourceMetrics streamingMetrics) {
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
    public TableNameCaseSensitivity getTableNameCaseSensitivity(OracleConnection connection) {
        // Always use tablename case insensitivity true when on Oracle 11, otherwise false.
        if (connection.getOracleVersion().getMajor() == 11) {
            return TableNameCaseSensitivity.SENSITIVE;
        }
        return super.getTableNameCaseSensitivity(connection);
    }
}

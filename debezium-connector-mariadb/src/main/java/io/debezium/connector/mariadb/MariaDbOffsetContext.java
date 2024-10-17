/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static io.debezium.connector.common.OffsetUtils.longOffsetValue;

import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.SnapshotType;
import io.debezium.connector.binlog.BinlogOffsetContext;
import io.debezium.connector.binlog.BinlogSourceInfo;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;

/**
 * Context that manages the connector offset details.
 *
 * @author Chris Cranford
 */
public class MariaDbOffsetContext extends BinlogOffsetContext<SourceInfo> {

    public MariaDbOffsetContext(SnapshotType snapshot, boolean snapshotCompleted, TransactionContext transactionContext,
                                IncrementalSnapshotContext<TableId> incrementalSnapshotContext, SourceInfo sourceInfo) {
        super(snapshot, snapshotCompleted, transactionContext, incrementalSnapshotContext, sourceInfo);
    }

    public static MariaDbOffsetContext initial(MariaDbConnectorConfig config) {
        final MariaDbOffsetContext offset = new MariaDbOffsetContext(
                null,
                false,
                new TransactionContext(),
                config.isReadOnlyConnection()
                        ? new MariaDbReadOnlyIncrementalSnapshotContext<>()
                        : new SignalBasedIncrementalSnapshotContext<>(),
                new SourceInfo(config));
        offset.setBinlogStartPoint("", 0L);
        return offset;
    }

    public static class Loader extends BinlogOffsetContext.Loader<MariaDbOffsetContext> {
        private final MariaDbConnectorConfig connectorConfig;

        public Loader(MariaDbConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public MariaDbOffsetContext load(Map<String, ?> offset) {
            final String binlogFilename = (String) offset.get(BinlogSourceInfo.BINLOG_FILENAME_OFFSET_KEY);
            if (binlogFilename == null) {
                throw new ConnectException("Source offset '" + BinlogSourceInfo.BINLOG_FILENAME_OFFSET_KEY + "' parameter is missing");
            }
            final long binlogPosition = longOffsetValue(offset, BinlogSourceInfo.BINLOG_POSITION_OFFSET_KEY);

            final MariaDbOffsetContext offsetContext = new MariaDbOffsetContext(
                    loadSnapshot((Map<String, Object>) offset),
                    isTrue(offset, SNAPSHOT_COMPLETED_KEY),
                    TransactionContext.load(offset),
                    connectorConfig.isReadOnlyConnection()
                            ? MariaDbReadOnlyIncrementalSnapshotContext.load(offset)
                            : SignalBasedIncrementalSnapshotContext.load(offset),
                    new SourceInfo(connectorConfig));
            offsetContext.setBinlogStartPoint(binlogFilename, binlogPosition);
            offsetContext.setInitialSkips(longOffsetValue(offset, EVENTS_TO_SKIP_OFFSET_KEY),
                    (int) longOffsetValue(offset, BinlogSourceInfo.BINLOG_ROW_IN_EVENT_OFFSET_KEY));
            offsetContext.setCompletedGtidSet((String) offset.get(GTID_SET_KEY)); // may be null
            return offsetContext;
        }
    }
}

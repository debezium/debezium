/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.connector.common.OffsetUtils.longOffsetValue;

import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.SnapshotType;
import io.debezium.connector.binlog.BinlogOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;

public class MySqlOffsetContext extends BinlogOffsetContext<SourceInfo> {

    public MySqlOffsetContext(SnapshotType snapshot, boolean snapshotCompleted, TransactionContext transactionContext,
                              IncrementalSnapshotContext<TableId> incrementalSnapshotContext, SourceInfo sourceInfo) {
        super(snapshot, snapshotCompleted, transactionContext, incrementalSnapshotContext, sourceInfo);
    }

    public static MySqlOffsetContext initial(MySqlConnectorConfig config) {
        final MySqlOffsetContext offset = new MySqlOffsetContext(
                null,
                false,
                new TransactionContext(),
                config.isReadOnlyConnection()
                        ? new MySqlReadOnlyIncrementalSnapshotContext<>()
                        : new SignalBasedIncrementalSnapshotContext<>(),
                new SourceInfo(config));
        offset.setBinlogStartPoint("", 0L); // start from the beginning of the binlog
        return offset;
    }

    public static class Loader extends BinlogOffsetContext.Loader<MySqlOffsetContext> {

        private final MySqlConnectorConfig connectorConfig;

        public Loader(MySqlConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public MySqlOffsetContext load(Map<String, ?> offset) {
            final String binlogFilename = (String) offset.get(SourceInfo.BINLOG_FILENAME_OFFSET_KEY);
            if (binlogFilename == null) {
                throw new ConnectException("Source offset '" + SourceInfo.BINLOG_FILENAME_OFFSET_KEY + "' parameter is missing");
            }
            long binlogPosition = longOffsetValue(offset, SourceInfo.BINLOG_POSITION_OFFSET_KEY);
            final MySqlOffsetContext offsetContext = new MySqlOffsetContext(
                    loadSnapshot(offset).orElse(null),
                    loadSnapshotCompleted(offset),
                    TransactionContext.load(offset),
                    connectorConfig.isReadOnlyConnection()
                            ? MySqlReadOnlyIncrementalSnapshotContext.load(offset)
                            : SignalBasedIncrementalSnapshotContext.load(offset),
                    new SourceInfo(connectorConfig));
            offsetContext.setBinlogStartPoint(binlogFilename, binlogPosition);
            offsetContext.setInitialSkips(longOffsetValue(offset, EVENTS_TO_SKIP_OFFSET_KEY),
                    (int) longOffsetValue(offset, SourceInfo.BINLOG_ROW_IN_EVENT_OFFSET_KEY));
            offsetContext.setCompletedGtidSet((String) offset.get(GTID_SET_KEY)); // may be null
            return offsetContext;
        }
    }
}

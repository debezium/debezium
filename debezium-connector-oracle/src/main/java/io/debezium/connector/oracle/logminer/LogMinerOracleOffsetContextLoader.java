/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.util.Map;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;

/**
 * @author Chris Cranford
 */
public class LogMinerOracleOffsetContextLoader implements OffsetContext.Loader<OracleOffsetContext> {

    private final OracleConnectorConfig connectorConfig;

    public LogMinerOracleOffsetContextLoader(OracleConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public OracleOffsetContext load(Map<String, ?> offset) {
        boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
        boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(OracleOffsetContext.SNAPSHOT_COMPLETED_KEY));

        Scn scn = OracleOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        Scn commitScn = OracleOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.COMMIT_SCN_KEY);
        Map<String, Scn> snapshotPendingTransactions = OracleOffsetContext.loadSnapshotPendingTransactions(offset);
        Scn snapshotScn = OracleOffsetContext.loadSnapshotScn(offset);
        return new OracleOffsetContext(connectorConfig, scn, commitScn, null, snapshotScn, snapshotPendingTransactions, snapshot, snapshotCompleted,
                TransactionContext.load(offset),
                SignalBasedIncrementalSnapshotContext.load(offset));
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.util.Map;

import io.debezium.connector.SnapshotType;
import io.debezium.connector.oracle.CommitScn;
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
        final SnapshotType snapshot = loadSnapshot(offset).orElse(null);
        final boolean snapshotCompleted = loadSnapshotCompleted(offset);

        Scn scn = OracleOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        CommitScn commitScn = CommitScn.load(offset);
        Map<String, Scn> snapshotPendingTransactions = OracleOffsetContext.loadSnapshotPendingTransactions(offset);
        Scn snapshotScn = OracleOffsetContext.loadSnapshotScn(offset);

        return new OracleOffsetContext(connectorConfig, scn, null, commitScn, null, snapshotScn,
                snapshotPendingTransactions, snapshot, snapshotCompleted,
                TransactionContext.load(offset),
                SignalBasedIncrementalSnapshotContext.load(offset));
    }

}

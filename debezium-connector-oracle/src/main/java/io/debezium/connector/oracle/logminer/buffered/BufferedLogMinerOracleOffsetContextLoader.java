/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.util.Map;

import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;

/**
 * @author Chris Cranford
 */
public class BufferedLogMinerOracleOffsetContextLoader implements OffsetContext.Loader<OracleOffsetContext> {

    private final OracleConnectorConfig connectorConfig;

    public BufferedLogMinerOracleOffsetContextLoader(OracleConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public OracleOffsetContext load(Map<String, ?> offset) {
        return OracleOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(OracleOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY))
                .commitScn(CommitScn.load(offset))
                .snapshotScn(OracleOffsetContext.loadSnapshotScn(offset))
                .snapshotPendingTransactions(OracleOffsetContext.loadSnapshotPendingTransactions(offset))
                .snapshot(loadSnapshot(offset).orElse(null))
                .snapshotCompleted(loadSnapshotCompleted(offset))
                .transactionContext(TransactionContext.load(offset))
                .transactionId(OracleOffsetContext.loadTransactionId(offset))
                .transactionSequence(OracleOffsetContext.loadTransactionSequence(offset))
                .incrementalSnapshotContext(SignalBasedIncrementalSnapshotContext.load(offset))
                .build();
    }

}

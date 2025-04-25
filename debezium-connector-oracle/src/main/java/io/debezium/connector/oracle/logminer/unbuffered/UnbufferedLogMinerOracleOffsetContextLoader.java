/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.unbuffered;

import java.util.Map;

import io.debezium.common.annotation.Incubating;
import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;

/**
 * An {@link OffsetContext.Loader} implementation for the unbuffered Oracle LogMiner adapter.
 *
 * @author Chris Cranford
 */
@Incubating
public class UnbufferedLogMinerOracleOffsetContextLoader implements OffsetContext.Loader<OracleOffsetContext> {

    private final OracleConnectorConfig connectorConfig;

    public UnbufferedLogMinerOracleOffsetContextLoader(OracleConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public OracleOffsetContext load(Map<String, ?> offset) {
        return new OracleOffsetContext(
                connectorConfig,
                OracleOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY),
                null,
                CommitScn.load(offset),
                null,
                OracleOffsetContext.loadSnapshotScn(offset),
                OracleOffsetContext.loadSnapshotPendingTransactions(offset),
                loadSnapshot(offset).orElse(null),
                loadSnapshotCompleted(offset),
                TransactionContext.load(offset),
                SignalBasedIncrementalSnapshotContext.load(offset),
                OracleOffsetContext.loadTransactionId(offset),
                OracleOffsetContext.loadTransactionSequence(offset));
    }
}

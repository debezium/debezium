/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import java.util.Map;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;

/**
 * The {@link OffsetContext} loader implementation for the Oracle XStream adapter
 *
 * @author Chris Cranford
 */
public class XStreamOracleOffsetContextLoader implements OffsetContext.Loader<OracleOffsetContext> {

    private final OracleConnectorConfig connectorConfig;

    public XStreamOracleOffsetContextLoader(OracleConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public OracleOffsetContext load(Map<String, ?> offset) {
        return OracleOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(resolveScn(offset))
                .lcrPosition(loadLcrPosition(offset))
                .snapshotScn(OracleOffsetContext.loadSnapshotScn(offset))
                .snapshotPendingTransactions(OracleOffsetContext.loadSnapshotPendingTransactions(offset))
                .snapshot(loadSnapshot(offset).orElse(null))
                .snapshotCompleted(loadSnapshotCompleted(offset))
                .transactionContext(TransactionContext.load(offset))
                .incrementalSnapshotContext(SignalBasedIncrementalSnapshotContext.load(offset))
                .build();
    }

    private Scn resolveScn(Map<String, ?> offset) {
        final String lcrPosition = loadLcrPosition(offset);
        return lcrPosition != null
                ? LcrPosition.valueOf(lcrPosition).getScn()
                : OracleOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
    }

    private String loadLcrPosition(Map<String, ?> offset) {
        return (String) offset.get(SourceInfo.LCR_POSITION_KEY);
    }
}

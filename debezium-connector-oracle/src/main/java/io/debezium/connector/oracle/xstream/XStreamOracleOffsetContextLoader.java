/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import java.util.Map;

import io.debezium.connector.SnapshotType;
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
        final SnapshotType snapshot = loadSnapshot(offset).orElse(null);
        boolean snapshotCompleted = loadSnapshotCompleted(offset);

        String lcrPosition = (String) offset.get(SourceInfo.LCR_POSITION_KEY);

        final Scn scn;
        if (lcrPosition != null) {
            scn = LcrPosition.valueOf(lcrPosition).getScn();
        }
        else {
            scn = OracleOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        }

        final Map<String, Scn> snapshotPendingTransactions = OracleOffsetContext.loadSnapshotPendingTransactions(offset);
        final Scn snapshotScn = OracleOffsetContext.loadSnapshotScn(offset);
        return new OracleOffsetContext(connectorConfig, scn, null, lcrPosition, snapshotScn, snapshotPendingTransactions,
                snapshot, snapshotCompleted, TransactionContext.load(offset), SignalBasedIncrementalSnapshotContext.load(offset),
                null, null);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import java.util.Map;

import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;

/**
 * The {@link OffsetContext} loader implementation for OpenLogReplicator.
 *
 * @author Chris Cranford
 */
public class OpenLogReplicatorOracleOffsetContextLoader implements OffsetContext.Loader<OracleOffsetContext> {

    private final OracleConnectorConfig connectorConfig;

    public OpenLogReplicatorOracleOffsetContextLoader(OracleConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public OracleOffsetContext load(Map<String, ?> offset) {
        boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
        boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(OracleOffsetContext.SNAPSHOT_COMPLETED_KEY));

        Scn scn = OracleOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        Long scnIndex = (Long) offset.get(SourceInfo.SCN_INDEX_KEY);
        CommitScn commitScn = CommitScn.valueOf((String) null);
        return new OracleOffsetContext(connectorConfig, scn, scnIndex, commitScn, null, null, null,
                snapshot, snapshotCompleted,
                TransactionContext.load(offset),
                SignalBasedIncrementalSnapshotContext.load(offset));
    }

}

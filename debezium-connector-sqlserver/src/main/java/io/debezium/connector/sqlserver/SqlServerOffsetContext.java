/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Collect;

public class SqlServerOffsetContext implements OffsetContext {

    private static final String SERVER_PARTITION_KEY = "server";
    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    private final Schema sourceInfoSchema;
    private final SourceInfo sourceInfo;
    private final Map<String, String> partition;
    private boolean snapshotCompleted;

    public SqlServerOffsetContext(String serverName, Lsn lsn, boolean snapshot, boolean snapshotCompleted) {
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, serverName);
        sourceInfo = new SourceInfo(serverName);

        sourceInfo.setChangeLsn(lsn);
        sourceInfoSchema = sourceInfo.schema();

        this.snapshotCompleted = snapshotCompleted;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot);
        }
    }

    @Override
    public Map<String, ?> getPartition() {
        return partition;
    }

    @Override
    public Map<String, ?> getOffset() {
        if (sourceInfo.isSnapshot()) {
            return Collect.hashMapOf(
                    SourceInfo.SNAPSHOT_KEY, true,
                    SNAPSHOT_COMPLETED_KEY, snapshotCompleted,
                    SourceInfo.CHANGE_LSN_KEY, sourceInfo.getChangeLsn().toString()
            );
        }
        else {
            return Collections.singletonMap(SourceInfo.CHANGE_LSN_KEY, sourceInfo.getChangeLsn().toString());
        }
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    public void setChangeLsn(Lsn lsn) {
        sourceInfo.setChangeLsn(lsn);
    }

    public Lsn getChangeLsn() {
        return sourceInfo.getChangeLsn() == null ? Lsn.NULL : sourceInfo.getChangeLsn();
    }

    public void setCommitLsn(Lsn lsn) {
        sourceInfo.setCommitLsn(lsn);
    }

    public void setSourceTime(Instant instant) {
        sourceInfo.setSourceTime(instant);
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot() && !snapshotCompleted;
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(true);
        snapshotCompleted = false;
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(false);
    }

    public static class Loader implements OffsetContext.Loader {

        private final String logicalName;

        public Loader(String logicalName) {
            this.logicalName = logicalName;
        }

        @Override
        public Map<String, ?> getPartition() {
            return Collections.singletonMap(SERVER_PARTITION_KEY, logicalName);
        }

        @Override
        public OffsetContext load(Map<String, ?> offset) {
            final Lsn lsn = Lsn.valueOf((String)offset.get(SourceInfo.CHANGE_LSN_KEY));
            boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
            boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY));

            return new SqlServerOffsetContext(logicalName, lsn, snapshot, snapshotCompleted);
        }
    }

    @Override
    public String toString() {
        return "SqlServerOffsetContext [" +
                "sourceInfoSchema=" + sourceInfoSchema +
                ", sourceInfo=" + sourceInfo +
                ", partition=" + partition +
                ", snapshotCompleted=" + snapshotCompleted +
                "]";
    }
}

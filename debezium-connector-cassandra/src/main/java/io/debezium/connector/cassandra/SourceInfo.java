/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.time.Conversions;

/**
 * Metadata about the source of the change event
 */
public class SourceInfo extends AbstractSourceInfo {
    public static final String CLUSTER_KEY = "cluster";
    public static final String COMMITLOG_FILENAME_KEY = "file";
    public static final String COMMITLOG_POSITION_KEY = "pos";
    public static final String KEYSPACE_NAME_KEY = "keyspace";
    public static final String TABLE_NAME_KEY = "table";
    public static final String SNAPSHOT_KEY = "snapshot";
    public static final String TIMESTAMP_KEY = "ts_micro";

    public final String version = Module.version();
    public final String connector = Module.name();
    public String cluster;
    public OffsetPosition offsetPosition;
    public KeyspaceTable keyspaceTable;
    public boolean snapshot;
    public Instant tsMicro;

    public SourceInfo(CommonConnectorConfig config, String cluster, OffsetPosition offsetPosition,
                      KeyspaceTable keyspaceTable, boolean snapshot, Instant tsMicro) {
        super(config);
        this.cluster = cluster;
        this.offsetPosition = offsetPosition;
        this.keyspaceTable = keyspaceTable;
        this.tsMicro = tsMicro;
        this.snapshot = snapshot;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SourceInfo that = (SourceInfo) o;
        return version.equals(that.version)
                && connector.equals(that.connector)
                && cluster.equals(that.cluster)
                && offsetPosition == that.offsetPosition
                && snapshot == that.snapshot
                && keyspaceTable == that.keyspaceTable
                && this.tsMicroInLong() == that.tsMicroInLong();
    }

    @Override
    public int hashCode() {
        return Objects.hash(cluster, snapshot, offsetPosition, keyspaceTable, tsMicroInLong());
    }

    @Override
    public String toString() {
        Map<String, Object> map = new HashMap<>();
        map.put(DEBEZIUM_VERSION_KEY, version);
        map.put(DEBEZIUM_CONNECTOR_KEY, connector);
        map.put(CLUSTER_KEY, cluster);
        map.put(SNAPSHOT_KEY, snapshot);
        map.put(COMMITLOG_FILENAME_KEY, offsetPosition.fileName);
        map.put(COMMITLOG_POSITION_KEY, offsetPosition.filePosition);
        map.put(KEYSPACE_NAME_KEY, keyspaceTable.keyspace);
        map.put(TABLE_NAME_KEY, keyspaceTable.table);
        map.put(TIMESTAMP_KEY, tsMicroInLong());
        return map.toString();
    }

    @Override
    protected Instant timestamp() {
        return tsMicro;
    }

    protected long tsMicroInLong() {
        return Conversions.toEpochMicros(tsMicro);
    }

    @Override
    protected SnapshotRecord snapshot() {
        return snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE;
    }

    @Override
    protected String database() {
        // Set the database field in SourceInfo to be "NULL" because Cassandra doesn't have the conception of database.
        return "NULL";
    }
}

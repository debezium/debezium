/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Metadata about the source of the change event
 */
public class SourceInfo {
    public static final String DEBEZIUM_VERSION_KEY = "version";
    public static final String DEBEZIUM_CONNECTOR_KEY = "connector";
    public static final String CLUSTER_KEY = "cluster";
    public static final String COMMITLOG_FILENAME_KEY = "file";
    public static final String COMMITLOG_POSITION_KEY = "pos";
    public static final String KEYSPACE_NAME_KEY = "keyspace";
    public static final String TABLE_NAME_KEY = "table";
    public static final String SNAPSHOT_KEY = "snapshot";
    public static final String TIMESTAMP_KEY = "ts_micro";

    public static final Schema SOURCE_SCHEMA = SchemaBuilder.struct().name(Record.SOURCE)
            .field(CLUSTER_KEY, Schema.STRING_SCHEMA)
            .field(COMMITLOG_FILENAME_KEY, Schema.STRING_SCHEMA)
            .field(COMMITLOG_POSITION_KEY, Schema.INT32_SCHEMA)
            .field(SNAPSHOT_KEY, Schema.BOOLEAN_SCHEMA)
            .field(KEYSPACE_NAME_KEY, Schema.STRING_SCHEMA)
            .field(TABLE_NAME_KEY, Schema.STRING_SCHEMA)
            .field(TIMESTAMP_KEY, CassandraTypeKafkaSchemaBuilders.TIMESTAMP_MICRO_TYPE)
            .build();

    public final String version = Module.version();
    public final String connector = Module.name();
    public final String cluster;
    public final OffsetPosition offsetPosition;
    public final KeyspaceTable keyspaceTable;
    public final boolean snapshot;
    public final long tsMicro;

    public SourceInfo(String cluster, OffsetPosition offsetPosition, KeyspaceTable keyspaceTable, boolean snapshot, long tsMicro) {
        this.cluster = cluster;
        this.offsetPosition = offsetPosition;
        this.keyspaceTable = keyspaceTable;
        this.tsMicro = tsMicro;
        this.snapshot = snapshot;
    }

    public Struct record(Schema schema) {
        return new Struct(schema)
                .put(CLUSTER_KEY, cluster)
                .put(COMMITLOG_FILENAME_KEY, offsetPosition.fileName)
                .put(COMMITLOG_POSITION_KEY, offsetPosition.filePosition)
                .put(SNAPSHOT_KEY, snapshot)
                .put(KEYSPACE_NAME_KEY, keyspaceTable.keyspace)
                .put(TABLE_NAME_KEY, keyspaceTable.table)
                .put(TIMESTAMP_KEY, tsMicro);

    }

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
                && tsMicro == that.tsMicro;
    }

    public int hashCode() {
        return Objects.hash(cluster, snapshot, offsetPosition, keyspaceTable, tsMicro);
    }

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
        map.put(TIMESTAMP_KEY, tsMicro);
        return map.toString();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import io.debezium.connector.cassandra.transforms.CassandraTypeToAvroSchemaMapper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.debezium.connector.cassandra.Record.SOURCE;

/**
 * Metadata about the source of the change event
 */
public class SourceInfo implements AvroRecord {
    public static final String CLUSTER_KEY = "cluster";
    public static final String COMMITLOG_FILENAME_KEY = "file";
    public static final String COMMITLOG_POSITION_KEY = "pos";
    public static final String KEYSPACE_NAME_KEY = "keyspace";
    public static final String TABLE_NAME_KEY = "table";
    public static final String COMMITLOG_SNAPSHOT_KEY = "snapshot";
    public static final String TIMESTAMP_MICRO_KEY = "ts_micro";

    public static final Schema SOURCE_SCHEMA = SchemaBuilder.builder().record(SOURCE).fields()
            .optionalString(CLUSTER_KEY)
            .optionalString(COMMITLOG_FILENAME_KEY)
            .optionalInt(COMMITLOG_POSITION_KEY)
            .optionalBoolean(COMMITLOG_SNAPSHOT_KEY)
            .optionalString(KEYSPACE_NAME_KEY)
            .optionalString(TABLE_NAME_KEY)
            .name(TIMESTAMP_MICRO_KEY).type(CassandraTypeToAvroSchemaMapper.nullable(CassandraTypeToAvroSchemaMapper.TIMESTAMP_MICRO_TYPE)).withDefault(null)
            .endRecord();

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

    @Override
    public GenericRecord record(Schema schema) {
        return new GenericRecordBuilder(schema)
                .set(CLUSTER_KEY, cluster)
                .set(COMMITLOG_FILENAME_KEY, offsetPosition.fileName)
                .set(COMMITLOG_POSITION_KEY, offsetPosition.filePosition)
                .set(COMMITLOG_SNAPSHOT_KEY, snapshot)
                .set(KEYSPACE_NAME_KEY, keyspaceTable.keyspace)
                .set(TABLE_NAME_KEY, keyspaceTable.table)
                .set(TIMESTAMP_MICRO_KEY, tsMicro)
                .build();
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
        return cluster.equals(that.cluster)
                && snapshot == that.snapshot
                && offsetPosition == that.offsetPosition
                && keyspaceTable == that.keyspaceTable
                && tsMicro == that.tsMicro;
    }

    @Override
    public int hashCode() {
        return Objects.hash(cluster, snapshot, offsetPosition, keyspaceTable, tsMicro);
    }

    @Override
    public String toString() {
        Map<String, Object> map = new HashMap<>();
        map.put(CLUSTER_KEY, cluster);
        map.put(COMMITLOG_SNAPSHOT_KEY, snapshot);
        map.put(COMMITLOG_FILENAME_KEY, offsetPosition.fileName);
        map.put(COMMITLOG_POSITION_KEY, offsetPosition.filePosition);
        map.put(KEYSPACE_NAME_KEY, keyspaceTable.keyspace);
        map.put(TABLE_NAME_KEY, keyspaceTable.table);
        map.put(TIMESTAMP_MICRO_KEY, tsMicro);
        return map.toString();
    }
}

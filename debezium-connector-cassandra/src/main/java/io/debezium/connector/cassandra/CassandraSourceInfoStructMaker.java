/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;
import io.debezium.util.SchemaNameAdjuster;

public class CassandraSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private final Schema schema;

    public CassandraSourceInfoStructMaker(String connector, String version, CommonConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name(SchemaNameAdjuster.defaultAdjuster().adjust(Record.SOURCE))
                .field(SourceInfo.CLUSTER_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.COMMITLOG_FILENAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.COMMITLOG_POSITION_KEY, Schema.INT32_SCHEMA)
                .field(SourceInfo.KEYSPACE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        return super.commonStruct(sourceInfo)
                .put(SourceInfo.CLUSTER_KEY, sourceInfo.cluster)
                .put(SourceInfo.COMMITLOG_FILENAME_KEY, sourceInfo.offsetPosition.fileName)
                .put(SourceInfo.COMMITLOG_POSITION_KEY, sourceInfo.offsetPosition.filePosition)
                .put(SourceInfo.KEYSPACE_NAME_KEY, sourceInfo.keyspaceTable.keyspace)
                .put(SourceInfo.TABLE_NAME_KEY, sourceInfo.keyspaceTable.table);
    }
}

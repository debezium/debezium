/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.LegacyV1AbstractSourceInfoStructMaker;
import io.debezium.util.SchemaNameAdjuster;

public class LegacyV1MongoDbSourceInfoStructMaker extends LegacyV1AbstractSourceInfoStructMaker<SourceInfo> {

    private final Schema schema;
    private final String serverName;

    public LegacyV1MongoDbSourceInfoStructMaker(String connector, String version, CommonConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name(SchemaNameAdjuster.defaultAdjuster().adjust("io.debezium.connector.mongo.Source"))
                .version(SourceInfo.SCHEMA_VERSION)
                .field(SourceInfo.SERVER_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.REPLICA_SET_NAME, Schema.STRING_SCHEMA)
                .field(SourceInfo.NAMESPACE, Schema.STRING_SCHEMA)
                .field(SourceInfo.TIMESTAMP, Schema.INT32_SCHEMA)
                .field(SourceInfo.ORDER, Schema.INT32_SCHEMA)
                .field(SourceInfo.OPERATION_ID, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.INITIAL_SYNC, SchemaBuilder.bool().optional().defaultValue(false).build())
                .build();
        this.serverName = connectorConfig.getLogicalName();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final Struct result = super.commonStruct()
                .put(SourceInfo.SERVER_NAME_KEY, serverName)
                .put(SourceInfo.REPLICA_SET_NAME, sourceInfo.replicaSetName())
                .put(SourceInfo.NAMESPACE, sourceInfo.collectionId().namespace())
                .put(SourceInfo.TIMESTAMP, sourceInfo.position().getTime())
                .put(SourceInfo.ORDER, sourceInfo.position().getInc())
                .put(SourceInfo.OPERATION_ID, sourceInfo.position().getOperationId());
        if (sourceInfo.isInitialSyncOngoing(sourceInfo.replicaSetName())) {
            result.put(SourceInfo.INITIAL_SYNC, true);
        }
        return result;
    }
}

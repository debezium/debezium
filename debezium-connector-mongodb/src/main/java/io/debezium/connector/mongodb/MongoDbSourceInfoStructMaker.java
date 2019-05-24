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
import io.debezium.connector.AbstractSourceInfoStructMaker;
import io.debezium.util.SchemaNameAdjuster;

public class MongoDbSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    public static final String TIMESTAMP = "ts_ms";
    public static final String SNAPSHOT = "snapshot";
    public static final String DATABASE = "db";
    public static final String COLLECTION = "collection";

    private final Schema schema;

    public MongoDbSourceInfoStructMaker(String connector, String version, CommonConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
            .name(SchemaNameAdjuster.defaultAdjuster().adjust("io.debezium.connector.mongo.Source"))
            .field(TIMESTAMP, Schema.INT64_SCHEMA)
            .field(SNAPSHOT, SchemaBuilder.bool().optional().defaultValue(false).build())
            .field(DATABASE, Schema.STRING_SCHEMA)
            .field(COLLECTION, Schema.STRING_SCHEMA)
            .field(SourceInfo.REPLICA_SET_NAME, Schema.STRING_SCHEMA)
            .field(SourceInfo.ORDER, Schema.INT32_SCHEMA)
            .field(SourceInfo.OPERATION_ID, Schema.OPTIONAL_INT64_SCHEMA)
            .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final Struct result = super.commonStruct()
                .put(TIMESTAMP, (long) sourceInfo.getPosition().getTime() * 1_000)
                .put(DATABASE, sourceInfo.getCollectionId().dbName())
                .put(SourceInfo.REPLICA_SET_NAME, sourceInfo.getCollectionId().replicaSetName())
                .put(COLLECTION, sourceInfo.getCollectionId().name())
                .put(SourceInfo.ORDER, sourceInfo.getPosition().getInc())
                .put(SourceInfo.OPERATION_ID, sourceInfo.getPosition().getOperationId());
        if (sourceInfo.isInitialSyncOngoing(sourceInfo.getCollectionId().replicaSetName())) {
            result.put(SNAPSHOT, true);
        }
        return result;
    }
}

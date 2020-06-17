/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;
import io.debezium.util.SchemaNameAdjuster;

public class MongoDbSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private final Schema schema;

    public MongoDbSourceInfoStructMaker(String connector, String version, CommonConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name(SchemaNameAdjuster.defaultAdjuster().adjust("io.debezium.connector.mongo.Source"))
                .field(SourceInfo.REPLICA_SET_NAME, Schema.STRING_SCHEMA)
                .field(SourceInfo.COLLECTION, Schema.STRING_SCHEMA)
                .field(SourceInfo.ORDER, Schema.INT32_SCHEMA)
                .field(SourceInfo.OPERATION_ID, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.TX_ORD, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.SESSION_TXN_ID, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        Struct struct = super.commonStruct(sourceInfo)
                .put(SourceInfo.REPLICA_SET_NAME, sourceInfo.replicaSetName())
                .put(SourceInfo.COLLECTION, sourceInfo.collectionId().name())
                .put(SourceInfo.ORDER, sourceInfo.position().getInc())
                .put(SourceInfo.OPERATION_ID, sourceInfo.position().getOperationId())
                .put(SourceInfo.SESSION_TXN_ID, sourceInfo.position().getSessionTxnId());

        sourceInfo.transactionPosition().ifPresent(transactionPosition -> struct.put(SourceInfo.TX_ORD, transactionPosition));

        return struct;
    }
}

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

public class MongoDbSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private Schema schema;

    @Override
    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name(connectorConfig.schemaNameAdjuster().adjust("io.debezium.connector.mongo.Source"))
                .field(SourceInfo.COLLECTION, Schema.STRING_SCHEMA)
                .field(SourceInfo.ORDER, Schema.INT32_SCHEMA)
                .field(SourceInfo.LSID, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.TXN_NUMBER, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.WALL_TIME, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        Struct struct = super.commonStruct(sourceInfo)
                .put(SourceInfo.COLLECTION, sourceInfo.collectionId().name())
                .put(SourceInfo.ORDER, sourceInfo.position().getInc());

        if (sourceInfo.position().getChangeStreamSessionTxnId() != null) {
            struct.put(SourceInfo.LSID, sourceInfo.position().getChangeStreamSessionTxnId().lsid)
                    .put(SourceInfo.TXN_NUMBER, sourceInfo.position().getChangeStreamSessionTxnId().txnNumber);
        }

        if (sourceInfo.wallTime() != 0L) {
            struct.put(SourceInfo.WALL_TIME, sourceInfo.wallTime());
        }

        return struct;
    }
}

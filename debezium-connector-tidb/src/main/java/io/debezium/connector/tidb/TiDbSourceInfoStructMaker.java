/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.AbstractSourceInfoStructMaker;

/**
 * Builds the {@code source} struct of TiDB change events. Unlike the MySQL-flavoured dummies that
 * TiCDC's Debezium output mode fills in ({@code server_id=0}, {@code file=""}, {@code pos=0}), this
 * exposes the actual TiDB position: the TSO commit timestamp and the TiCDC cluster id.
 *
 * @author Aviral Srivastava
 */
public class TiDbSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private Schema schema;

    @Override
    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name(connectorConfig.schemaNameAdjuster().adjust("io.debezium.connector.tidb.Source"))
                .field(AbstractSourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.COMMIT_TS_KEY, Schema.INT64_SCHEMA)
                .field(SourceInfo.CLUSTER_ID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final Struct struct = super.commonStruct(sourceInfo)
                .put(AbstractSourceInfo.TABLE_NAME_KEY, sourceInfo.table() == null ? "" : sourceInfo.table())
                .put(SourceInfo.COMMIT_TS_KEY, sourceInfo.commitTs());

        if (sourceInfo.clusterId() != null) {
            struct.put(SourceInfo.CLUSTER_ID_KEY, sourceInfo.clusterId());
        }
        return struct;
    }
}

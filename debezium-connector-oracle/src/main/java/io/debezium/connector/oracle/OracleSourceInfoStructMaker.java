/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

public class OracleSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private final Schema schema;

    public OracleSourceInfoStructMaker(String connector, String version, CommonConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name("io.debezium.connector.oracle.Source")
                .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TXID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.SCN_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.COMMIT_SCN_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.LCR_POSITION_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final Struct ret = super.commonStruct(sourceInfo)
                .put(SourceInfo.SCHEMA_NAME_KEY, sourceInfo.getTableId().schema())
                .put(SourceInfo.TABLE_NAME_KEY, sourceInfo.getTableId().table())
                .put(SourceInfo.TXID_KEY, sourceInfo.getTransactionId())
                .put(SourceInfo.SCN_KEY, sourceInfo.getScn());

        if (sourceInfo.getLcrPosition() != null) {
            ret.put(SourceInfo.LCR_POSITION_KEY, sourceInfo.getLcrPosition().toString());
        }
        if (sourceInfo.getCommitScn() != null) {
            ret.put(SourceInfo.COMMIT_SCN_KEY, sourceInfo.getCommitScn());
        }
        return ret;
    }
}

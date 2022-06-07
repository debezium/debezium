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
                .field(SourceInfo.SCN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.COMMIT_SCN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.LCR_POSITION_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final String commitScn = sourceInfo.getCommitScn() == null ? null : sourceInfo.getCommitScn().toString();
        final String eventScn = sourceInfo.getEventScn() == null ? null : sourceInfo.getEventScn().toString();

        final Struct ret = super.commonStruct(sourceInfo)
                .put(SourceInfo.SCHEMA_NAME_KEY, sourceInfo.tableSchema())
                .put(SourceInfo.TABLE_NAME_KEY, sourceInfo.table())
                .put(SourceInfo.TXID_KEY, sourceInfo.getTransactionId())
                // While we reuse the same SCN_KEY here as the one in the connector's offsets, the value we
                // store here is the per-event SCN. The SCN value stored with this key in the offsets is
                // the resume SCN, which is where the connector will start from after a restart. The resume
                // SCN will always be <= to the SCN stored in the source info block here.
                .put(SourceInfo.SCN_KEY, eventScn);

        if (sourceInfo.getLcrPosition() != null) {
            ret.put(SourceInfo.LCR_POSITION_KEY, sourceInfo.getLcrPosition());
        }
        if (commitScn != null) {
            ret.put(SourceInfo.COMMIT_SCN_KEY, commitScn);
        }
        return ret;
    }
}

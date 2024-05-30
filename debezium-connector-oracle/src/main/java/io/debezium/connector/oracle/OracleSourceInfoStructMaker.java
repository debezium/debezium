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
import io.debezium.util.Strings;

public class OracleSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private Schema schema;

    @Override
    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);
        this.schema = CommitScn.schemaBuilder(commonSchemaBuilder()
                .name("io.debezium.connector.oracle.Source")
                .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TXID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.EVENT_SCN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.COMMIT_SCN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.LCR_POSITION_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(CommitScn.ROLLBACK_SEGMENT_ID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(CommitScn.SQL_SEQUENCE_NUMBER_KEY, Schema.OPTIONAL_INT64_SCHEMA))
                .field(SourceInfo.USERNAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.REDO_SQL, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.ROW_ID, Schema.OPTIONAL_STRING_SCHEMA).build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final String eventScn = sourceInfo.getEventScn() == null ? null : sourceInfo.getEventScn().toString();

        final Struct ret = super.commonStruct(sourceInfo)
                .put(SourceInfo.SCHEMA_NAME_KEY, sourceInfo.tableSchema())
                .put(SourceInfo.TABLE_NAME_KEY, sourceInfo.table())
                .put(SourceInfo.TXID_KEY, sourceInfo.getTransactionId())
                .put(SourceInfo.EVENT_SCN_KEY, eventScn);

        if (sourceInfo.getLcrPosition() != null) {
            ret.put(SourceInfo.LCR_POSITION_KEY, sourceInfo.getLcrPosition());
        }
        if (sourceInfo.getUserName() != null) {
            ret.put(SourceInfo.USERNAME_KEY, sourceInfo.getUserName());
        }
        if (sourceInfo.getRsId() != null) {
            ret.put(CommitScn.ROLLBACK_SEGMENT_ID_KEY, sourceInfo.getRsId());
        }
        if (!Strings.isNullOrBlank(sourceInfo.getRedoSql())) {
            ret.put(SourceInfo.REDO_SQL, sourceInfo.getRedoSql());
        }
        if (!Strings.isNullOrBlank(sourceInfo.getRowId())) {
            ret.put(SourceInfo.ROW_ID, sourceInfo.getRowId());
        }

        ret.put(CommitScn.SQL_SEQUENCE_NUMBER_KEY, sourceInfo.getSsn());

        final CommitScn commitScn = sourceInfo.getCommitScn();
        if (commitScn != null) {
            commitScn.store(sourceInfo, ret);
        }

        return ret;
    }
}

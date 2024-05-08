/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;
import io.debezium.relational.TableId;

public class SqlServerSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private Schema schema;

    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name("io.debezium.connector.sqlserver.Source")
                .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.CHANGE_LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.COMMIT_LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.EVENT_SERIAL_NO_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final TableId tableId = sourceInfo.getTableId();
        final Struct ret = super.commonStruct(sourceInfo)
                .put(SourceInfo.SCHEMA_NAME_KEY, tableId != null ? tableId.schema() : "")
                .put(SourceInfo.TABLE_NAME_KEY, tableId != null ? tableId.table() : "");

        if (sourceInfo.getChangeLsn() != null && sourceInfo.getChangeLsn().isAvailable()) {
            ret.put(SourceInfo.CHANGE_LSN_KEY, sourceInfo.getChangeLsn().toString());
        }
        if (sourceInfo.getCommitLsn() != null && sourceInfo.getCommitLsn().isAvailable()) {
            ret.put(SourceInfo.COMMIT_LSN_KEY, sourceInfo.getCommitLsn().toString());
        }
        if (sourceInfo.getEventSerialNo() != null) {
            ret.put(SourceInfo.EVENT_SERIAL_NO_KEY, sourceInfo.getEventSerialNo());
        }
        return ret;
    }
}

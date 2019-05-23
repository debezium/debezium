/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfoStructMaker;

public class SqlServerSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {
    public static final String LOG_TIMESTAMP_KEY = "ts_ms";
    public static final String CHANGE_LSN_KEY = "change_lsn";
    public static final String COMMIT_LSN_KEY = "commit_lsn";
    public static final String SNAPSHOT_KEY = "snapshot";

    private final Schema schema;

    public SqlServerSourceInfoStructMaker() {
        schema = commonSchemaBuilder()
                .name("io.debezium.connector.sqlserver.Source")
                .field(LOG_TIMESTAMP_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(CHANGE_LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(COMMIT_LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SNAPSHOT_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final Struct ret = super.commonStruct()
                .put(LOG_TIMESTAMP_KEY, sourceInfo.getSourceTime() == null ? null : sourceInfo.getSourceTime().toEpochMilli())
                .put(SNAPSHOT_KEY, sourceInfo.isSnapshot());

        if (sourceInfo.getChangeLsn() != null && sourceInfo.getChangeLsn().isAvailable()) {
            ret.put(CHANGE_LSN_KEY, sourceInfo.getChangeLsn().toString());
        }
        if (sourceInfo.getCommitLsn() != null && sourceInfo.getCommitLsn().isAvailable()) {
            ret.put(COMMIT_LSN_KEY, sourceInfo.getCommitLsn().toString());
        }
        return ret;
    }
}

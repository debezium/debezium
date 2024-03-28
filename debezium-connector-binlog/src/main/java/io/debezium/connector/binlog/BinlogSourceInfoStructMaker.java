/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

/**
 * An abstract implementation of the {@code source} struct maker for binlog connectors.
 *
 * @author Chris Cranford
 */
public abstract class BinlogSourceInfoStructMaker<T extends BinlogSourceInfo> extends AbstractSourceInfoStructMaker<T> {

    private Schema schema;

    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name(String.format("io.debezium.connector.%s.Source", getConnectorName()))
                .field(BinlogSourceInfo.TABLE_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(BinlogSourceInfo.SERVER_ID_KEY, Schema.INT64_SCHEMA)
                .field(BinlogSourceInfo.GTID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(BinlogSourceInfo.BINLOG_FILENAME_OFFSET_KEY, Schema.STRING_SCHEMA)
                .field(BinlogSourceInfo.BINLOG_POSITION_OFFSET_KEY, Schema.INT64_SCHEMA)
                .field(BinlogSourceInfo.BINLOG_ROW_IN_EVENT_OFFSET_KEY, Schema.INT32_SCHEMA)
                .field(BinlogSourceInfo.THREAD_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(BinlogSourceInfo.QUERY_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(T sourceInfo) {
        Struct result = commonStruct(sourceInfo);
        result.put(BinlogSourceInfo.SERVER_ID_KEY, sourceInfo.getServerId());
        if (sourceInfo.getCurrentGtid() != null) {
            // Don't put the GTID Set into the struct; only the current GTID is fine ...
            result.put(BinlogSourceInfo.GTID_KEY, sourceInfo.getCurrentGtid());
        }
        result.put(BinlogSourceInfo.BINLOG_FILENAME_OFFSET_KEY, sourceInfo.getCurrentBinlogFilename());
        result.put(BinlogSourceInfo.BINLOG_POSITION_OFFSET_KEY, sourceInfo.getCurrentBinlogPosition());
        result.put(BinlogSourceInfo.BINLOG_ROW_IN_EVENT_OFFSET_KEY, sourceInfo.getCurrentRowNumber());
        if (sourceInfo.getThreadId() >= 0) {
            result.put(BinlogSourceInfo.THREAD_KEY, sourceInfo.getThreadId());
        }
        if (sourceInfo.table() != null) {
            result.put(BinlogSourceInfo.TABLE_NAME_KEY, sourceInfo.table());
        }
        if (sourceInfo.getQuery() != null) {
            result.put(BinlogSourceInfo.QUERY_KEY, sourceInfo.getQuery());
        }
        return result;
    }

    /**
     * @return the connector name
     */
    protected abstract String getConnectorName();

}

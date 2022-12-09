/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.converters;

import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.sqlserver.SqlServerPartition;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * CloudEvents maker for records produced by SQL Server connector.
 *
 * @author Chris Cranford
 */
public class SqlServerCloudEventsMaker extends CloudEventsMaker {

    public SqlServerCloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        super(parser, contentType, dataSchemaUriBase);
    }

    @Override
    public String ceId() {
        return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";change_lsn:" + recordParser.getMetadata(SqlServerRecordParser.CHANGE_LSN_KEY)
                + ";commit_lsn:" + recordParser.getMetadata(SqlServerRecordParser.COMMIT_LSN_KEY)
                + ";event_serial_no:" + recordParser.getMetadata(SqlServerRecordParser.EVENT_SERIAL_NO_KEY);
    }

    @Override
    public String cePartitionKey() {
        Map<String, String> partitionKeys = new SqlServerPartition(
                recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY).toString(),
                recordParser.getMetadata(AbstractSourceInfo.DATABASE_NAME_KEY).toString()).getSourcePartition();

        return partitionKeys.keySet().stream().sorted().map(k -> k + ":" + partitionKeys.get(k)).collect(Collectors.joining(";"));
    }
}

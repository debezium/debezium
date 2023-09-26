/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.converters;

import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * CloudEvents maker for records produced by the MySQL connector.
 *
 * @author Chris Cranford
 */
public class MySqlCloudEventsMaker extends CloudEventsMaker {

    public MySqlCloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        super(parser, contentType, dataSchemaUriBase);
    }

    @Override
    public String ceId() {
        return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";file:" + recordParser.getMetadata(MySqlRecordParser.BINLOG_FILENAME_OFFSET_KEY)
                + ";pos:" + recordParser.getMetadata(MySqlRecordParser.BINLOG_POSITION_OFFSET_KEY);
    }

    @Override
    public String cePartitionKey() {
        Map<String, String> partitionKeys = new MySqlPartition(
                recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY).toString(),
                recordParser.getMetadata(AbstractSourceInfo.DATABASE_NAME_KEY).toString()).getSourcePartition();

        return partitionKeys.keySet().stream().sorted().map(k -> k + ":" + partitionKeys.get(k)).collect(Collectors.joining(";"));
    }
}

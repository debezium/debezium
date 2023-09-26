/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.converters;

import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * CloudEvents maker for records producer by the PostgreSQL connector.
 *
 * @author Chris Cranford
 */
public class PostgresCloudEventsMaker extends CloudEventsMaker {

    public PostgresCloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        super(parser, contentType, dataSchemaUriBase);
    }

    @Override
    public String ceId() {
        return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";lsn:" + recordParser.getMetadata(PostgresRecordParser.LSN_KEY).toString()
                + ";txId:" + recordParser.getMetadata(PostgresRecordParser.TXID_KEY).toString()
                + ";sequence:" + recordParser.getMetadata(PostgresRecordParser.SEQUENCE_KEY).toString();
    }

    @Override
    public String cePartitionKey() {
        Map<String, String> partitionKeys = new PostgresPartition(
                recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY).toString(),
                recordParser.getMetadata(AbstractSourceInfo.DATABASE_NAME_KEY).toString()).getSourcePartition();

        return partitionKeys.keySet().stream().sorted().map(k -> k + ":" + partitionKeys.get(k)).collect(Collectors.joining(";"));
    }
}

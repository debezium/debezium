/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.converters;

import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * @author Chris Cranford
 */
public class OracleCloudEventsMaker extends CloudEventsMaker {

    public OracleCloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        super(parser, contentType, dataSchemaUriBase);
    }

    @Override
    public String ceId() {
        return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";scn:" + recordParser.getMetadata(OracleRecordParser.SCN_KEY)
                + ";commit_scn:" + recordParser.getMetadata(OracleRecordParser.COMMIT_SCN_KEY)
                + ";lcr_position:" + recordParser.getMetadata(OracleRecordParser.LCR_POSITION_KEY);
    }

    @Override
    public String cePartitionKey() {
        Map<String, String> partitionKeys = new OraclePartition(
                recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY).toString(),
                recordParser.getMetadata(AbstractSourceInfo.DATABASE_NAME_KEY).toString()).getSourcePartition();

        return partitionKeys.keySet().stream().sorted().map(k -> k + ":" + partitionKeys.get(k)).collect(Collectors.joining(";"));
    }
}

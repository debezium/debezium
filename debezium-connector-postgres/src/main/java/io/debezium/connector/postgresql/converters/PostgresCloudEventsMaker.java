/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.converters;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * CloudEvents maker for records producer by the PostgreSQL connector.
 *
 * @author Chris Cranford
 */
public class PostgresCloudEventsMaker extends CloudEventsMaker {

    public PostgresCloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase, String cloudEventsSchemaName) {
        super(parser, contentType, dataSchemaUriBase, cloudEventsSchemaName);
    }

    @Override
    public String ceId() {
        return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";lsn:" + recordParser.getMetadata(PostgresRecordParser.LSN_KEY).toString()
                + ";txId:" + recordParser.getMetadata(PostgresRecordParser.TXID_KEY).toString()
                + ";sequence:" + recordParser.getMetadata(PostgresRecordParser.SEQUENCE_KEY).toString();
    }
}

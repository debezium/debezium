/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.converters;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * CloudEvents maker for records produced by SQL Server connector.
 *
 * @author Chris Cranford
 */
public class SqlServerCloudEventsMaker extends CloudEventsMaker {

    public SqlServerCloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase, String cloudEventsSchemaName) {
        super(parser, contentType, dataSchemaUriBase, cloudEventsSchemaName);
    }

    @Override
    public String ceId() {
        return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";change_lsn:" + recordParser.getMetadata(SqlServerRecordParser.CHANGE_LSN_KEY)
                + ";commit_lsn:" + recordParser.getMetadata(SqlServerRecordParser.COMMIT_LSN_KEY)
                + ";event_serial_no:" + recordParser.getMetadata(SqlServerRecordParser.EVENT_SERIAL_NO_KEY);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.converters;

import io.debezium.connector.sqlserver.transforms.SqlServerAbstractRecordParserProvider;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsProvider;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * An implementation of {@link CloudEventsProvider} for SQL Server.
 *
 * @author Chris Cranford
 */
public class SqlServerCloudEventsProvider extends SqlServerAbstractRecordParserProvider implements CloudEventsProvider {

    @Override
    public CloudEventsMaker createMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        return new SqlServerCloudEventsMaker(parser, contentType, dataSchemaUriBase);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.converters;

import io.debezium.connector.postgresql.transforms.PostgresAbstractRecordParserProvider;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsProvider;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * An implementation of {@link CloudEventsProvider} for PostgreSQL.
 *
 * @author Chris Cranford
 */
public class PostgresCloudEventsProvider extends PostgresAbstractRecordParserProvider implements CloudEventsProvider {
    @Override
    public CloudEventsMaker createMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        return new PostgresCloudEventsMaker(parser, contentType, dataSchemaUriBase);
    }
}

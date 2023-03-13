/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.converters;

import io.debezium.connector.oracle.transforms.OracleAbstractRecordParserProvider;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsProvider;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * An implementation of {@link CloudEventsProvider} for Oracle.
 *
 * @author Chris Cranford
 */
public class OracleCloudEventsProvider extends OracleAbstractRecordParserProvider implements CloudEventsProvider {

    @Override
    public CloudEventsMaker createMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        return new OracleCloudEventsMaker(parser, contentType, dataSchemaUriBase);
    }
}

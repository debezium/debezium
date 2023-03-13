/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.converters;

import io.debezium.connector.mongodb.transforms.MongoDbAbstractRecordParserProvider;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsProvider;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * An implementation of {@link CloudEventsProvider} for MongoDB.
 *
 * @author Chris Cranford
 */
public class MongoDbCloudEventsProvider extends MongoDbAbstractRecordParserProvider implements CloudEventsProvider {

    @Override
    public CloudEventsMaker createMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        return new MongoDbCloudEventsMaker(parser, contentType, dataSchemaUriBase);
    }
}

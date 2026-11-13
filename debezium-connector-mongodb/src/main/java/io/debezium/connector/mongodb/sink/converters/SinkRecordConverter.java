/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkRecordConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkRecordConverter.class);

    private static final SinkValueConverter SCHEMA_VALUE_CONVERTER = new SchemaValueConverter();
    private static final SinkValueConverter MAP_VALUE_CONVERTER = new MapValueConverter();
    private static final SinkValueConverter JSON_STRING_VALUE_CONVERTER = new JsonStringValueConverter();
    private static final SinkValueConverter BYTE_ARRAY_VALUE_CONVERTER = new ByteArrayValueConverter();

    public static BsonDocument convertStruct(final Struct struct) {
        if (struct == null) {
            return null;
        }
        return getConverter(struct.schema(), struct).convert(struct.schema(), struct);
    }

    private static SinkValueConverter getConverter(final Schema schema, final Object data) {
        if (schema != null && data instanceof Struct) {
            LOGGER.debug("Using schema-ed converter");
            return SCHEMA_VALUE_CONVERTER;
        }

        // structured JSON without schema
        if (data instanceof Map) {
            LOGGER.debug("Using schemaless / map converter");
            return MAP_VALUE_CONVERTER;
        }

        // JSON string
        if (data instanceof String) {
            LOGGER.debug("Using JSON string converter");
            return JSON_STRING_VALUE_CONVERTER;
        }

        // BSON bytes
        if (data instanceof byte[]) {
            LOGGER.debug("Using BSON converter");
            return BYTE_ARRAY_VALUE_CONVERTER;
        }

        throw new DataException("No converter found for unexpected object type: " + data.getClass().getName());
    }
}

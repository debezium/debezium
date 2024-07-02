/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;

class JsonStringValueConverter implements SinkValueConverter {

    @Override
    public BsonDocument convert(final Schema schema, final Object value) {
        if (value == null) {
            throw new DataException("JSON string conversion failed due to record key and/or value was null");
        }

        return BsonDocument.parse((String) value);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.MongoClientSettings;

/** Used for converting Maps e.g. schema-less JSON */
class MapValueConverter implements SinkValueConverter {

    @SuppressWarnings({ "unchecked" })
    @Override
    public BsonDocument convert(final Schema schema, final Object value) {
        if (value == null) {
            throw new DataException("JSON conversion failed due to record key and/or value was null");
        }
        return new Document((Map<String, Object>) value)
                .toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry());
    }
}

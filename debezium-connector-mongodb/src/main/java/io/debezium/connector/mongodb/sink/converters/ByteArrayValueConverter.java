/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;

class ByteArrayValueConverter implements SinkValueConverter {

    private static final BsonDocumentCodec BSON_DOCUMENT_CODEC = new BsonDocumentCodec();

    @Override
    public BsonDocument convert(final Schema schema, final Object value) {
        if (value == null) {
            throw new DataException("BSON conversion failed due to record key and/or value was null");
        }

        return new RawBsonDocument((byte[]) value).decode(BSON_DOCUMENT_CODEC);
    }
}

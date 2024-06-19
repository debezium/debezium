/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters.bson;

import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonBinary;
import org.bson.BsonValue;

public class BytesType extends AbstractBsonType {

    public BytesType() {
        super(Schema.BYTES_SCHEMA);
    }

    @Override
    public BsonValue toBson(final Object data) {
        if (data instanceof ByteBuffer) {
            return new BsonBinary(((ByteBuffer) data).array());
        }
        if (data instanceof byte[]) {
            return new BsonBinary((byte[]) data);
        }

        throw new DataException("Bytes field conversion failed due to unexpected object type " + data.getClass().getName());
    }
}

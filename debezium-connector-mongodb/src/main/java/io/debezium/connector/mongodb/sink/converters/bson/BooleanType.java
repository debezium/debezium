/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters.bson;

import org.apache.kafka.connect.data.Schema;
import org.bson.BsonBoolean;
import org.bson.BsonValue;

public class BooleanType extends AbstractBsonType {

    public BooleanType() {
        super(Schema.BOOLEAN_SCHEMA);
    }

    public BsonValue toBson(final Object data) {
        return new BsonBoolean((Boolean) data);
    }
}

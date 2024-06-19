/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters.bson;

import org.apache.kafka.connect.data.Schema;
import org.bson.BsonString;
import org.bson.BsonValue;

public class StringType extends AbstractBsonType {

    public StringType() {
        super(Schema.STRING_SCHEMA);
    }

    @Override
    public BsonValue toBson(final Object data) {
        return new BsonString((String) data);
    }
}

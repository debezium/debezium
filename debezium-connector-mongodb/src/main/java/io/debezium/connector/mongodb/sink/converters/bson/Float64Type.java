/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters.bson;

import org.apache.kafka.connect.data.Schema;
import org.bson.BsonDouble;
import org.bson.BsonValue;

public class Float64Type extends AbstractBsonType {

    public Float64Type() {
        super(Schema.FLOAT64_SCHEMA);
    }

    @Override
    public BsonValue toBson(final Object data) {
        return new BsonDouble((Double) data);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters.bson;

import org.apache.kafka.connect.data.Time;
import org.bson.BsonDateTime;
import org.bson.BsonValue;

public class TimeType extends AbstractBsonType {

    public TimeType() {
        super(Time.SCHEMA);
    }

    @Override
    public BsonValue toBson(final Object data) {
        return new BsonDateTime(((java.util.Date) data).getTime());
    }
}

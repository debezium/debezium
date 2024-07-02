/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters.bson;

import org.apache.kafka.connect.data.Date;
import org.bson.BsonDateTime;
import org.bson.BsonValue;

public class DateType extends AbstractBsonType {

    public DateType() {
        super(Date.SCHEMA);
    }

    @Override
    public BsonValue toBson(final Object data) {
        return new BsonDateTime(((java.util.Date) data).getTime());
    }
}

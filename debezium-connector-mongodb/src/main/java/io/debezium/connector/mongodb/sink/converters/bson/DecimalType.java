/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters.bson;

import java.math.BigDecimal;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDecimal128;
import org.bson.BsonValue;
import org.bson.types.Decimal128;

public class DecimalType extends AbstractBsonType {

    public DecimalType() {
        super(Decimal.schema(0));
    }

    @Override
    public BsonValue toBson(final Object data) {
        if (data instanceof BigDecimal) {
            return new BsonDecimal128(new Decimal128((BigDecimal) data));
        }

        throw new DataException(
                "Decimal conversion not possible when data is of type " + data.getClass().getName());
    }
}

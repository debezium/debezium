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
import org.bson.BsonDouble;
import org.bson.BsonValue;
import org.bson.types.Decimal128;

public class DecimalType extends AbstractBsonType {

    public enum Format {
        DECIMAL128, // for MongoDB v3.4+
        LEGACYDOUBLE // results in double approximation
    }

    private final Format format;

    public DecimalType() {
        super(Decimal.schema(0));
        this.format = Format.DECIMAL128;
    }

    public DecimalType(final Format format) {
        super(Decimal.schema(0));
        this.format = format;
    }

    @Override
    public BsonValue toBson(final Object data) {
        if (data instanceof BigDecimal) {
            if (format.equals(Format.DECIMAL128)) {
                return new BsonDecimal128(new Decimal128((BigDecimal) data));
            }
            if (format.equals(Format.LEGACYDOUBLE)) {
                return new BsonDouble(((BigDecimal) data).doubleValue());
            }
        }

        throw new DataException(
                "Decimal conversion not possible when data is of type " + data.getClass().getName()
                        + " and format is " + format);
    }
}

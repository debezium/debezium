/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters.bson;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractBsonType {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBsonType.class);

    private final Schema schema;

    public AbstractBsonType(final Schema schema) {
        this.schema = schema;
    }

    public Schema getSchema() {
        return schema;
    }

    public abstract BsonValue toBson(Object data);

    public BsonValue toBson(final Object data, final Schema fieldSchema) {
        if (!fieldSchema.isOptional()) {
            if (data == null) {
                throw new DataException("Schema of field \"" + fieldSchema.name() + "\" is not defined as optional but value is null");
            }
            LOGGER.trace("Non-optional field \"{}\" with value \"{}\"", fieldSchema.name(), data);
            return toBson(data);
        }

        if (data != null) {
            LOGGER.trace("Optional field \"{}\" with value \"{}\"", fieldSchema.name(), data);
            return toBson(data);
        }

        if (fieldSchema.defaultValue() != null) {
            LOGGER.trace("Optional field \"{}\" with no data / null value but default value is \"{}\"", fieldSchema.name(), fieldSchema.defaultValue());
            return toBson(fieldSchema.defaultValue());
        }

        LOGGER.trace("Optional field \"{}\" with no data / null value and no default value thus value set to \"{}\"", fieldSchema.name(), BsonNull.VALUE);
        return BsonNull.VALUE;
    }
}

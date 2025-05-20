/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.field;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.util.SchemaUtils;

/**
 * An immutable representation of a {@link org.apache.kafka.connect.data.Field} in a {@link org.apache.kafka.connect.sink.SinkRecord}.
 *
 * @author Chris Cranford
 * @author rk3rn3r
 */
@Immutable
public class FieldDescriptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldDescriptor.class);

    protected final Schema schema;
    protected final String name;
    protected final boolean isKey;
    protected final String columnName;

    public FieldDescriptor(Schema schema, String name, boolean isKey) {
        this.schema = schema;
        this.name = name;
        this.isKey = isKey;
        this.columnName = SchemaUtils.getSourceColumnName(schema).orElse(name);

        LOGGER.trace("Field [{}] with schema [{}]", this.name, schema.type());
        LOGGER.trace("    Optional  : {}", schema.isOptional());

        if (schema.parameters() != null && !schema.parameters().isEmpty()) {
            LOGGER.trace("    Parameters: {}", schema.parameters());
        }

        if (schema.defaultValue() != null) {
            LOGGER.trace("    Def. Value: {}", schema.defaultValue());
        }
    }

    public Schema getSchema() {
        return schema;
    }

    public String getName() {
        return name;
    }

    public boolean isKey() {
        return isKey;
    }

    public String getColumnName() {
        return columnName;
    }

    @Override
    public String toString() {
        return "FieldDescriptor{" +
                "schema=" + schema +
                ", name='" + name + '\'' +
                ", isKey='" + isKey + '\'' +
                ", columnName='" + columnName + '\'' +
                '}';
    }
}

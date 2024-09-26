/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import org.apache.kafka.connect.data.Schema;

import io.debezium.annotation.Immutable;
import io.debezium.data.SchemaAndValueField;

/**
 * Helper class for defining a field, its schema, and value for testing.
 *
 * @author Chris Cranford
 */
@Immutable
public class FieldSchemaAndValue extends SchemaAndValueField {

    private final String fieldName;
    private final Schema schema;
    private final Object value;

    public FieldSchemaAndValue(String fieldName, Schema schema, Object value) {
        super(fieldName, schema, value);
        this.fieldName = fieldName;
        this.schema = schema;
        this.value = value;
    }

    public String fieldName() {
        return fieldName;
    }

    public Schema schema() {
        return schema;
    }

    public Object value() {
        return value;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.schema.SchemaBuilderFactory;
import io.debezium.util.Strings;

/**
 * A semantic type for a set of enumerated values, where the string values contain comma-separated values from an enumeration.
 *
 * @author Randall Hauch
 */
public class EnumSet implements SchemaBuilderFactory {

    public static final String LOGICAL_NAME = "io.debezium.data.EnumSet";
    public static final String VALUES_FIELD = "allowed";

    /**
     * Returns a {@link SchemaBuilder} for a set of enumerated values. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @param allowedValues the comma separated list of allowed values; may not be null
     * @return the schema builder
     */
    public SchemaBuilder builder(String allowedValues) {
        return builder().parameter(VALUES_FIELD, allowedValues);
    }

    /**
     * Returns a {@link SchemaBuilder} for a set of enumerated values. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @param allowedValues the list of allowed values; may not be null
     * @return the schema builder
     */
    public SchemaBuilder builder(List<String> allowedValues) {
        if (allowedValues == null) {
            return builder("");
        }
        return builder(Strings.join(",", allowedValues));
    }

    /**
     * Returns a {@link Schema} for a set of enumerated values, with all other default Schema settings.
     *
     * @param allowedValues the comma separated list of allowed values; may not be null
     * @return the schema
     * @see #builder(String)
     */
    public Schema schema(String allowedValues) {
        return builder(allowedValues).build();
    }

    /**
     * Returns a {@link Schema} for a set of enumerated values, with all other default Schema settings.
     *
     * @param allowedValues the list of allowed values; may not be null
     * @return the schema
     * @see #builder(String)
     */
    public Schema schema(List<String> allowedValues) {
        if (allowedValues == null) {
            return builder("").build();
        }
        return builder(allowedValues).build();
    }

    @Override
    public SchemaBuilder builder() {
        return SchemaBuilder.string()
                .name(LOGICAL_NAME)
                .version(1);
    }
}

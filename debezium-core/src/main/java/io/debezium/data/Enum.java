/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.schema.SchemaFactory;
import io.debezium.util.Strings;

/**
 * A semantic type for an enumeration, where the string values are one of the enumeration's values.
 *
 * @author Randall Hauch
 */
public class Enum {

    public static final String LOGICAL_NAME = "io.debezium.data.Enum";
    public static final String VALUES_FIELD = "allowed";
    public static final int SCHEMA_VERSION = 1;

    /**
     * Returns a {@link SchemaBuilder} for an enumeration. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @param allowedValues the comma separated list of allowed values; may not be null
     * @return the schema builder
     */
    public static SchemaBuilder builder(String allowedValues) {
        return SchemaFactory.get().datatypeEnumSchema(allowedValues);
    }

    /**
     * Returns a {@link SchemaBuilder} for an enumeration. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @param allowedValues the list of allowed values; may not be null
     * @return the schema builder
     */
    public static SchemaBuilder builder(List<String> allowedValues) {
        if (allowedValues == null) {
            return builder("");
        }
        return builder(Strings.join(",", allowedValues));
    }

    /**
     * Returns a {@link SchemaBuilder} for an enumeration, with all other default Schema settings.
     *
     * @param allowedValues the comma separated list of allowed values; may not be null
     * @return the schema
     * @see #builder(String)
     */
    public static Schema schema(String allowedValues) {
        return builder(allowedValues).build();
    }

    /**
     * Returns a {@link SchemaBuilder} for an enumeration, with all other default Schema settings.
     *
     * @param allowedValues the list of allowed values; may not be null
     * @return the schema
     * @see #builder(String)
     */
    public static Schema schema(List<String> allowedValues) {
        if (allowedValues == null) {
            return builder("").build();
        }
        return builder(Strings.join(",", allowedValues)).build();
    }
}

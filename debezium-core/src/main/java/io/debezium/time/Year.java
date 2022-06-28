/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.schema.SchemaBuilderFactory;

/**
 * A utility for defining a Kafka Connect {@link Schema} that represents year values.
 *
 * @author Randall Hauch
 */
public class Year implements SchemaBuilderFactory {

    public static final String SCHEMA_NAME = "io.debezium.time.Year";

    /**
     * Returns a {@link SchemaBuilder} for a {@link Year}. The builder will create a schema that describes a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#int32() INT32} for the literal
     * type storing the year number.
     * <p>
     * You can use the resulting SchemaBuilder to set or override additional schema settings such as required/optional, default
     * value, and documentation.
     *
     * @return the schema builder
     */
    @Override
    public SchemaBuilder builder() {
        return SchemaBuilder.int32()
                .name(SCHEMA_NAME)
                .version(1);
    }

    public Year() {
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.data;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.connector.postgresql.PostgresSchemaFactory;

/**
 * A semantic type for a Ltree string.
 *
 * @author Mincong Huang
 */
public class Ltree {

    public static final String LOGICAL_NAME = "io.debezium.data.Ltree";
    public static int SCHEMA_VERSION = 1;

    /**
     * Returns a {@link SchemaBuilder} for a Ltree field. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return PostgresSchemaFactory.get().datatypeLtreeSchema();
    }

    /**
     * Returns a {@link SchemaBuilder} for a Ltree field, with all other default Schema settings.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }
}

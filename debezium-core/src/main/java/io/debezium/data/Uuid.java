/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.data;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.schema.SchemaBuilderFactory;

/**
 * A semantic type for a Uuid string.
 *
 * @author Horia Chiorean
 */
public class Uuid implements SchemaBuilderFactory {

    public static final String LOGICAL_NAME = "io.debezium.data.Uuid";

    /**
     * Returns a {@link SchemaBuilder} for a Uuid field. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @return the schema builder
     */
    @Override
    public SchemaBuilder builder() {
        return SchemaBuilder.string()
                .name(LOGICAL_NAME)
                .version(1);
    }
}

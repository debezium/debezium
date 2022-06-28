/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.schema.SchemaBuilderFactory;

public class MySqlSchemaBuilderFactory implements SchemaBuilderFactory {
    @Override
    public SchemaBuilder builder() {
        return null;
    }
}

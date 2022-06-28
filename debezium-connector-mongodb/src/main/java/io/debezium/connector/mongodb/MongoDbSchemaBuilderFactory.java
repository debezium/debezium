/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.schema.SchemaBuilderFactory;

public class MongoDbSchemaBuilderFactory implements SchemaBuilderFactory {

    @Override
    public SchemaBuilder builder() {
        return SchemaBuilder.struct().version(1);
    }

    public SchemaBuilder builder(String name) {
        return builder().name(name);
    }
}

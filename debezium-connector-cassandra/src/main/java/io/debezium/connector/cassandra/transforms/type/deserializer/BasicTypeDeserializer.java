/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.kafka.connect.data.SchemaBuilder;

public class BasicTypeDeserializer extends TypeDeserializer {

    private SchemaBuilder schemaBuilder;

    public BasicTypeDeserializer(SchemaBuilder schemaBuilder) {
        this.schemaBuilder = schemaBuilder;
    }

    @Override
    public SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        return schemaBuilder;
    }
}

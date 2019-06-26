/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import org.apache.avro.Schema;
import org.apache.cassandra.db.marshal.AbstractType;

public class BasicTypeDeserializer extends TypeDeserializer {

    private Schema schema;

    public BasicTypeDeserializer(Schema schema) {
        this.schema = schema;
    }

    @Override
    public Schema getSchema(AbstractType<?> abstractType) {
        return schema;
    }
}

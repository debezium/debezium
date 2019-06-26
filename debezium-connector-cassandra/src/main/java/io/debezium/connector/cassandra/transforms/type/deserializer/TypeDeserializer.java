/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import org.apache.avro.Schema;
import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;

public abstract class TypeDeserializer {

    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        return abstractType.getSerializer().deserialize(bb);
    }

    public abstract Schema getSchema(AbstractType<?> abstractType);
}

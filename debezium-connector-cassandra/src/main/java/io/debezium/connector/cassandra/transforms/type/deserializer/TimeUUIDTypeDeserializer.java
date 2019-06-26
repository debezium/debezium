/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import io.debezium.connector.cassandra.transforms.UuidUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;

import static io.debezium.connector.cassandra.transforms.CassandraTypeToAvroSchemaMapper.UUID_TYPE;

public class TimeUUIDTypeDeserializer extends TypeDeserializer {

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        Object value = super.deserialize(abstractType, bb);
        byte[] bytes = UuidUtil.asBytes((java.util.UUID) value);
        return new GenericData.Fixed(UUID_TYPE, bytes);
    }

    @Override
    public Schema getSchema(AbstractType<?> abstractType) {
        return UUID_TYPE;
    }
}

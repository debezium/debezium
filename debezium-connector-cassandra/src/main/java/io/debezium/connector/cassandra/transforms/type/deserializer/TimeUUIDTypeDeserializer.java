/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import static io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders.UUID_TYPE;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Values;

import io.debezium.connector.cassandra.transforms.UuidUtil;

public class TimeUUIDTypeDeserializer extends LogicalTypeDeserializer {

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        Object value = super.deserialize(abstractType, bb);
        return convertDeserializedValue(abstractType, value);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        return UUID_TYPE;
    }

    @Override
    public Object convertDeserializedValue(AbstractType<?> abstractType, Object value) {
        byte[] bytes = UuidUtil.asBytes((java.util.UUID) value);
        return Values.convertToString(getSchemaBuilder(abstractType).build(), bytes);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders;

public class TimestampTypeDeserializer extends LogicalTypeDeserializer {

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        Object value = super.deserialize(abstractType, bb);
        return convertDeserializedValue(abstractType, value);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        return CassandraTypeKafkaSchemaBuilders.TIMESTAMP_MILLI_TYPE;
    }

    @Override
    public Object convertDeserializedValue(AbstractType<?> abstractType, Object value) {
        Date date = (Date) value;
        return date.getTime();
    }
}

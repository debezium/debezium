/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms;

import com.datastax.driver.core.DataType;
import io.debezium.connector.cassandra.transforms.type.deserializer.BasicTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.DurationTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.InetAddressDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.ListTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.MapTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.SetTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.TimeUUIDTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.TimestampTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.TupleTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.TypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.UUIDTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.UserTypeDeserializer;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public final class CassandraTypeDeserializer {

    private CassandraTypeDeserializer() { }

    private static final Map<Class<? extends AbstractType>, TypeDeserializer> typeMap = new HashMap<>();

    static {
        typeMap.put(AsciiType.class, new BasicTypeDeserializer(CassandraTypeKafkaSchemaBuilders.STRING_TYPE));
        typeMap.put(BooleanType.class, new BasicTypeDeserializer(CassandraTypeKafkaSchemaBuilders.BOOLEAN_TYPE));
        typeMap.put(BytesType.class, new BasicTypeDeserializer(CassandraTypeKafkaSchemaBuilders.BYTES_TYPE));
        typeMap.put(ByteType.class, new BasicTypeDeserializer(CassandraTypeKafkaSchemaBuilders.BYTE_TYPE));
        typeMap.put(CounterColumnType.class, new BasicTypeDeserializer(CassandraTypeKafkaSchemaBuilders.LONG_TYPE));
        typeMap.put(DecimalType.class, new BasicTypeDeserializer(CassandraTypeKafkaSchemaBuilders.DOUBLE_TYPE));
        typeMap.put(DoubleType.class, new BasicTypeDeserializer(CassandraTypeKafkaSchemaBuilders.DOUBLE_TYPE));
        typeMap.put(DurationType.class, new DurationTypeDeserializer());
        typeMap.put(FloatType.class, new BasicTypeDeserializer(CassandraTypeKafkaSchemaBuilders.FLOAT_TYPE));
        typeMap.put(InetAddressType.class, new InetAddressDeserializer());
        typeMap.put(Int32Type.class, new BasicTypeDeserializer(CassandraTypeKafkaSchemaBuilders.INT_TYPE));
        typeMap.put(ListType.class, new ListTypeDeserializer());
        typeMap.put(LongType.class, new BasicTypeDeserializer(CassandraTypeKafkaSchemaBuilders.LONG_TYPE));
        typeMap.put(MapType.class, new MapTypeDeserializer());
        typeMap.put(SetType.class, new SetTypeDeserializer());
        typeMap.put(ShortType.class, new BasicTypeDeserializer(CassandraTypeKafkaSchemaBuilders.SHORT_TYPE));
        typeMap.put(SimpleDateType.class, new BasicTypeDeserializer(CassandraTypeKafkaSchemaBuilders.DATE_TYPE));
        typeMap.put(TimeType.class, new BasicTypeDeserializer(CassandraTypeKafkaSchemaBuilders.LONG_TYPE));
        typeMap.put(TimestampType.class, new TimestampTypeDeserializer());
        typeMap.put(TimeUUIDType.class, new TimeUUIDTypeDeserializer());
        typeMap.put(TupleType.class, new TupleTypeDeserializer());
        typeMap.put(UserType.class, new UserTypeDeserializer());
        typeMap.put(UTF8Type.class, new BasicTypeDeserializer(CassandraTypeKafkaSchemaBuilders.STRING_TYPE));
        typeMap.put(UUIDType.class, new UUIDTypeDeserializer());
    }

    /**
     * Deserialize from snapshot/datastax-sourced cassandra data.
     *
     * @param dataType the {@link DataType} of the object
     * @param bb the bytes to deserialize
     * @return the deserialized object.
     */
    public static Object deserialize(DataType dataType, ByteBuffer bb) {
        AbstractType abstractType = CassandraTypeConverter.convert(dataType);
        return deserialize(abstractType, bb);
    }

    /**
     * Deserialize from cdc-log-sourced cassandra data.
     *
     * @param abstractType the {@link AbstractType}
     * @param bb the bytes to deserialize
     * @return the deserialized object.
     */
    public static Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        if (bb == null) {
            return null;
        }

        TypeDeserializer typeDeserializer = typeMap.get(abstractType.getClass());
        return typeDeserializer.deserialize(abstractType, bb);
    }

    /**
     * Construct a kafka connect SchemaBuilder object from a Cassandra data type
     * @param abstractType implementation of Cassandra's AbstractType
     * @return the kafka connect SchemaBuilder object
     */
    public static SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        TypeDeserializer typeDeserializer = typeMap.get(abstractType.getClass());
        return typeDeserializer.getSchemaBuilder(abstractType);
    }

}

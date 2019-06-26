/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms;

import com.datastax.driver.core.DataType;
import io.debezium.connector.cassandra.transforms.type.converter.BasicTypeConverter;
import io.debezium.connector.cassandra.transforms.type.converter.ListTypeConverter;
import io.debezium.connector.cassandra.transforms.type.converter.MapTypeConverter;
import io.debezium.connector.cassandra.transforms.type.converter.SetTypeConverter;
import io.debezium.connector.cassandra.transforms.type.converter.TupleTypeConverter;
import io.debezium.connector.cassandra.transforms.type.converter.TypeConverter;
import io.debezium.connector.cassandra.transforms.type.converter.UserTypeConverter;
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
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;

import java.util.HashMap;
import java.util.Map;

public final class CassandraTypeConverter {

    private CassandraTypeConverter() { }

    private static final Map<DataType.Name, TypeConverter<?>> typeMap = new HashMap<>();

    static {
        typeMap.put(DataType.Name.ASCII, new BasicTypeConverter<>(AsciiType.instance));
        typeMap.put(DataType.Name.BIGINT, new BasicTypeConverter<>(LongType.instance));
        typeMap.put(DataType.Name.BLOB, new BasicTypeConverter<>(BytesType.instance));
        typeMap.put(DataType.Name.BOOLEAN, new BasicTypeConverter<>(BooleanType.instance));
        typeMap.put(DataType.Name.COUNTER, new BasicTypeConverter<>(CounterColumnType.instance));
        typeMap.put(DataType.Name.DATE, new BasicTypeConverter<>(SimpleDateType.instance));
        typeMap.put(DataType.Name.DECIMAL, new BasicTypeConverter<>(DecimalType.instance));
        typeMap.put(DataType.Name.DOUBLE, new BasicTypeConverter<>(DoubleType.instance));
        typeMap.put(DataType.Name.DURATION, new BasicTypeConverter<>(DurationType.instance));
        typeMap.put(DataType.Name.FLOAT, new BasicTypeConverter<>(FloatType.instance));
        typeMap.put(DataType.Name.INET, new BasicTypeConverter<>(InetAddressType.instance));
        typeMap.put(DataType.Name.INT, new BasicTypeConverter<>(Int32Type.instance));
        typeMap.put(DataType.Name.LIST, new ListTypeConverter());
        typeMap.put(DataType.Name.MAP, new MapTypeConverter());
        typeMap.put(DataType.Name.SET, new SetTypeConverter());
        typeMap.put(DataType.Name.SMALLINT, new BasicTypeConverter<>(ShortType.instance));
        typeMap.put(DataType.Name.TEXT, new BasicTypeConverter<>(UTF8Type.instance));
        typeMap.put(DataType.Name.TIME, new BasicTypeConverter<>(TimeType.instance));
        typeMap.put(DataType.Name.TIMESTAMP, new BasicTypeConverter<>(TimestampType.instance));
        typeMap.put(DataType.Name.TIMEUUID, new BasicTypeConverter<>(TimeUUIDType.instance));
        typeMap.put(DataType.Name.TINYINT, new BasicTypeConverter<>(ByteType.instance));
        typeMap.put(DataType.Name.TUPLE, new TupleTypeConverter());
        typeMap.put(DataType.Name.UDT, new UserTypeConverter());
        typeMap.put(DataType.Name.UUID, new BasicTypeConverter<>(UUIDType.instance));
    }

    public static AbstractType<?> convert(DataType type) {
        TypeConverter<?> typeConverter = typeMap.get(type.getName());
        return typeConverter.convert(type);
    }
}

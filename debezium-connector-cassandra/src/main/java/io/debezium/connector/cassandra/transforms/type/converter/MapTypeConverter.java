/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.converter;

import java.util.List;

import org.apache.cassandra.db.marshal.MapType;

import com.datastax.driver.core.DataType;

import io.debezium.connector.cassandra.transforms.CassandraTypeConverter;

public class MapTypeConverter implements TypeConverter<MapType<?, ?>> {
    @Override
    public MapType convert(DataType dataType) {
        List<DataType> innerDataTypes = dataType.getTypeArguments();
        return MapType.getInstance(CassandraTypeConverter.convert(innerDataTypes.get(0)),
                CassandraTypeConverter.convert(innerDataTypes.get(1)),
                !dataType.isFrozen());
    }
}

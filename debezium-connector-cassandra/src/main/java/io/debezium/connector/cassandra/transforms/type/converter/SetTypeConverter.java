/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.converter;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.SetType;

import com.datastax.driver.core.DataType;

import io.debezium.connector.cassandra.transforms.CassandraTypeConverter;

public class SetTypeConverter implements TypeConverter<SetType<?>> {
    @Override
    public SetType convert(DataType dataType) {
        AbstractType<?> innerType = CassandraTypeConverter.convert(dataType.getTypeArguments().get(0));
        innerType.getSerializer();
        return SetType.getInstance(innerType, !dataType.isFrozen());
    }
}

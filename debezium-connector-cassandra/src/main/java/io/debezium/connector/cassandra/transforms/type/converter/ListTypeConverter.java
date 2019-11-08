/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.converter;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;

import com.datastax.driver.core.DataType;

import io.debezium.connector.cassandra.transforms.CassandraTypeConverter;

public class ListTypeConverter implements TypeConverter<ListType<?>> {

    @Override
    public ListType convert(DataType dataType) {
        // list only has one inner type.
        DataType innerDataType = dataType.getTypeArguments().get(0);
        AbstractType<?> innerAbstractType = CassandraTypeConverter.convert(innerDataType);
        return ListType.getInstance(innerAbstractType, !dataType.isFrozen());
    }
}

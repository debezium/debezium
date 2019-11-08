/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.converter;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TupleType;

import com.datastax.driver.core.DataType;

import io.debezium.connector.cassandra.transforms.CassandraTypeConverter;

public class TupleTypeConverter implements TypeConverter<TupleType> {
    @Override
    public TupleType convert(DataType dataType) {
        com.datastax.driver.core.TupleType tupleDataType = (com.datastax.driver.core.TupleType) dataType;
        List<DataType> innerTypes = tupleDataType.getComponentTypes();
        List<AbstractType<?>> innerAbstractTypes = new ArrayList<>(innerTypes.size());
        for (DataType dt : innerTypes) {
            innerAbstractTypes.add(CassandraTypeConverter.convert(dt));
        }
        return new TupleType(innerAbstractTypes);
    }
}

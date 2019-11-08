/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.converter;

import org.apache.cassandra.db.marshal.AbstractType;

import com.datastax.driver.core.DataType;

public class BasicTypeConverter<T extends AbstractType<?>> implements TypeConverter<T> {

    private T abstractType;

    public BasicTypeConverter(T abstractType) {
        this.abstractType = abstractType;
    }

    @Override
    public T convert(DataType dataType) {
        return abstractType;
    }
}

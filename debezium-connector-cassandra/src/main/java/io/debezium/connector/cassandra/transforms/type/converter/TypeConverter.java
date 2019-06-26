/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.converter;

import com.datastax.driver.core.DataType;
import org.apache.cassandra.db.marshal.AbstractType;

public interface TypeConverter<T extends AbstractType<?>> {

    T convert(DataType dataType);

}

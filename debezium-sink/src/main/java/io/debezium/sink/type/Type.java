/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.type;

import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * A type represents a relational column type used for query abd parameter binding.
 *
 * todo: this is heavily modeled after Hibernate's type system which perhaps could be used instead?
 * todo: is it possible to incorporate default value resolution into the type system?
 *
 * @author rk3rn3r
 */
public interface Type {

    /**
     * Returns the names that this type will be mapped as.
     *
     * <p>For example, when creating a custom mapping for {@code io.debezium.data.Bits}, a type
     * could be registered using the {@code LOGICAL_NAME} of the schema if the type is to be
     * used when a schema name is identified; otherwise it could be registered as the raw column
     * type when column type propagation is enabled.
     */
    String[] getRegistrationKeys();

    /**
     * Return the SQL string to be used in DML statements for binding this type to SQL.
     *
     * @param column column descriptor in the table relational model, never {@code null}
     * @param schema field schema, never {@code null}
     * @param value value to be bound, may be {@code null}
     * @return query parameter argument binding SQL fragment
     */
    String getQueryBinding(ColumnDescriptor column, Schema schema, Object value);

    /**
     * Resolve the default value clause value.
     *
     * @param schema field schema, never {@code null}
     * @param value the default value, should not be {@code null}
     * @return the formatted default value for the SQL statement as a string
     */
    String getDefaultValueBinding(Schema schema, Object value);

    /**
     * Binds the value to the query.
     *
     * @param index  parameter index to bind
     * @param schema field schema, never {@code null}
     * @param value  value to be bound, may be {@code null}
     * @return the list of {@link ValueBindDescriptor}
     */
    List<ValueBindDescriptor> bind(int index, Schema schema, Object value);
}

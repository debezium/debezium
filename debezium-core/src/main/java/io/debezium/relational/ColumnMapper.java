/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

/**
 * A factory for a function used to map values of a column.
 * 
 * @author Randall Hauch
 */
@FunctionalInterface
public interface ColumnMapper {

    /**
     * Create for the given column a function that maps values.
     * 
     * @param column the column description; never null
     * @return the function that converts the value; may be null
     */
    ValueConverter create(Column column);

}

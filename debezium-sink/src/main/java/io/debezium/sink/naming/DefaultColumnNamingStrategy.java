/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.naming;

/**
 * The default implementation of the {@link ColumnNamingStrategy} that simply returns the field's
 * name as the column name in the destination table.
 *
 * @author Chris Cranford
 */
public class DefaultColumnNamingStrategy implements ColumnNamingStrategy {
    @Override
    public String resolveColumnName(String fieldName) {
        // Default behavior is a no-op
        return fieldName;
    }
}

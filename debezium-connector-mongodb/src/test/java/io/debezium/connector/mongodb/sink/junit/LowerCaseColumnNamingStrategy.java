/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.junit;

import io.debezium.sink.naming.ColumnNamingStrategy;

public class LowerCaseColumnNamingStrategy implements ColumnNamingStrategy {
    @Override
    public String resolveColumnName(String fieldName) {
        return fieldName.toLowerCase();
    }
}

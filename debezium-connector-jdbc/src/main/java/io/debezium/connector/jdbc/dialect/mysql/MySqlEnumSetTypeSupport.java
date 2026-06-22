/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.util.stream.Collectors;

import io.debezium.data.EnumeratedValues;

final class MySqlEnumSetTypeSupport {

    private MySqlEnumSetTypeSupport() {
    }

    static String getTypeName(String typeName, String allowedValues) {
        return typeName + "(" + EnumeratedValues.fromCommaSeparatedString(allowedValues).stream()
                .map(MySqlEnumSetTypeSupport::quote)
                .collect(Collectors.joining(",")) + ")";
    }

    private static String quote(String value) {
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'";
    }
}

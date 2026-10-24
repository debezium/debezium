/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;

/**
 * Helper for MariaDB {@code YEAR(2)} columns.
 * <p>
 * MariaDB keeps a {@code YEAR(2)} value as a two-digit year, so reading the column yields {@code 0} for the
 * zero year, while the binlog reports that value as {@code 1900}. Selecting {@code YEAR(column)} yields the
 * value the binlog reports.
 */
final class Year2Column {

    private Year2Column() {
    }

    static boolean isYear2(Column column) {
        return column.length() == 2 && "YEAR".equalsIgnoreCase(column.typeName());
    }

    static String selectExpression(JdbcConnection connection, Column column) {
        final String quoted = connection.quoteIdentifier(column.name());
        return "YEAR(" + quoted + ") AS " + quoted;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Helper class for creating SQL statement expressions.
 *
 * @author Chris Cranford
 */
public class SqlStatementBuilder {

    private final StringBuilder builder;

    public SqlStatementBuilder() {
        this.builder = new StringBuilder();
    }

    public SqlStatementBuilder append(Object value) {
        builder.append(value);
        return this;
    }

    public SqlStatementBuilder appendList(Collection<String> columnNames, Function<String, String> transform) {
        appendList(",", columnNames, transform);
        return this;
    }

    public SqlStatementBuilder appendList(String delimiter, Collection<String> columnNames, Function<String, String> transform) {
        for (Iterator<String> iterator = columnNames.iterator(); iterator.hasNext();) {
            builder.append(transform.apply(iterator.next()));
            if (iterator.hasNext()) {
                builder.append(delimiter);
            }
        }
        return this;
    }

    public SqlStatementBuilder appendLists(Collection<String> columnNames1, Collection<String> columnNames2, Function<String, String> transform) {
        appendLists(",", columnNames1, columnNames2, transform);
        return this;
    }

    public SqlStatementBuilder appendLists(String delimiter, Collection<String> columnNames1, Collection<String> columnNames2, Function<String, String> transform) {
        appendList(delimiter, columnNames1, transform);
        if (!columnNames1.isEmpty() && !columnNames2.isEmpty()) {
            builder.append(delimiter);
        }
        appendList(delimiter, columnNames2, transform);
        return this;
    }

    public String build() {
        return builder.toString();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.relational.TableIdPredicates;

/**
 * {@link io.debezium.relational.TableId} predicates specific to SQL server.
 *
 * @author vjuranek
 */
public class SqlServerTableIdPredicates implements TableIdPredicates {

    @Override
    public boolean isStartDelimiter(char c) {
        return c == '[';
    }

    @Override
    public boolean isEndDelimiter(char c) {
        return c == ']';
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.function;

import java.sql.SQLException;

@FunctionalInterface
public interface JDBCConnectionRequest <T> {
    T execute() throws SQLException;
}

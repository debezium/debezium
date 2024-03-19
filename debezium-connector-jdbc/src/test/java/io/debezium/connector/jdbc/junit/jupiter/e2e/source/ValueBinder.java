/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter.e2e.source;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Functional contract for binding a value to a provided JDBC statement.
 *
 * @author Chris Cranford
 */
@FunctionalInterface
public interface ValueBinder {
    /**
     * Binds a value to the prepared statement at the given index.
     *
     * @param ps prepared statement
     * @param index the index to bind
     * @throws SQLException if there is a database exception
     */
    void bind(PreparedStatement ps, int index) throws SQLException;
}

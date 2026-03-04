/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.ResultSet;
import java.sql.SQLException;

import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;

/**
 * Reads a single column value from a JDBC {@link ResultSet} into a {@link LogMinerEventRow} field.
 *
 * @author Debezium Authors
 */
@FunctionalInterface
public interface ResultSetValueResolver {

    /**
     * Reads a value from {@code rs} and writes it to the appropriate field of {@code row}.
     *
     * @param row the target row being populated, never {@code null}
     * @param rs  the current JDBC result set, never {@code null}
     * @throws SQLException if the result set cannot be read
     */
    void resolve(LogMinerEventRow row, ResultSet rs) throws SQLException;
}

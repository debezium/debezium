/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.jdbc;

import java.sql.SQLException;

/**
 * {@link RuntimeException} which is raised for various {@link java.sql.SQLException} instances and which retains the error
 * code from the original exception.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public final class JdbcConnectionException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final String sqlState;
    private final int errorCode;

    /**
     * Creates a new exception instance, wrapping the supplied SQLException
     *
     * @param e a {@link SQLException} instance, may not be null
     */
    public JdbcConnectionException(SQLException e) {
        this(e.getMessage(), e);
    }

    /**
     * Creates a new exception instance, wrapping the supplied SQLException with a custom message
     *
     * @param message the exception message, may not be null
     * @param e a {@link SQLException} instance, may not be null
     */
    public JdbcConnectionException(String message, SQLException e) {
        super(message, e);
        this.sqlState = e.getSQLState();
        this.errorCode = e.getErrorCode();
    }

    /**
     * Returns the SQL state from the original exception
     *
     * @return the SQL state string
     * @see SQLException#getSQLState()
     */
    public String getSqlState() {
        return sqlState;
    }

    /**
     * Returns the SQL error code from the original exception
     *
     * @return the SQL error code
     * @see SQLException#getErrorCode()
     */
    public int getErrorCode() {
        return errorCode;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

import org.postgresql.util.PSQLState;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Collect;

/**
 * Error handler for Postgres.
 *
 * @author Gunnar Morling
 */
public class PostgresErrorHandler extends ErrorHandler {

    public PostgresErrorHandler(PostgresConnectorConfig connectorConfig, ChangeEventQueue<?> queue, ErrorHandler replacedErrorHandler) {
        super(PostgresConnector.class, connectorConfig, queue, replacedErrorHandler);
    }

    @Override
    protected Set<Class<? extends Exception>> communicationExceptions() {
        return Collect.unmodifiableSet(IOException.class, SQLException.class);
    }

    /**
     * Returns true if the exception (or any cause in its chain) is a permanent SQL error
     * that should not be retried, such as an authentication or authorization failure.
     */
    public static boolean isPermanentError(Throwable t) {
        for (Throwable cause = t; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException) {
                final String sqlState = ((SQLException) cause).getSQLState();
                if (sqlState != null && sqlState.startsWith("28")) {
                    return true;
                }
            }
        }
        return false;
    }

    // Introduced for testing only
    @Override
    protected boolean isRetriable(Throwable throwable) {
        // Check for permanent errors first so the task fails immediately instead of retrying forever.
        if (isPermanentError(throwable) || isSlotInvalidated(throwable)) {
            return false;
        }
        return super.isRetriable(throwable);
    }

    /**
     * Checks if the exception (or any cause in its chain) is the error PostgreSQL raises when a
     * replication slot has been invalidated (SQLSTATE 55000), so retrying cannot succeed.
     */
    public static boolean isSlotInvalidated(Throwable t) {
        for (Throwable cause = t; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sqlException
                    && PSQLState.OBJECT_NOT_IN_STATE.getState().equals(sqlException.getSQLState())) {
                return true;
            }
        }
        return false;
    }

}

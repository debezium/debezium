/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.Set;

import io.debezium.annotation.Immutable;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Collect;

/**
 * Error handle for Oracle.
 *
 * @author Chris Cranford
 */
public class OracleErrorHandler extends ErrorHandler {

    /**
     * Contents of this set should only be ORA-xxxxx errors;
     * The error check uses starts-with semantics
     */
    @Immutable
    private static final Set<String> RETRIABLE_ERROR_CODES = Collect.unmodifiableSet(
            "ORA-03135", // connection lost
            "ORA-12543", // TNS:destination host unreachable
            "ORA-00604", // error occurred at recursive SQL level 1
            "ORA-01089", // Oracle immediate shutdown in progress
            "ORA-01333", // Failed to establish LogMiner dictionary
            "ORA-01284", // Redo/Archive log cannot be opened, likely locked
            "ORA-26653", // Apply DBZXOUT did not start properly and is currently in state INITIALI
            "ORA-01291", // missing logfile
            "ORA-01327", // failed to exclusively lock system dictionary as required BUILD
            "ORA-04030", // out of process memory
            "ORA-00310", // archived log contains sequence *; sequence * required
            "ORA-01343", // LogMiner encountered corruption in the logstream
            "ORA-01371"); // Complete LogMiner dictionary not found

    /**
     * Contents of this set should be any type of error message text;
     * The error check uses case-insensitive contains semantics
     */
    @Immutable
    private static final Set<String> RETRIABLE_ERROR_MESSAGES = Collect.unmodifiableSet(
            "No more data to read from socket",
            "immediate shutdown or close in progress", // nested ORA-01089
            "failed to exclusively lock system dictionary" // nested ORA-01327
    );

    /**
     * Contents of this set will be applied if and only if the error is ORA-00600.
     * These are special cases where an internal error was thrown and we need to
     * evaluate the argument contents.
     */
    @Immutable
    private static final Set<String> RETRIABLE_ORA600_ERROR_MESSAGES = Collect.unmodifiableSet(
            "krvrdGetUID" // Changes made to object identifier (schema change)
    );

    public OracleErrorHandler(OracleConnectorConfig connectorConfig, ChangeEventQueue<?> queue, ErrorHandler replacedErrorHandler) {
        super(OracleConnector.class, connectorConfig, queue, replacedErrorHandler);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        while (throwable != null) {
            // Always retry any recoverable error
            if (throwable instanceof SQLRecoverableException) {
                return true;
            }

            // Specifically checks the SQLException ORA-00600 use cases separately.
            // We do this because we want to specifically check these parameter arguments, but
            // only when the SQL error code is 600 for (ORA-00600).
            if (throwable instanceof SQLException) {
                final SQLException sqlException = (SQLException) throwable;
                final String message = sqlException.getMessage();
                if (sqlException.getErrorCode() == 600 || (message != null && message.startsWith("ORA-00600"))) {
                    for (String match : RETRIABLE_ORA600_ERROR_MESSAGES) {
                        if (message.contains(match)) {
                            return true;
                        }
                    }
                }
            }

            // If message is provided, run checks against it
            final String message = throwable.getMessage();
            if (message != null && message.length() > 0) {
                // Check Oracle error codes
                for (String errorCode : RETRIABLE_ERROR_CODES) {
                    if (message.startsWith(errorCode)) {
                        return true;
                    }
                }
                // Check Oracle error message texts
                for (String messageText : RETRIABLE_ERROR_MESSAGES) {
                    if (message.toUpperCase().contains(messageText.toUpperCase())) {
                        return true;
                    }
                }
            }

            if (throwable.getCause() != null) {
                // We explicitly check this below the top-level error as we only want
                // certain nested exceptions to be retried, not if they're at the top
                final Throwable cause = throwable.getCause();
                if (cause instanceof IOException) {
                    return true;
                }
            }

            throwable = throwable.getCause();
        }
        return false;
    }
}

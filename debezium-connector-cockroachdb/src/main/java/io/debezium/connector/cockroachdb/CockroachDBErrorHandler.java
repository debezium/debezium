package io.debezium.connector.cockroachdb;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles CockroachDB-specific error classification.
 * Determines whether an exception is retryable or recoverable,
 * based on SQL state codes and exception classes.
 *
 * @author Virag Tripathi
 */
public class CockroachDBErrorHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBErrorHandler.class);

    // SQLSTATE class codes for connection errors
    private static final String SQLSTATE_CLASS_CONNECTION_EXCEPTION = "08";
    private static final String SQLSTATE_CLASS_OPERATOR_INTERVENTION = "57";

    // CockroachDB specific retry error codes
    private static final Set<String> RETRYABLE_SQLSTATES = Set.of(
            "40001", // serialization_failure
            "CR000" // CockroachDB retry error (deprecated but still used in messages)
    );

    /**
     * Returns true if the given exception indicates a retryable error.
     */
    public static boolean isRetryable(SQLException e) {
        String sqlState = e.getSQLState();
        if (sqlState == null) {
            return false;
        }

        if (RETRYABLE_SQLSTATES.contains(sqlState)) {
            return true;
        }

        if (sqlState.startsWith(SQLSTATE_CLASS_CONNECTION_EXCEPTION)
                || sqlState.startsWith(SQLSTATE_CLASS_OPERATOR_INTERVENTION)) {
            return true;
        }

        return e instanceof SQLTransientException || e instanceof SQLRecoverableException;
    }

    /**
     * Logs a warning and returns true if the operation should be retried.
     */
    public static boolean handleAndLogRetry(String operationDescription, SQLException e) {
        boolean retry = isRetryable(e);
        if (retry) {
            LOGGER.warn("Retryable error during '{}': {} (SQLState: {})", operationDescription, e.getMessage(), e.getSQLState());
        }
        else {
            LOGGER.error("Non-retryable error during '{}': {} (SQLState: {})", operationDescription, e.getMessage(), e.getSQLState());
        }
        return retry;
    }
}

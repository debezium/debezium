/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.io.IOException;
import java.sql.SQLRecoverableException;
import java.util.ArrayList;
import java.util.List;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handle for Oracle.
 *
 * @author Chris Cranford
 */
public class OracleErrorHandler extends ErrorHandler {

    private static final List<String> retryOracleErrors = new ArrayList<>();
    private static final List<String> retryOracleMessageContainsTexts = new ArrayList<>();
    static {
        // Contents of this list should only be ORA-xxxxx errors
        // The error check uses starts-with semantics
        retryOracleErrors.add("ORA-03135"); // connection lost
        retryOracleErrors.add("ORA-12543"); // TNS:destination host unreachable
        retryOracleErrors.add("ORA-00604"); // error occurred at recursive SQL level 1
        retryOracleErrors.add("ORA-01089"); // Oracle immediate shutdown in progress
        retryOracleErrors.add("ORA-01333"); // Failed to establish LogMiner dictionary
        retryOracleErrors.add("ORA-01284"); // Redo/Archive log cannot be opened, likely locked
        retryOracleErrors.add("ORA-26653"); // Apply DBZXOUT did not start properly and is currently in state INITIALI
        retryOracleErrors.add("ORA-01291"); // missing logfile
        retryOracleErrors.add("ORA-01327"); // failed to exclusively lock system dictionary as required BUILD
        retryOracleErrors.add("ORA-04030"); // out of process memory

        // Contents of this list should be any type of error message text
        // The error check uses case-insensitive contains semantics
        retryOracleMessageContainsTexts.add("No more data to read from socket");
    }

    public OracleErrorHandler(String logicalName, ChangeEventQueue<?> queue) {
        super(OracleConnector.class, logicalName, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        while (throwable != null) {
            // Always retry any recoverable error
            if (throwable instanceof SQLRecoverableException) {
                return true;
            }

            // If message is provided, run checks against it
            final String message = throwable.getMessage();
            if (message != null && message.length() > 0) {
                // Check Oracle error codes
                for (String errorCode : retryOracleErrors) {
                    if (message.startsWith(errorCode)) {
                        return true;
                    }
                }
                // Check Oracle error message texts
                for (String messageText : retryOracleMessageContainsTexts) {
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

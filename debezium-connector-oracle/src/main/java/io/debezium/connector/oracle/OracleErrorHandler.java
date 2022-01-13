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

import oracle.net.ns.NetException;

/**
 * Error handle for Oracle.
 *
 * @author Chris Cranford
 */
public class OracleErrorHandler extends ErrorHandler {

    private static final String NO_MORE_DATA_TO_READ_FROM_SOCKET = "NO MORE DATA TO READ FROM SOCKET";

    private static final List<String> retryOracleErrors = new ArrayList<>();
    static {
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
    }

    public OracleErrorHandler(String logicalName, ChangeEventQueue<?> queue) {
        super(OracleConnector.class, logicalName, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        // Always retry any recoverable error
        if (throwable instanceof SQLRecoverableException) {
            return true;
        }

        // If message is provided, check if it starts with a given Oracle error that's considered flagged for retry
        if (isRetriableByMessageContents(throwable.getMessage())) {
            return true;
        }

        // If there is a cause, inspect the cause, and its message details
        if (throwable.getCause() != null) {
            final Throwable cause = throwable.getCause();
            if (cause instanceof IOException || cause instanceof SQLRecoverableException) {
                return true;
            }

            if (isRetriableByMessageContents(cause.getMessage())) {
                return true;
            }
        }

        return isNestedNetException(throwable);
    }

    private boolean isRetriableByMessageContents(String message) {
        if (message == null || message.length() == 0) {
            return false;
        }
        // Check Oracle error codes
        for (String errorCode : retryOracleErrors) {
            if (message.startsWith(errorCode)) {
                return true;
            }
        }
        // Check specific Oracle message texts
        return message.toUpperCase().contains(NO_MORE_DATA_TO_READ_FROM_SOCKET);
    }

    private boolean isNestedNetException(Throwable throwable) {
        while (throwable != null) {
            if (throwable.getCause() != null && throwable.getCause() instanceof NetException) {
                return true;
            }
            throwable = throwable.getCause();
        }
        return false;
    }
}

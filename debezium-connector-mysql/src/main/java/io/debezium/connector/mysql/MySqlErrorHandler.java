/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

import io.debezium.annotation.Immutable;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Collect;

/**
 * Error handler for MySQL.
 *
 * @author Jiri Pechanec
 */
public class MySqlErrorHandler extends ErrorHandler {

    public MySqlErrorHandler(MySqlConnectorConfig connectorConfig, ChangeEventQueue<?> queue, ErrorHandler replacedErrorHandler) {
        super(MySqlConnector.class, connectorConfig, queue, replacedErrorHandler);
    }

    @Immutable
    private static final Set<Integer> NON_RETRIABLE_ERROR_CODES = Collect.unmodifiableSet(
            1236 // ER_MASTER_FATAL_ERROR_READING_BINLOG
    );

    @Override
    protected Set<Class<? extends Exception>> communicationExceptions() {
        return Collect.unmodifiableSet(IOException.class, SQLException.class);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        while (throwable != null) {
            if (throwable instanceof SQLException sqlException) {
                if (NON_RETRIABLE_ERROR_CODES.contains(sqlException.getErrorCode())) {
                    return false;
                }
            }
            throwable = throwable.getCause();
        }
        return super.isRetriable(throwable);
    }
}

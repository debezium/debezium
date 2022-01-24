/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Set;

import org.postgresql.util.PSQLException;

import io.debezium.DebeziumException;
import io.debezium.annotation.Immutable;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Collect;

/**
 * Error handler for Postgres.
 *
 * @author Gunnar Morling
 */
public class PostgresErrorHandler extends ErrorHandler {

    @Immutable
    private static final Set<String> RETRIABLE_EXCEPTION_MESSSAGES = Collect.unmodifiableSet(
            "Database connection failed when writing to copy",
            "Database connection failed when reading from copy",
            "An I/O error occurred while sending to the backend",
            "ERROR: could not open relation with OID");

    public PostgresErrorHandler(PostgresConnectorConfig connectorConfig, ChangeEventQueue<?> queue) {
        super(PostgresConnector.class, connectorConfig, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (isRetriablePsqlException(throwable)) {
            return true;
        }
        else if (throwable instanceof DebeziumException) {
            return isRetriablePsqlException(throwable.getCause());
        }
        return false;
    }

    public boolean isRetriablePsqlException(Throwable throwable) {
        if (throwable != null && throwable instanceof PSQLException && throwable.getMessage() != null) {
            for (String messageText : RETRIABLE_EXCEPTION_MESSSAGES) {
                if (throwable.getMessage().contains(messageText)) {
                    return true;
                }
            }
        }
        return false;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.ArrayList;
import java.util.List;

import org.postgresql.util.PSQLException;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for Postgres.
 *
 * @author Gunnar Morling
 */
public class PostgresErrorHandler extends ErrorHandler {

    public PostgresErrorHandler(String logicalName, ChangeEventQueue<?> queue) {
        super(PostgresConnector.class, logicalName, queue);
    }

    private static final List<String> retryPostgresMessageContainsTexts = new ArrayList<>();

    static {
        retryPostgresMessageContainsTexts.add("Database connection failed when writing to copy");
        retryPostgresMessageContainsTexts.add("Database connection failed when reading from copy");
        retryPostgresMessageContainsTexts.add("FATAL: terminating connection due to administrator command");
        retryPostgresMessageContainsTexts.add("An I/O error occurred while sending to the backend");
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (throwable instanceof PSQLException && throwable.getMessage() != null) {
            for (String messageText : retryPostgresMessageContainsTexts) {
                if (throwable.getMessage().contains(messageText)) {
                    return true;
                }
            }
        }
        return false;
    }
}

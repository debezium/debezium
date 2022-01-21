/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.postgresql.util.PSQLException;

import io.debezium.annotation.Immutable;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for Postgres.
 *
 * @author Gunnar Morling
 */
public class PostgresErrorHandler extends ErrorHandler {

    @Immutable
    private static final Set<String> RETRIABLE_EXCEPTION_MESSSAGES;

    public PostgresErrorHandler(String logicalName, ChangeEventQueue<?> queue) {
        super(PostgresConnector.class, logicalName, queue);
    }

    static {
        Set<String> tmp = new HashSet<>();
        tmp.add("Database connection failed when writing to copy");
        tmp.add("Database connection failed when reading from copy");
        tmp.add("FATAL: terminating connection due to administrator command");
        tmp.add("An I/O error occurred while sending to the backend");

        RETRIABLE_EXCEPTION_MESSSAGES = Collections.unmodifiableSet(tmp);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (throwable instanceof PSQLException && throwable.getMessage() != null) {
            for (String messageText : RETRIABLE_EXCEPTION_MESSSAGES) {
                if (throwable.getMessage().contains(messageText)) {
                    return true;
                }
            }
        }
        return false;
    }
}

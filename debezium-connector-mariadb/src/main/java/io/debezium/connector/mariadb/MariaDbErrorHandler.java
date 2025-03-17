/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Collect;

/**
 * Error handler for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbErrorHandler extends ErrorHandler {

    public MariaDbErrorHandler(MariaDbConnectorConfig connectorConfig, ChangeEventQueue<?> queue, ErrorHandler replacedErrorHandler) {
        super(MariaDbConnector.class, connectorConfig, queue, replacedErrorHandler);
    }

    @Override
    protected Set<Class<? extends Exception>> communicationExceptions() {
        return Collect.unmodifiableSet(IOException.class, SQLException.class);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (throwable == null) {
            return false;
        }

        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            // Indicates a server_id/server_uuid misconfiguration requiring manual intervention, so retries won't resolve it.
            if (message != null && message.contains("A replica with the same server_uuid/server_id as this replica has connected to the source")) {
                return false;
            }
            current = current.getCause();
        }

        return super.isRetriable(throwable);
    }
}

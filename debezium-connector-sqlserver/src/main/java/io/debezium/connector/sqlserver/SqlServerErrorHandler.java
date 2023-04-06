/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Collect;

/**
 * Error handler for SQL Server.
 *
 * @author Chris Cranford
 */
public class SqlServerErrorHandler extends ErrorHandler {

    public SqlServerErrorHandler(SqlServerConnectorConfig connectorConfig, ChangeEventQueue<?> queue) {
        super(SqlServerConnector.class, connectorConfig, queue);
    }

    public SqlServerErrorHandler(SqlServerConnectorConfig connectorConfig, ChangeEventQueue<?> queue,
                                 SqlServerErrorHandler errorHandler) {
        super(SqlServerConnector.class, connectorConfig, queue, errorHandler);
    }

    @Override
    protected Set<Class<? extends Exception>> communicationExceptions() {
        return Collect.unmodifiableSet(IOException.class, SQLException.class);
    }
}

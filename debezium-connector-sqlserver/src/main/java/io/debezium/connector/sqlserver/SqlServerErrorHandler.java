/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import com.microsoft.sqlserver.jdbc.SQLServerException;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for SQL Server.
 *
 * @author Chris Cranford
 */
public class SqlServerErrorHandler extends ErrorHandler {

    public SqlServerErrorHandler(String logicalName, ChangeEventQueue<?> queue) {
        super(SqlServerConnector.class, logicalName, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        return throwable instanceof SQLServerException
                && (throwable.getMessage().contains("Connection timed out (Read failed)")
                        || throwable.getMessage().contains("The connection has been closed.")
                        || throwable.getMessage().contains("Connection reset"));
    }
}

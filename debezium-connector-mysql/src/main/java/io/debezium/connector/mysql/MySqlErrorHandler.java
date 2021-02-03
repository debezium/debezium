/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for SQL Server.
 *
 * @author Chris Cranford
 */
public class MySqlErrorHandler extends ErrorHandler {

    public MySqlErrorHandler(String logicalName, ChangeEventQueue<?> queue) {
        super(MySqlConnector.class, logicalName, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        return false;
    }
}

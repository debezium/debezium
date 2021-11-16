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
        if (!(throwable instanceof SQLServerException) && throwable.getCause() instanceof SQLServerException) {
            throwable = throwable.getCause();
        }

        return throwable instanceof SQLServerException
                && (throwable.getMessage().contains("Connection timed out (Read failed)")
                        || throwable.getMessage().contains("Connection timed out (Write failed)")
                        || throwable.getMessage().contains("The connection has been closed.")
                        || throwable.getMessage().contains("The connection is closed.")
                        || throwable.getMessage().contains("The login failed.")
                        || throwable.getMessage().contains("Server is in script upgrade mode.")
                        || throwable.getMessage().contains("Try the statement later.")
                        || throwable.getMessage().contains("Connection reset")
                        || throwable.getMessage().contains("SHUTDOWN is in progress")
                        || throwable.getMessage().contains("The server failed to resume the transaction")
                        || throwable.getMessage().contains("Verify the connection properties")
                        || throwable.getMessage()
                                .startsWith("An insufficient number of arguments were supplied for the procedure or function cdc.fn_cdc_get_all_changes_")
                        || throwable.getMessage()
                                .endsWith("was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction."));
    }
}

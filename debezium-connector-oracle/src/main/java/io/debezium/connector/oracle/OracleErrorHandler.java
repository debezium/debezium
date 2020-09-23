/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.io.IOException;
import java.sql.SQLRecoverableException;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

import oracle.net.ns.NetException;

/**
 * Error handle for Oracle.
 *
 * @author Chris Cranford
 */
public class OracleErrorHandler extends ErrorHandler {

    public OracleErrorHandler(String logicalName, ChangeEventQueue<?> queue) {
        super(OracleConnector.class, logicalName, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (throwable.getMessage() == null || throwable.getCause() == null) {
            return false;
        }

        return throwable.getMessage().startsWith("ORA-03135") || // connection lost
                throwable.getMessage().startsWith("ORA-12543") || // TNS:destination host unreachable
                throwable.getMessage().startsWith("ORA-00604") || // error occurred at recursive SQL level 1
                throwable.getMessage().startsWith("ORA-01089") || // Oracle immediate shutdown in progress
                throwable.getCause() instanceof IOException ||
                throwable instanceof SQLRecoverableException ||
                throwable.getMessage().toUpperCase().startsWith("NO MORE DATA TO READ FROM SOCKET") ||
                (throwable.getCause() != null && throwable.getCause().getCause() instanceof NetException);
    }
}

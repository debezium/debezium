/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;

import com.github.shyiko.mysql.binlog.network.ServerException;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for MySQL.
 *
 * @author Jiri Pechanec
 */
public class MySqlErrorHandler extends ErrorHandler {

    private static final String SQL_CODE_TOO_MANY_CONNECTIONS = "08004";

    public MySqlErrorHandler(MySqlConnectorConfig connectorConfig, ChangeEventQueue<?> queue) {
        super(MySqlConnector.class, connectorConfig, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (throwable instanceof SQLException) {
            final SQLException sql = (SQLException) throwable;
            return SQL_CODE_TOO_MANY_CONNECTIONS.equals(sql.getSQLState());
        }
        else if (throwable instanceof ServerException) {
            final ServerException sql = (ServerException) throwable;
            return SQL_CODE_TOO_MANY_CONNECTIONS.equals(sql.getSqlState());
        }
        else if (throwable instanceof DebeziumException && throwable.getCause() != null) {
            return isRetriable(throwable.getCause());
        }
        return false;
    }
}

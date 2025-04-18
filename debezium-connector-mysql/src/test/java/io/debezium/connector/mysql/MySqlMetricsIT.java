/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogMetricsIT;
import io.debezium.connector.binlog.util.BinlogTestConnection;

/**
 * @author Chris Cranford
 */
public class MySqlMetricsIT extends BinlogMetricsIT<MySqlConnector> implements MySqlCommon {

    @Override
    public String getConnectorName() {
        return MySqlCommon.super.getConnectorName();
    }

    @Override
    public Class<MySqlConnector> getConnectorClass() {
        return MySqlCommon.super.getConnectorClass();
    }

    @Override
    public BinlogTestConnection getTestDatabaseConnection(String databaseName) {
        return MySqlCommon.super.getTestDatabaseConnection(databaseName);
    }
}

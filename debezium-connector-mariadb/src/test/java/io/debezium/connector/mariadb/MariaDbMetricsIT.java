/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.BinlogMetricsIT;
import io.debezium.connector.binlog.util.BinlogTestConnection;

/**
 * @author Chris Cranford
 */
public class MariaDbMetricsIT extends BinlogMetricsIT<MariaDbConnector> implements MariaDbCommon {

    @Override
    public Class<MariaDbConnector> getConnectorClass() {
        return MariaDbCommon.super.getConnectorClass();
    }

    @Override
    public BinlogTestConnection getTestDatabaseConnection(String databaseName) {
        return MariaDbCommon.super.getTestDatabaseConnection(databaseName);
    }

    @Override
    public String getConnectorName() {
        return MariaDbCommon.super.getConnectorName();
    }

}

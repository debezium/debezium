/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.zzz;

import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.zzz.ZZZBinlogGtidSetIT;
import io.debezium.connector.mariadb.MariaDbConnector;
import io.debezium.connector.mariadb.Module;
import io.debezium.connector.mariadb.util.MariaDbTestConnection;

/**
 * @author Chris Cranford
 */
public class ZZZMariaDbGtidSetIT extends ZZZBinlogGtidSetIT<MariaDbConnector> {
    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    public Class<MariaDbConnector> getConnectorClass() {
        return MariaDbConnector.class;
    }

    @Override
    public BinlogTestConnection getTestDatabaseConnection(String databaseName) {
        return MariaDbTestConnection.forTestDatabase(databaseName);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.BinlogConnectorTest;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.mariadb.util.MariaDbTestConnection;

/**
 * Common implementation bits for MariaDB for a {@link BinlogConnectorTest}.<p></p>
 *
 * By using this common interface, we avoid needing to duplicate this information in each test, allowing for
 * modifying or adding to the common interface in the future in a single location.
 *
 * @author Chris Cranford
 */
public interface MariaDbCommon extends BinlogConnectorTest<MariaDbConnector> {
    @Override
    default String getConnectorName() {
        return Module.name();
    }

    @Override
    default Class<MariaDbConnector> getConnectorClass() {
        return MariaDbConnector.class;
    }

    @Override
    default BinlogTestConnection getTestDatabaseConnection(String databaseName) {
        return MariaDbTestConnection.forTestDatabase(databaseName);
    }

    @Override
    default BinlogTestConnection getTestDatabaseConnection(String databaseName, int queryTimeout) {
        return MariaDbTestConnection.forTestDatabase(databaseName, queryTimeout);
    }

    @Override
    default BinlogTestConnection getTestReplicaDatabaseConnection(String databaseName) {
        return MariaDbTestConnection.forTestReplicaDatabase(databaseName);
    }

    @Override
    default boolean isMariaDb() {
        return true;
    }

    @Override
    default void dropAllDatabases() {
        MariaDbTestConnection.dropAllDatabases();
    }
}

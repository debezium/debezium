/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogConnectorTest;
import io.debezium.connector.binlog.util.BinlogTestConnection;

/**
 * Common implementation bits for MySQL for a {@link BinlogConnectorTest}.<p></p>
 *
 * By using this common interface, we avoid needing to duplicate this information in each test, allowing for
 * modifying or adding to the common interface in the future in a single location.
 *
 * @author Chris Cranford
 */
public interface MySqlCommon extends BinlogConnectorTest<MySqlConnector> {

    @Override
    default String getConnectorName() {
        return Module.name();
    }

    @Override
    default Class<MySqlConnector> getConnectorClass() {
        return MySqlConnector.class;
    }

    @Override
    default BinlogTestConnection getTestDatabaseConnection(String databaseName) {
        return MySqlTestConnection.forTestDatabase(databaseName);
    }

    @Override
    default BinlogTestConnection getTestDatabaseConnection(String databaseName, int queryTimeout) {
        return MySqlTestConnection.forTestDatabase(databaseName, queryTimeout);
    }

    @Override
    default BinlogTestConnection getTestReplicaDatabaseConnection(String databaseName) {
        return MySqlTestConnection.forTestReplicaDatabase(databaseName);
    }

    @Override
    default boolean isMariaDb() {
        return false;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import org.apache.kafka.connect.source.SourceConnector;

import io.debezium.connector.binlog.util.BinlogTestConnection;

/**
 * @author Chris Cranford
 */
public interface BinlogConnectorTest<C extends SourceConnector> {
    String getConnectorName();

    Class<C> getConnectorClass();

    BinlogTestConnection getTestDatabaseConnection(String databaseName);

    BinlogTestConnection getTestDatabaseConnection(String databaseName, int queryTimeout);

    BinlogTestConnection getTestReplicaDatabaseConnection(String databaseName);

    boolean isMariaDb();

    default void executeStatements(String databaseName, String... statements) {
        throw new UnsupportedOperationException("not support operation for the datasource");
    }

    void dropAllDatabases();

}

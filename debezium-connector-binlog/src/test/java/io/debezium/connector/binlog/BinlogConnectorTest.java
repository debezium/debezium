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

    boolean isMariaDb();
}

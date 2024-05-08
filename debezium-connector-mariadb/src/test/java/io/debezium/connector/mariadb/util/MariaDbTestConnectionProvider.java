/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.util;

import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestConnectionProvider;

/**
 * @author Chris Cranford
 */
public class MariaDbTestConnectionProvider implements TestConnectionProvider {
    @Override
    public BinlogTestConnection forTestDatabase(String databaseName) {
        return MariaDbTestConnection.forTestDatabase(databaseName);
    }
}

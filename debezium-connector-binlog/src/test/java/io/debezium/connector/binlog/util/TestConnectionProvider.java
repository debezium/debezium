/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.util;

/**
 * @author Chris Cranford
 */
public interface TestConnectionProvider {
    BinlogTestConnection forTestDatabase(String databaseName);
}

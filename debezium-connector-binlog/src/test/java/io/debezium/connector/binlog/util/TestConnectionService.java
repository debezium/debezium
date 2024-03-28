/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.util;

import java.util.ServiceLoader;

/**
 * @author Chris Cranford
 */
public class TestConnectionService {

    public static BinlogTestConnection forTestDatabase(String databaseName) {
        final ServiceLoader<TestConnectionProvider> providers = ServiceLoader.load(TestConnectionProvider.class);
        final TestConnectionProvider provider = providers.findFirst().orElseThrow();
        return provider.forTestDatabase(databaseName);
    }

    public static BinlogTestConnection forTestDatabase() {
        return forTestDatabase("mysql");
    }

}

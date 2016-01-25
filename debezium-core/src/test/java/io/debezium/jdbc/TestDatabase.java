/*
 * Copyright 2016 Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import io.debezium.config.Configuration;

public class TestDatabase {
    
    public static JdbcConfiguration testConfig( String databaseName ) {
        return buildTestConfig().withDatabase(databaseName).build();
    }
    
    public static JdbcConfiguration.Builder buildTestConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."));
    }
}

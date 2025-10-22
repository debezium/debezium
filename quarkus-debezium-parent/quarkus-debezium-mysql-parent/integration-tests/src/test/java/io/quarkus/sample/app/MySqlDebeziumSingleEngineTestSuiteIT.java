/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app;

import java.sql.SQLException;

import org.junit.platform.suite.api.BeforeSuite;

public class MySqlDebeziumSingleEngineTestSuiteIT implements QuarkusDebeziumSingleEngineTestSuite {
    @BeforeSuite
    public static void init() throws SQLException {
        Scripts.apply(Scripts.Database.DEFAULT);
    }
}

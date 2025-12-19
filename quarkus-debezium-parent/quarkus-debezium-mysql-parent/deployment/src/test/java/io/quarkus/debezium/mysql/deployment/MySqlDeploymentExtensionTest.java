/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.mysql.deployment;

import org.junit.platform.suite.api.AfterSuite;
import org.junit.platform.suite.api.BeforeSuite;
import org.junit.platform.suite.api.SuiteDisplayName;

import io.quarkus.debezium.testsuite.deployment.QuarkusDebeziumNoSqlExtensionTestSuite;

@SuiteDisplayName("MySQL Debezium Extensions for Quarkus Test Suite")
public class MySqlDeploymentExtensionTest implements QuarkusDebeziumNoSqlExtensionTestSuite {

    private static final MySqlResource mySqlResource = new MySqlResource();

    @BeforeSuite
    public static void init() throws Exception {
        mySqlResource.start();
    }

    @AfterSuite
    public static void close() {
        mySqlResource.stop();
    }
}

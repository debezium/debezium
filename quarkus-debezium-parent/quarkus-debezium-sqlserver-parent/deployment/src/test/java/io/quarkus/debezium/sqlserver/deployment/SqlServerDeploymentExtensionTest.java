/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.sqlserver.deployment;

import org.junit.platform.suite.api.AfterSuite;
import org.junit.platform.suite.api.BeforeSuite;
import org.junit.platform.suite.api.SuiteDisplayName;

import io.quarkus.debezium.testsuite.deployment.QuarkusDebeziumNoSqlExtensionTestSuite;

@SuiteDisplayName("Sql Server Debezium Extensions for Quarkus Test Suite")
public class SqlServerDeploymentExtensionTest implements QuarkusDebeziumNoSqlExtensionTestSuite {
    private static final SqlServerResource sqlServerResource = new SqlServerResource();

    @BeforeSuite
    public static void init() {
        sqlServerResource.start();
    }

    @AfterSuite
    public static void close() {
        sqlServerResource.stop();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.deployment.mariadb.deployment;

import org.junit.platform.suite.api.AfterSuite;
import org.junit.platform.suite.api.BeforeSuite;
import org.junit.platform.suite.api.SuiteDisplayName;

import io.quarkus.debezium.testsuite.deployment.QuarkusDebeziumSqlExtensionTestSuite;

@SuiteDisplayName("MariaDB Debezium Extensions for Quarkus Test Suite")
public class MariaDbDeploymentExtensionTest implements QuarkusDebeziumSqlExtensionTestSuite {
    private static final MariaDbResource mariaDbResource = new MariaDbResource();

    @BeforeSuite
    public static void init() throws Exception {
        mariaDbResource.start();
    }

    @AfterSuite
    public static void close() {
        mariaDbResource.stop();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.mongodb.deployment;

import java.util.List;
import java.util.Map;

import io.quarkus.debezium.testsuite.deployment.QuarkusDebeziumNoSqlExtensionTestSuite;
import org.junit.platform.suite.api.AfterSuite;
import org.junit.platform.suite.api.BeforeSuite;
import org.junit.platform.suite.api.SuiteDisplayName;

@SuiteDisplayName("MongoDB Debezium Extensions for Quarkus Test Suite")
public class MongoDbDeploymentExtensionTest implements QuarkusDebeziumNoSqlExtensionTestSuite {

    private static final MongoDbTestResource mongoDbTestResource = new MongoDbTestResource();

    @BeforeSuite
    public static void init() {
        mongoDbTestResource.start(List.of(
                Map.of("id", 1, "name", "giovanni", "description", "developer"),
                Map.of("id", 2, "name", "mario", "description", "developer")),
                List.of(Map.of("key", 1, "name", "one"),
                        Map.of("key", 2, "name", "two")));
    }

    @AfterSuite
    public static void close() {
        mongoDbTestResource.stop();
    }
}

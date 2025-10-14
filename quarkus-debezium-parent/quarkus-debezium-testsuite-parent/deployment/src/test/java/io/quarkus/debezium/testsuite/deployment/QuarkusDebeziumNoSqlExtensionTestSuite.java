/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.testsuite.deployment;

import org.junit.platform.suite.api.AfterSuite;
import org.junit.platform.suite.api.BeforeSuite;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

import io.quarkus.debezium.testsuite.deployment.suite.CapturingTest;
import io.quarkus.debezium.testsuite.deployment.suite.CapturingTest.Order;
import io.quarkus.debezium.testsuite.deployment.suite.CapturingTest.User;
import io.quarkus.debezium.testsuite.deployment.suite.DebeziumLifeCycleTest;
import io.quarkus.debezium.testsuite.deployment.suite.HeartbeatTest;
import io.quarkus.debezium.testsuite.deployment.suite.NotificationTest;
import io.quarkus.debezium.testsuite.deployment.suite.PostProcessingTest;

/**
 *
 * Test Suite Quarkus Debezium Extension for nosql datasource
 * <p>
 * Test Suite Scenario expectations:
 *  - one source of data called `inventory`
 *  - three collections: general, products, orders {@link Order} and users {@link User}
 *  - expected data:
 *      - users:
 *          | id | name | description |
 *          | 1  | giovanni | developer |
 *          | 2 | mario | developer |
 *      - orders:
 *          | key | name |
 *          | 1 | one |
 *          | 2 | two |
 *  - three destinations:
 *      - topic.inventory.orders -> mapped to {@link Order}
 *      - topic.inventory.users -> mapped to {@link User}
 *      - topic.inventory.products -> not mapped
 *  - heartbeat active with 5ms
 *  - snapshot activated at `initial`
 * <p>
 *  How to set up:
 *   - create in resource test directory a property file 'quarkus-debezium-testsuite.properties' which contains the extension configuration
 *   - define necessary resources lifecycle (like test containers) in the {@link BeforeSuite} and {@link AfterSuite} static method
 *
 */
@Suite
@SelectClasses({ CapturingTest.class, DebeziumLifeCycleTest.class, HeartbeatTest.class, NotificationTest.class, PostProcessingTest.class })
@SuiteDisplayName("Nosql Debezium Extensions for Quarkus Test Suite")
public interface QuarkusDebeziumNoSqlExtensionTestSuite {

}

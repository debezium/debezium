/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
/**
 * This package provides utility classes to automate the lifecycle management of a Debezium
 * outbox connector during Quarkus tests.
 *
 * <p>
 * The goal is to ensure that a Debezium connector is created before each test and
 * deleted afterward, allowing each test to run in isolation with a fresh connector instance.
 * This helps maintain a clean state between tests and prevents potential issues caused
 * by leftover resources from previous tests.
 * </p>
 *
 * <p>
 * The package includes:
 * </p>
 *
 * <ul>
 *     <li>{@link io.debezium.outbox.test.quarkus.callback.InitOutboxConnectorBeforeEachCallback}: Registers the outbox connector
 *     before each test. It retrieves the database configuration from the Quarkus runtime
 *     and sets up the connector using the {@link io.debezium.outbox.test.quarkus.remote.RemoteConnectorApi}.</li>
 *
 *     <li>{@link io.debezium.outbox.test.quarkus.callback.DeleteOutboxConnectorAfterEachCallback}: Deletes the outbox connector
 *     after each test to ensure a clean environment for subsequent tests.</li>
 * </ul>
 *
 * <p>
 * These utilities are essential for testing applications that rely on change data capture (CDC)
 * via Debezium to publish database changes to Kafka topics. By managing the lifecycle of the
 * connector, these classes provide a reliable and isolated testing environment.
 * </p>
 */
package io.debezium.outbox.test.quarkus.callback;
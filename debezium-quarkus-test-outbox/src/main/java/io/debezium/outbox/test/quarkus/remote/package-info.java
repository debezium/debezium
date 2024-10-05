/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
/**
 * This package contains classes for configuring and managing a Debezium connector using RESTful API interactions.
 * It includes classes to represent the configuration, status, and interaction with the connector, as well as utilities
 * for building and managing these configurations programmatically.
 *
 * <p>
 * Main components:
 * </p>
 *
 * <ul>
 *     <li>{@link io.debezium.outbox.test.quarkus.remote.Connector}: Represents the state of the Debezium connector (e.g., RUNNING, FAILED).</li>
 *
 *     <li>{@link io.debezium.outbox.test.quarkus.remote.DebeziumConnectorConfiguration}: Represents the overall configuration of a Debezium connector,
 *     including its name and a detailed configuration object ({@link io.debezium.outbox.test.quarkus.remote.DebeziumConnectorConfigurationConfig}).
 *     This class supports building configuration instances via a fluent Builder pattern.</li>
 *
 *     <li>{@link io.debezium.outbox.test.quarkus.remote.DebeziumConnectorConfigurationConfig}: Contains specific settings for connecting to the database,
 *     such as hostname, port, credentials, and database details. It also defines Kafka topic configuration,
 *     converter settings, and additional Debezium-specific transforms for event processing.</li>
 *
 *     <li>{@link io.debezium.outbox.test.quarkus.remote.DebeziumConnectorStatus}: Encapsulates the current status of a Debezium connector, including its name
 *     and state, and provides a utility method to check if the connector is running.</li>
 *
 *     <li>{@link io.debezium.outbox.test.quarkus.remote.RemoteConnectorApi}: A REST client interface for interacting with the Debezium connector.
 *     It defines methods to register a new connector, retrieve its status, and delete it.
 *     The client is designed to interact with Debezium's REST API, specifically using the Quarkus REST client for HTTP requests.</li>
 * </ul>
 *
 * <p>
 * This package is primarily used for managing the lifecycle and configuration of a Debezium connector, which captures
 * changes from a PostgreSQL database and streams them to Apache Kafka topics for downstream processing.
 * </p>
 */
package io.debezium.outbox.test.quarkus.remote;
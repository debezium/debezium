/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.embedded;

/**
 * The Debezium Embedded API provides a simple and lightweight way for an application to directly monitor an external database
 * and receive all of the data changes that are made to that database.
 * <p>
 * The primary benefit of embedding a connector is that the application and connector no longer require nor use the external
 * systems normally required by Debezium (e.g., Kafka, Kafka Connect, and Zookeeper), so the application architecture becomes
 * more straightforward.
 * <p>
 * However, an embedded connector is not as fault tolerant or scalable, and the connector only monitors the database while
 * the application is running. Furthermore, the embedding application is completely responsible for the connector's lifecycle
 * and storage of its internal state (e.g., offsets, database schema history, etc.) so that, after the application stops and
 * restarts its connector, the connector can continue processing exactly where it left off.
 * <h2>Usage</h2>
 * <p>
 * Applications do not directly work with Debezium connectors, but instead configure and build an
 * {@link io.debezium.embedded.EmbeddedEngine} instance that wraps and completely manages a single standard Debezium connector.
 * The application also provides the engine with a function that it will use to deliver data change events to the application.
 * <p>
 * Once the application has configured its {@link io.debezium.embedded.EmbeddedEngine} instance and is ready to start receiving
 * data change events, the application submits the EmbeddedEngine to an {@link java.util.concurrent.Executor} or
 * {@link java.util.concurrent.ExecutorService} managed by the application. The EmbeddedEngine's
 * {@link io.debezium.embedded.EmbeddedConnector#run()} method will start the standard Debezium connector and continuously
 * deliver any data changes events to the application.
 * <p>
 * When the application is ready to shut down the engine, it should call {@link io.debezium.embedded.EmbeddedEngine#stop()} on the
 * engine, which will then stop the connector and have it gracefully complete all current work and shut down.
 * The application can wait for the engine to complete by using the
 * {@link io.debezium.embedded.EmbeddedEngine#await(long, java.util.concurrent.TimeUnit)} method.
 * <h2>Storing connector state</h2>
 * <p>
 * All connector state is managed by components defined in the engine's configuration.
 * <p>
 * As Debezium connectors operate, they keep track of how much information from the source database they have processed, and they
 * record this <em>offset information</em> in an {@link org.apache.kafka.connect.storage.OffsetBackingStore}. Kafka Connect
 * provides several implementations that can be used by an application, including a
 * {@link org.apache.kafka.connect.storage.FileOffsetBackingStore file-based store} and an
 * {@link org.apache.kafka.connect.storage.MemoryOffsetBackingStore memory-based store}. For most applications the memory-based
 * store will not be sufficient, since when the application shuts down all offset information will be lost. Instead, most
 * applications should use the file-based store (or another persistent implementation of
 * {@link org.apache.kafka.connect.storage.OffsetBackingStore}) so that all offset information is persisted after the application
 * terminates and can be read upon restart.
 * <p>
 * Some Debezium connectors to relational databases may also keep track of all changes to the database's schema so that it has
 * the correct table structure for any point in time as it reads the transaction logs. This is critical information, since the
 * data being read from the transaction log reflects the database structure at the time those records were written in the log,
 * and the database's table structure may have changed since that point in time. These connectors use a
 * {@link io.debezium.relational.history.SchemaHistory} store to persist the database schema changes and the offsets at which
 * the changes are recorded. This way, no matter at which offset the database connector starts reading the transaction log, the
 * connector will have the correct database schema for that point in time. And, just like with the
 * {@link org.apache.kafka.connect.storage.OffsetBackingStore}, the application must provide the EmbeddedConnector with a
 * {@link io.debezium.relational.history.SchemaHistory} implementation such as the
 * {@link io.debezium.storage.file.history.FileSchemaHistory} that stores the schema changes on the local file system.
 */

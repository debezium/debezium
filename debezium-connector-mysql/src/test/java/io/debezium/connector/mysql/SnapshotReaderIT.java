/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.source.SourceRecord;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.debezium.config.Configuration;
import io.debezium.connector.cube.DatabaseCube;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.connector.mysql.cube.DefaultDatabase;
import io.debezium.data.KeyValueStore;
import io.debezium.data.KeyValueStore.Collection;
import io.debezium.data.SchemaChangeHistory;
import io.debezium.data.VerifyRecord;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 *
 */
@RunWith(Arquillian.class)
public class SnapshotReaderIT {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-snapshot.txt").toAbsolutePath();
    private static final String DB_NAME = "connector_test_ro";
    private static final String LOGICAL_NAME = "logical_server_name";

    @DefaultDatabase
    private DatabaseCube cube;

    private Configuration config;
    private MySqlTaskContext context;
    private SnapshotReader reader;
    private CountDownLatch completed;

    @Before
    public void beforeEach() {
        Testing.Files.delete(DB_HISTORY_PATH);
        completed = new CountDownLatch(1);
    }

    @After
    public void afterEach() {
        if (reader != null) {
            try {
                reader.stop();
            } finally {
                reader = null;
            }
        }
        if (context != null) {
            try {
                context.shutdown();
            } finally {
                context = null;
                Testing.Files.delete(DB_HISTORY_PATH);
            }
        }
    }

    protected Configuration.Builder simpleConfig() {
        return cube.configuration()
                            .with(MySqlConnectorConfig.USER, "snapper")
                            .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                            .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED)
                            .with(MySqlConnectorConfig.SERVER_ID, 18911)
                            .with(MySqlConnectorConfig.SERVER_NAME, LOGICAL_NAME)
                            .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                            .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                            .with(MySqlConnectorConfig.DATABASE_WHITELIST, DB_NAME)
                            .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                            .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH);
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabase() throws Exception {
        config = simpleConfig().build();
        context = new MySqlTaskContext(config);
        context.start();
        reader = new SnapshotReader("snapshot", context);
        reader.uponCompletion(completed::countDown);
        reader.generateInsertEvents();
        reader.useMinimalBlocking(true);

        // Start the snapshot ...
        reader.start();

        // Poll for records ...
        // Testing.Print.enable();
        List<SourceRecord> records = null;
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(LOGICAL_NAME + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(LOGICAL_NAME);
        while ((records = reader.poll()) != null) {
            records.forEach(record -> {
                VerifyRecord.isValid(record);
                store.add(record);
                schemaChanges.add(record);
            });
        }
        // The last poll should always return null ...
        assertThat(records).isNull();

        // There should be no schema changes ...
        assertThat(schemaChanges.recordCount()).isEqualTo(0);

        // Check the records via the store ...
        assertThat(store.collectionCount()).isEqualTo(4);
        Collection products = store.collection(DB_NAME, "products");
        assertThat(products.numberOfCreates()).isEqualTo(9);
        assertThat(products.numberOfUpdates()).isEqualTo(0);
        assertThat(products.numberOfDeletes()).isEqualTo(0);
        assertThat(products.numberOfReads()).isEqualTo(0);
        assertThat(products.numberOfTombstones()).isEqualTo(0);
        assertThat(products.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection products_on_hand = store.collection(DB_NAME, "products_on_hand");
        assertThat(products_on_hand.numberOfCreates()).isEqualTo(9);
        assertThat(products_on_hand.numberOfUpdates()).isEqualTo(0);
        assertThat(products_on_hand.numberOfDeletes()).isEqualTo(0);
        assertThat(products_on_hand.numberOfReads()).isEqualTo(0);
        assertThat(products_on_hand.numberOfTombstones()).isEqualTo(0);
        assertThat(products_on_hand.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products_on_hand.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection customers = store.collection(DB_NAME, "customers");
        assertThat(customers.numberOfCreates()).isEqualTo(4);
        assertThat(customers.numberOfUpdates()).isEqualTo(0);
        assertThat(customers.numberOfDeletes()).isEqualTo(0);
        assertThat(customers.numberOfReads()).isEqualTo(0);
        assertThat(customers.numberOfTombstones()).isEqualTo(0);
        assertThat(customers.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(customers.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection orders = store.collection(DB_NAME, "orders");
        assertThat(orders.numberOfCreates()).isEqualTo(5);
        assertThat(orders.numberOfUpdates()).isEqualTo(0);
        assertThat(orders.numberOfDeletes()).isEqualTo(0);
        assertThat(orders.numberOfReads()).isEqualTo(0);
        assertThat(orders.numberOfTombstones()).isEqualTo(0);
        assertThat(orders.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(orders.numberOfValueSchemaChanges()).isEqualTo(1);

        // Make sure the snapshot completed ...
        if (completed.await(10, TimeUnit.SECONDS)) {
            // completed the snapshot ...
            Testing.print("completed the snapshot");
        } else {
            fail("failed to complete the snapshot within 10 seconds");
        }
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabaseUsingReadEvents() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.DATABASE_WHITELIST, "connector_(.*)").build();
        context = new MySqlTaskContext(config);
        context.start();
        reader = new SnapshotReader("snapshot", context);
        reader.uponCompletion(completed::countDown);
        reader.generateReadEvents();
        reader.useMinimalBlocking(true);

        // Start the snapshot ...
        reader.start();

        // Poll for records ...
        // Testing.Print.enable();
        List<SourceRecord> records = null;
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(LOGICAL_NAME + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(LOGICAL_NAME);
        while ((records = reader.poll()) != null) {
            records.forEach(record -> {
                VerifyRecord.isValid(record);
                store.add(record);
                schemaChanges.add(record);
            });
        }
        // The last poll should always return null ...
        assertThat(records).isNull();

        // There should be no schema changes ...
        assertThat(schemaChanges.recordCount()).isEqualTo(0);

        // Check the records via the store ...
        assertThat(store.databases()).containsOnly(DB_NAME, "connector_test"); // 2 databases
        assertThat(store.collectionCount()).isEqualTo(8); // 2 databases

        Collection products = store.collection(DB_NAME, "products");
        assertThat(products.numberOfCreates()).isEqualTo(0);
        assertThat(products.numberOfUpdates()).isEqualTo(0);
        assertThat(products.numberOfDeletes()).isEqualTo(0);
        assertThat(products.numberOfReads()).isEqualTo(9);
        assertThat(products.numberOfTombstones()).isEqualTo(0);
        assertThat(products.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection products_on_hand = store.collection(DB_NAME, "products_on_hand");
        assertThat(products_on_hand.numberOfCreates()).isEqualTo(0);
        assertThat(products_on_hand.numberOfUpdates()).isEqualTo(0);
        assertThat(products_on_hand.numberOfDeletes()).isEqualTo(0);
        assertThat(products_on_hand.numberOfReads()).isEqualTo(9);
        assertThat(products_on_hand.numberOfTombstones()).isEqualTo(0);
        assertThat(products_on_hand.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products_on_hand.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection customers = store.collection(DB_NAME, "customers");
        assertThat(customers.numberOfCreates()).isEqualTo(0);
        assertThat(customers.numberOfUpdates()).isEqualTo(0);
        assertThat(customers.numberOfDeletes()).isEqualTo(0);
        assertThat(customers.numberOfReads()).isEqualTo(4);
        assertThat(customers.numberOfTombstones()).isEqualTo(0);
        assertThat(customers.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(customers.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection orders = store.collection(DB_NAME, "orders");
        assertThat(orders.numberOfCreates()).isEqualTo(0);
        assertThat(orders.numberOfUpdates()).isEqualTo(0);
        assertThat(orders.numberOfDeletes()).isEqualTo(0);
        assertThat(orders.numberOfReads()).isEqualTo(5);
        assertThat(orders.numberOfTombstones()).isEqualTo(0);
        assertThat(orders.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(orders.numberOfValueSchemaChanges()).isEqualTo(1);

        // Make sure the snapshot completed ...
        if (completed.await(10, TimeUnit.SECONDS)) {
            // completed the snapshot ...
            Testing.print("completed the snapshot");
        } else {
            fail("failed to complete the snapshot within 10 seconds");
        }
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabaseWithSchemaChanges() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true).build();
        context = new MySqlTaskContext(config);
        context.start();
        reader = new SnapshotReader("snapshot", context);
        reader.uponCompletion(completed::countDown);
        reader.generateInsertEvents();
        reader.useMinimalBlocking(true);

        // Start the snapshot ...
        reader.start();

        // Poll for records ...
        // Testing.Print.enable();
        List<SourceRecord> records = null;
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(LOGICAL_NAME + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(LOGICAL_NAME);
        while ((records = reader.poll()) != null) {
            records.forEach(record -> {
                VerifyRecord.isValid(record);
                store.add(record);
                schemaChanges.add(record);
            });
        }
        // The last poll should always return null ...
        assertThat(records).isNull();

        // There should be 11 schema changes plus 1 SET statement ...
        assertThat(schemaChanges.recordCount()).isEqualTo(12);
        assertThat(schemaChanges.databaseCount()).isEqualTo(2);
        assertThat(schemaChanges.databases()).containsOnly(DB_NAME, "");

        // Check the records via the store ...
        assertThat(store.collectionCount()).isEqualTo(4);
        Collection products = store.collection(DB_NAME, "products");
        assertThat(products.numberOfCreates()).isEqualTo(9);
        assertThat(products.numberOfUpdates()).isEqualTo(0);
        assertThat(products.numberOfDeletes()).isEqualTo(0);
        assertThat(products.numberOfReads()).isEqualTo(0);
        assertThat(products.numberOfTombstones()).isEqualTo(0);
        assertThat(products.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection products_on_hand = store.collection(DB_NAME, "products_on_hand");
        assertThat(products_on_hand.numberOfCreates()).isEqualTo(9);
        assertThat(products_on_hand.numberOfUpdates()).isEqualTo(0);
        assertThat(products_on_hand.numberOfDeletes()).isEqualTo(0);
        assertThat(products_on_hand.numberOfReads()).isEqualTo(0);
        assertThat(products_on_hand.numberOfTombstones()).isEqualTo(0);
        assertThat(products_on_hand.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products_on_hand.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection customers = store.collection(DB_NAME, "customers");
        assertThat(customers.numberOfCreates()).isEqualTo(4);
        assertThat(customers.numberOfUpdates()).isEqualTo(0);
        assertThat(customers.numberOfDeletes()).isEqualTo(0);
        assertThat(customers.numberOfReads()).isEqualTo(0);
        assertThat(customers.numberOfTombstones()).isEqualTo(0);
        assertThat(customers.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(customers.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection orders = store.collection(DB_NAME, "orders");
        assertThat(orders.numberOfCreates()).isEqualTo(5);
        assertThat(orders.numberOfUpdates()).isEqualTo(0);
        assertThat(orders.numberOfDeletes()).isEqualTo(0);
        assertThat(orders.numberOfReads()).isEqualTo(0);
        assertThat(orders.numberOfTombstones()).isEqualTo(0);
        assertThat(orders.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(orders.numberOfValueSchemaChanges()).isEqualTo(1);

        // Make sure the snapshot completed ...
        if (completed.await(10, TimeUnit.SECONDS)) {
            // completed the snapshot ...
            Testing.print("completed the snapshot");
        } else {
            fail("failed to complete the snapshot within 10 seconds");
        }
    }

    @Test
    public void shouldCreateSnapshotSchemaOnly() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY).build();
        context = new MySqlTaskContext(config);
        context.start();
        reader = new SnapshotReader("snapshot", context);
        reader.uponCompletion(completed::countDown);
        reader.generateInsertEvents();
        reader.useMinimalBlocking(true);

        // Start the snapshot ...
        reader.start();

        // Poll for records ...
        // Testing.Print.enable();
        List<SourceRecord> records = null;
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(LOGICAL_NAME + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(LOGICAL_NAME);
        while ((records = reader.poll()) != null) {
            records.forEach(record -> {
                VerifyRecord.isValid(record);
                store.add(record);
                schemaChanges.add(record);
            });
        }
        // The last poll should always return null ...
        assertThat(records).isNull();

        // There should be no schema changes ...
        assertThat(schemaChanges.recordCount()).isEqualTo(0);

        // Check the records via the store ...
        assertThat(store.collectionCount()).isEqualTo(0);

        // Make sure the snapshot completed ...
        if (completed.await(10, TimeUnit.SECONDS)) {
            // completed the snapshot ...
            Testing.print("completed the snapshot");
        } else {
            fail("failed to complete the snapshot within 10 seconds");
        }
    }
}

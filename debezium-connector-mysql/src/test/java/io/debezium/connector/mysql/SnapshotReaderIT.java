/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.KeyValueStore;
import io.debezium.data.KeyValueStore.Collection;
import io.debezium.data.SchemaChangeHistory;
import io.debezium.data.VerifyRecord;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 *
 */
public class SnapshotReaderIT {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-snapshot.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("logical_server_name", "connector_test_ro")
            .withDbHistoryPath(DB_HISTORY_PATH);
    private final UniqueDatabase OTHER_DATABASE = new UniqueDatabase("logical_server_name", "connector_test", DATABASE);

    private Configuration config;
    private MySqlTaskContext context;
    private SnapshotReader reader;
    private CountDownLatch completed;

    @Before
    public void beforeEach() {
        Testing.Files.delete(DB_HISTORY_PATH);
        DATABASE.createAndInitialize();
        OTHER_DATABASE.createAndInitialize();
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
        return DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE, MySqlConnectorConfig.SnapshotLockingMode.MINIMAL);
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabase() throws Exception {
        config = simpleConfig()
                .build();
        context = new MySqlTaskContext(config);
        context.start();
        reader = new SnapshotReader("snapshot", context);
        reader.uponCompletion(completed::countDown);
        reader.generateInsertEvents();

        // Start the snapshot ...
        reader.start();

        // Poll for records ...
        // Testing.Print.enable();
        List<SourceRecord> records = null;
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
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
        assertThat(store.collectionCount()).isEqualTo(5);
        Collection products = store.collection(DATABASE.getDatabaseName(), productsTableName());
        assertThat(products.numberOfCreates()).isEqualTo(9);
        assertThat(products.numberOfUpdates()).isEqualTo(0);
        assertThat(products.numberOfDeletes()).isEqualTo(0);
        assertThat(products.numberOfReads()).isEqualTo(0);
        assertThat(products.numberOfTombstones()).isEqualTo(0);
        assertThat(products.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection products_on_hand = store.collection(DATABASE.getDatabaseName(), "products_on_hand");
        assertThat(products_on_hand.numberOfCreates()).isEqualTo(9);
        assertThat(products_on_hand.numberOfUpdates()).isEqualTo(0);
        assertThat(products_on_hand.numberOfDeletes()).isEqualTo(0);
        assertThat(products_on_hand.numberOfReads()).isEqualTo(0);
        assertThat(products_on_hand.numberOfTombstones()).isEqualTo(0);
        assertThat(products_on_hand.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products_on_hand.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection customers = store.collection(DATABASE.getDatabaseName(), "customers");
        assertThat(customers.numberOfCreates()).isEqualTo(4);
        assertThat(customers.numberOfUpdates()).isEqualTo(0);
        assertThat(customers.numberOfDeletes()).isEqualTo(0);
        assertThat(customers.numberOfReads()).isEqualTo(0);
        assertThat(customers.numberOfTombstones()).isEqualTo(0);
        assertThat(customers.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(customers.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection orders = store.collection(DATABASE.getDatabaseName(), "orders");
        assertThat(orders.numberOfCreates()).isEqualTo(5);
        assertThat(orders.numberOfUpdates()).isEqualTo(0);
        assertThat(orders.numberOfDeletes()).isEqualTo(0);
        assertThat(orders.numberOfReads()).isEqualTo(0);
        assertThat(orders.numberOfTombstones()).isEqualTo(0);
        assertThat(orders.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(orders.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection timetest = store.collection(DATABASE.getDatabaseName(), "dbz_342_timetest");
        assertThat(timetest.numberOfCreates()).isEqualTo(1);
        assertThat(timetest.numberOfUpdates()).isEqualTo(0);
        assertThat(timetest.numberOfDeletes()).isEqualTo(0);
        assertThat(timetest.numberOfReads()).isEqualTo(0);
        assertThat(timetest.numberOfTombstones()).isEqualTo(0);
        assertThat(timetest.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(timetest.numberOfValueSchemaChanges()).isEqualTo(1);
        final List<Struct> timerecords = new ArrayList<>();
        timetest.forEach(val -> {
            timerecords.add(((Struct) val.value()).getStruct("after"));
        });
        Struct after = timerecords.get(0);
        assertThat(after.get("c1")).isEqualTo(toMicroSeconds("PT517H51M04.78S"));
        assertThat(after.get("c2")).isEqualTo(toMicroSeconds("-PT13H14M50S"));
        assertThat(after.get("c3")).isEqualTo(toMicroSeconds("-PT733H0M0.001S"));
        assertThat(after.get("c4")).isEqualTo(toMicroSeconds("-PT1H59M59.001S"));
        assertThat(after.get("c5")).isEqualTo(toMicroSeconds("-PT838H59M58.999999S"));

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
        config = simpleConfig().with(MySqlConnectorConfig.DATABASE_WHITELIST, "connector_(.*)_" + DATABASE.getIdentifier()).build();
        context = new MySqlTaskContext(config);
        context.start();
        reader = new SnapshotReader("snapshot", context);
        reader.uponCompletion(completed::countDown);
        reader.generateReadEvents();

        // Start the snapshot ...
        reader.start();

        // Poll for records ...
        // Testing.Print.enable();
        List<SourceRecord> records = null;
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
        while ((records = reader.poll()) != null) {
            records.forEach(record -> {
                VerifyRecord.isValid(record);
                store.add(record);
                schemaChanges.add(record);
                System.out.println(record);
            });
        }
        // The last poll should always return null ...
        assertThat(records).isNull();

        // There should be no schema changes ...
        assertThat(schemaChanges.recordCount()).isEqualTo(0);

        // Check the records via the store ...
        assertThat(store.databases()).containsOnly(DATABASE.getDatabaseName(), OTHER_DATABASE.getDatabaseName()); // 2 databases
        assertThat(store.collectionCount()).isEqualTo(9); // 2 databases

        Collection products = store.collection(DATABASE.getDatabaseName(), productsTableName());
        assertThat(products.numberOfCreates()).isEqualTo(0);
        assertThat(products.numberOfUpdates()).isEqualTo(0);
        assertThat(products.numberOfDeletes()).isEqualTo(0);
        assertThat(products.numberOfReads()).isEqualTo(9);
        assertThat(products.numberOfTombstones()).isEqualTo(0);
        assertThat(products.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection products_on_hand = store.collection(DATABASE.getDatabaseName(), "products_on_hand");
        assertThat(products_on_hand.numberOfCreates()).isEqualTo(0);
        assertThat(products_on_hand.numberOfUpdates()).isEqualTo(0);
        assertThat(products_on_hand.numberOfDeletes()).isEqualTo(0);
        assertThat(products_on_hand.numberOfReads()).isEqualTo(9);
        assertThat(products_on_hand.numberOfTombstones()).isEqualTo(0);
        assertThat(products_on_hand.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products_on_hand.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection customers = store.collection(DATABASE.getDatabaseName(), "customers");
        assertThat(customers.numberOfCreates()).isEqualTo(0);
        assertThat(customers.numberOfUpdates()).isEqualTo(0);
        assertThat(customers.numberOfDeletes()).isEqualTo(0);
        assertThat(customers.numberOfReads()).isEqualTo(4);
        assertThat(customers.numberOfTombstones()).isEqualTo(0);
        assertThat(customers.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(customers.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection orders = store.collection(DATABASE.getDatabaseName(), "orders");
        assertThat(orders.numberOfCreates()).isEqualTo(0);
        assertThat(orders.numberOfUpdates()).isEqualTo(0);
        assertThat(orders.numberOfDeletes()).isEqualTo(0);
        assertThat(orders.numberOfReads()).isEqualTo(5);
        assertThat(orders.numberOfTombstones()).isEqualTo(0);
        assertThat(orders.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(orders.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection timetest = store.collection(DATABASE.getDatabaseName(), "dbz_342_timetest");
        assertThat(timetest.numberOfCreates()).isEqualTo(0);
        assertThat(timetest.numberOfUpdates()).isEqualTo(0);
        assertThat(timetest.numberOfDeletes()).isEqualTo(0);
        assertThat(timetest.numberOfReads()).isEqualTo(1);
        assertThat(timetest.numberOfTombstones()).isEqualTo(0);
        assertThat(timetest.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(timetest.numberOfValueSchemaChanges()).isEqualTo(1);
        final List<Struct> timerecords = new ArrayList<>();
        timetest.forEach(val -> {
            timerecords.add(((Struct) val.value()).getStruct("after"));
        });
        Struct after = timerecords.get(0);
        assertThat(after.get("c1")).isEqualTo(toMicroSeconds("PT517H51M04.78S"));
        assertThat(after.get("c2")).isEqualTo(toMicroSeconds("-PT13H14M50S"));
        assertThat(after.get("c3")).isEqualTo(toMicroSeconds("-PT733H0M0.001S"));
        assertThat(after.get("c4")).isEqualTo(toMicroSeconds("-PT1H59M59.001S"));
        assertThat(after.get("c5")).isEqualTo(toMicroSeconds("-PT838H59M58.999999S"));

        // Make sure the snapshot completed ...
        if (completed.await(10, TimeUnit.SECONDS)) {
            // completed the snapshot ...
            Testing.print("completed the snapshot");
        } else {
            fail("failed to complete the snapshot within 10 seconds");
        }
    }

    private String productsTableName() {
        return context.isTableIdCaseInsensitive() ? "products" : "Products";
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabaseWithSchemaChanges() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true).build();
        context = new MySqlTaskContext(config);
        context.start();
        reader = new SnapshotReader("snapshot", context);
        reader.uponCompletion(completed::countDown);
        reader.generateInsertEvents();

        // Start the snapshot ...
        reader.start();

        // Poll for records ...
        // Testing.Print.enable();
        List<SourceRecord> records = null;
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
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
        assertThat(schemaChanges.recordCount()).isEqualTo(14);
        assertThat(schemaChanges.databaseCount()).isEqualTo(2);
        assertThat(schemaChanges.databases()).containsOnly(DATABASE.getDatabaseName(), "");

        // Check the records via the store ...
        assertThat(store.collectionCount()).isEqualTo(5);
        Collection products = store.collection(DATABASE.getDatabaseName(), productsTableName());
        assertThat(products.numberOfCreates()).isEqualTo(9);
        assertThat(products.numberOfUpdates()).isEqualTo(0);
        assertThat(products.numberOfDeletes()).isEqualTo(0);
        assertThat(products.numberOfReads()).isEqualTo(0);
        assertThat(products.numberOfTombstones()).isEqualTo(0);
        assertThat(products.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection products_on_hand = store.collection(DATABASE.getDatabaseName(), "products_on_hand");
        assertThat(products_on_hand.numberOfCreates()).isEqualTo(9);
        assertThat(products_on_hand.numberOfUpdates()).isEqualTo(0);
        assertThat(products_on_hand.numberOfDeletes()).isEqualTo(0);
        assertThat(products_on_hand.numberOfReads()).isEqualTo(0);
        assertThat(products_on_hand.numberOfTombstones()).isEqualTo(0);
        assertThat(products_on_hand.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products_on_hand.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection customers = store.collection(DATABASE.getDatabaseName(), "customers");
        assertThat(customers.numberOfCreates()).isEqualTo(4);
        assertThat(customers.numberOfUpdates()).isEqualTo(0);
        assertThat(customers.numberOfDeletes()).isEqualTo(0);
        assertThat(customers.numberOfReads()).isEqualTo(0);
        assertThat(customers.numberOfTombstones()).isEqualTo(0);
        assertThat(customers.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(customers.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection orders = store.collection(DATABASE.getDatabaseName(), "orders");
        assertThat(orders.numberOfCreates()).isEqualTo(5);
        assertThat(orders.numberOfUpdates()).isEqualTo(0);
        assertThat(orders.numberOfDeletes()).isEqualTo(0);
        assertThat(orders.numberOfReads()).isEqualTo(0);
        assertThat(orders.numberOfTombstones()).isEqualTo(0);
        assertThat(orders.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(orders.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection timetest = store.collection(DATABASE.getDatabaseName(), "dbz_342_timetest");
        assertThat(timetest.numberOfCreates()).isEqualTo(1);
        assertThat(timetest.numberOfUpdates()).isEqualTo(0);
        assertThat(timetest.numberOfDeletes()).isEqualTo(0);
        assertThat(timetest.numberOfReads()).isEqualTo(0);
        assertThat(timetest.numberOfTombstones()).isEqualTo(0);
        assertThat(timetest.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(timetest.numberOfValueSchemaChanges()).isEqualTo(1);
        final List<Struct> timerecords = new ArrayList<>();
        timetest.forEach(val -> {
            timerecords.add(((Struct) val.value()).getStruct("after"));
        });
        Struct after = timerecords.get(0);
        assertThat(after.get("c1")).isEqualTo(toMicroSeconds("PT517H51M04.78S"));
        assertThat(after.get("c2")).isEqualTo(toMicroSeconds("-PT13H14M50S"));
        assertThat(after.get("c3")).isEqualTo(toMicroSeconds("-PT733H0M0.001S"));
        assertThat(after.get("c4")).isEqualTo(toMicroSeconds("-PT1H59M59.001S"));
        assertThat(after.get("c5")).isEqualTo(toMicroSeconds("-PT838H59M58.999999S"));

        // Make sure the snapshot completed ...
        if (completed.await(10, TimeUnit.SECONDS)) {
            // completed the snapshot ...
            Testing.print("completed the snapshot");
        } else {
            fail("failed to complete the snapshot within 10 seconds");
        }
    }

    @Test(expected = ConnectException.class)
    public void shouldCreateSnapshotSchemaOnlyRecovery_exception() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY).build();
        context = new MySqlTaskContext(config);
        context.start();
        reader = new SnapshotReader("snapshot", context);
        reader.uponCompletion(completed::countDown);
        reader.generateInsertEvents();

        // Start the snapshot ...
        reader.start();

        // Poll for records ...
        // Testing.Print.enable();
        List<SourceRecord> records = null;
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
        while ((records = reader.poll()) != null) {
            records.forEach(record -> {
                VerifyRecord.isValid(record);
                store.add(record);
                schemaChanges.add(record);
            });
        }
        
        // should fail because we have no existing binlog information
    }    
    
    @Test
    public void shouldCreateSnapshotSchemaOnlyRecovery() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY).build();
        context = new MySqlTaskContext(config);
        context.start();
        context.source().setBinlogStartPoint("binlog1", 555); // manually set for happy path testing
        reader = new SnapshotReader("snapshot", context);
        reader.uponCompletion(completed::countDown);
        reader.generateInsertEvents();

        // Start the snapshot ...
        reader.start();

        // Poll for records ...
        // Testing.Print.enable();
        List<SourceRecord> records = null;
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
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
    
    public void shouldCreateSnapshotSchemaOnly() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY).build();
        context = new MySqlTaskContext(config);
        context.start();
        reader = new SnapshotReader("snapshot", context);
        reader.uponCompletion(completed::countDown);
        reader.generateInsertEvents();

        // Start the snapshot ...
        reader.start();

        // Poll for records ...
        // Testing.Print.enable();
        List<SourceRecord> records = null;
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
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

    private long toMicroSeconds(String duration) {
        return Duration.parse(duration).toNanos() / 1_000;
    }
}

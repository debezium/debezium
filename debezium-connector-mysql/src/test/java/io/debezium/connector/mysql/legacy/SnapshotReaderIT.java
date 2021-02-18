/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlTestConnection;
import io.debezium.connector.mysql.UniqueDatabase;
import io.debezium.data.KeyValueStore;
import io.debezium.data.KeyValueStore.Collection;
import io.debezium.data.SchemaChangeHistory;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 *
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public class SnapshotReaderIT {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-snapshot.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("logical_server_name", "connector_test_ro")
            .withDbHistoryPath(DB_HISTORY_PATH);
    private final UniqueDatabase OTHER_DATABASE = new UniqueDatabase("logical_server_name", "connector_test", DATABASE);

    private Configuration config;
    private MySqlTaskContext context;
    private SnapshotReader reader;
    private CountDownLatch completed;

    @Rule
    public SkipTestRule skipRule = new SkipTestRule();

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
            }
            finally {
                reader = null;
            }
        }
        if (context != null) {
            try {
                context.shutdown();
            }
            finally {
                context = null;
                Testing.Files.delete(DB_HISTORY_PATH);
            }
        }
    }

    protected Configuration.Builder simpleConfig() {
        return DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE, MySqlConnectorConfig.SnapshotLockingMode.MINIMAL)
                // Explicitly enable INCLUDE_SQL_QUERY connector option. For snapshots it should have no effect as
                // source query should not included in snapshot events.
                .with(MySqlConnectorConfig.INCLUDE_SQL_QUERY, true)
                .with(MySqlConnector.IMPLEMENTATION_PROP, MySqlConnector.LEGACY_IMPLEMENTATION);
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabase() throws Exception {
        snapshotOfSingleDatabase(true, false);
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabaseWithoutGlobalLock() throws Exception {
        snapshotOfSingleDatabase(false, false);
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabaseWithoutGlobalLockAndStoreOnlyMonitoredTables() throws Exception {
        snapshotOfSingleDatabase(false, true);
    }

    private void snapshotOfSingleDatabase(boolean useGlobalLock, boolean storeOnlyMonitoredTables) throws Exception {
        final Builder builder = simpleConfig();
        if (!useGlobalLock) {
            builder
                    .with(MySqlConnectorConfig.USER, "cloud")
                    .with(MySqlConnectorConfig.PASSWORD, "cloudpass")
                    .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                    .with(DatabaseHistory.STORE_ONLY_MONITORED_TABLES_DDL, storeOnlyMonitoredTables);
        }
        config = builder.build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build());
        context.start();
        reader = new SnapshotReader("snapshot", context, useGlobalLock);
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
                VerifyRecord.hasNoSourceQuery(record);
                store.add(record);
                schemaChanges.add(record);
            });
        }
        // The last poll should always return null ...
        assertThat(records).isNull();

        if (!useGlobalLock) {
            // There should be schema changes ...
            assertThat(schemaChanges.recordCount()).isGreaterThan(0);
        }
        else {
            // There should be no schema changes ...
            assertThat(schemaChanges.recordCount()).isEqualTo(0);
        }

        // Check the records via the store ...
        assertThat(store.collectionCount()).isEqualTo(5);
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

        List<Struct> customerRecrods = new ArrayList<>();
        customers.forEach(val -> {
            customerRecrods.add(((Struct) val.value()).getStruct("after"));
        });

        Struct customer = customerRecrods.stream().sorted((a, b) -> a.getInt32("id").compareTo(b.getInt32("id"))).findFirst().get();
        assertThat(customer.get("first_name")).isInstanceOf(String.class);
        assertThat(customer.get("last_name")).isInstanceOf(String.class);
        assertThat(customer.get("email")).isInstanceOf(String.class);

        assertThat(customer.get("first_name")).isEqualTo("Sally");
        assertThat(customer.get("last_name")).isEqualTo("Thomas");
        assertThat(customer.get("email")).isEqualTo("sally.thomas@acme.com");

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
        }
        else {
            fail("failed to complete the snapshot within 10 seconds");
        }
    }

    @Test
    public void snapshotWithBackupLocksShouldNotWaitForReads() throws Exception {
        final Builder builder = simpleConfig();
        builder
                .with(MySqlConnectorConfig.USER, "cloud")
                .with(MySqlConnectorConfig.PASSWORD, "cloudpass")
                .with(MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE, MySqlConnectorConfig.SnapshotLockingMode.MINIMAL_PERCONA);

        config = builder.build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build());
        context.start();

        reader = new SnapshotReader("snapshot", context, true);
        reader.generateReadEvents();

        if (!MySqlTestConnection.isPerconaServer()) {
            reader.start(); // Start the reader to avoid failure in the afterEach method.
            return; // Skip these tests for non-Percona flavours of MySQL
        }

        MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    JdbcConnection connection = db.connect();
                    connection.executeWithoutCommitting("SELECT *, SLEEP(20) FROM products_on_hand");
                }
                catch (Exception e) {
                    // Do nothing.
                }
            }
        };
        t.start();

        // Start the snapshot ...
        boolean connectException = false;
        reader.start();

        List<SourceRecord> records = null;
        try {
            reader.poll();
        }
        catch (org.apache.kafka.connect.errors.ConnectException e) {
            connectException = true;
        }
        t.join();
        assertFalse(connectException);
    }

    @Test
    @FixFor("DBZ-2456")
    public void shouldCreateSnapshotSelectively() throws Exception {
        config = simpleConfig()
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, "connector_(.*)_" + DATABASE.getIdentifier())
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "connector_(.*).customers")
                .build();

        context = new MySqlTaskContext(config, new Filters.Builder(config).build());
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
                VerifyRecord.hasNoSourceQuery(record);
                store.add(record);
                schemaChanges.add(record);
            });
        }
        // The last poll should always return null ...
        assertThat(records).isNull();

        // There should be no schema changes ...
        assertThat(schemaChanges.recordCount()).isEqualTo(0);

        // Check the records via the store ...
        assertThat(store.databases()).containsOnly(DATABASE.getDatabaseName(), OTHER_DATABASE.getDatabaseName()); // 2 databases
        assertThat(store.collectionCount()).isEqualTo(2); // 2 databases

        Collection customers = store.collection(DATABASE.getDatabaseName(), "customers");
        assertThat(customers.numberOfCreates()).isEqualTo(0);
        assertThat(customers.numberOfUpdates()).isEqualTo(0);
        assertThat(customers.numberOfDeletes()).isEqualTo(0);
        assertThat(customers.numberOfReads()).isEqualTo(4);
        assertThat(customers.numberOfTombstones()).isEqualTo(0);
        assertThat(customers.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(customers.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection orders = store.collection(DATABASE.getDatabaseName(), "orders");
        assertThat(orders).isNull();
    }

    private String productsTableName() {
        return context.isTableIdCaseInsensitive() ? "products" : "Products";
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabaseWithSchemaChanges() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true).build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build());
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
                VerifyRecord.hasNoSourceQuery(record);
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
        }
        else {
            fail("failed to complete the snapshot within 10 seconds");
        }
    }

    @Test(expected = ConnectException.class)
    public void shouldCreateSnapshotSchemaOnlyRecovery_exception() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY).build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build());
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
                VerifyRecord.hasNoSourceQuery(record);
                store.add(record);
                schemaChanges.add(record);
            });
        }

        // should fail because we have no existing binlog information
    }

    @Test
    public void shouldCreateSnapshotSchemaOnlyRecovery() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY).build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build());
        context.start();
        context.source().setBinlogStartPoint("binlog1", 555); // manually set for happy path testing
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
                VerifyRecord.hasNoSourceQuery(record);
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
        }
        else {
            fail("failed to complete the snapshot within 10 seconds");
        }
    }

    @Test
    public void shouldSnapshotTablesInOrderSpecifiedInTableIncludeList() throws Exception {
        config = simpleConfig()
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST,
                        "connector_test_ro_(.*).orders,connector_test_ro_(.*).Products,connector_test_ro_(.*).products_on_hand,connector_test_ro_(.*).dbz_342_timetest")
                .build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build());
        context.start();
        reader = new SnapshotReader("snapshot", context);
        reader.uponCompletion(completed::countDown);
        reader.generateReadEvents();
        // Start the snapshot ...
        reader.start();
        // Poll for records ...
        List<SourceRecord> records;
        LinkedHashSet<String> tablesInOrder = new LinkedHashSet<>();
        LinkedHashSet<String> tablesInOrderExpected = getTableNamesInSpecifiedOrder("orders", "Products", "products_on_hand", "dbz_342_timetest");
        while ((records = reader.poll()) != null) {
            records.forEach(record -> {
                VerifyRecord.isValid(record);
                if (record.value() != null) {
                    tablesInOrder.add(getTableNameFromSourceRecord.apply(record));
                }
            });
        }
        assertArrayEquals(tablesInOrder.toArray(), tablesInOrderExpected.toArray());
    }

    @Test
    public void shouldSnapshotTablesInOrderSpecifiedInTablesWhitelist() throws Exception {
        config = simpleConfig()
                .with(MySqlConnectorConfig.TABLE_WHITELIST,
                        "connector_test_ro_(.*).orders,connector_test_ro_(.*).Products,connector_test_ro_(.*).products_on_hand,connector_test_ro_(.*).dbz_342_timetest")
                .build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build());
        context.start();
        reader = new SnapshotReader("snapshot", context);
        reader.uponCompletion(completed::countDown);
        reader.generateReadEvents();
        // Start the snapshot ...
        reader.start();
        // Poll for records ...
        List<SourceRecord> records;
        LinkedHashSet<String> tablesInOrder = new LinkedHashSet<>();
        LinkedHashSet<String> tablesInOrderExpected = getTableNamesInSpecifiedOrder("orders", "Products", "products_on_hand", "dbz_342_timetest");
        while ((records = reader.poll()) != null) {
            records.forEach(record -> {
                VerifyRecord.isValid(record);
                if (record.value() != null) {
                    tablesInOrder.add(getTableNameFromSourceRecord.apply(record));
                }
            });
        }
        assertArrayEquals(tablesInOrder.toArray(), tablesInOrderExpected.toArray());
    }

    @Test
    public void shouldSnapshotTablesInLexicographicalOrder() throws Exception {
        config = simpleConfig()
                .build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build());
        context.start();
        reader = new SnapshotReader("snapshot", context);
        reader.uponCompletion(completed::countDown);
        reader.generateReadEvents();
        // Start the snapshot ...
        reader.start();
        // Poll for records ...
        // Testing.Print.enable();
        List<SourceRecord> records;
        LinkedHashSet<String> tablesInOrder = new LinkedHashSet<>();
        LinkedHashSet<String> tablesInOrderExpected = getTableNamesInSpecifiedOrder("Products", "customers", "dbz_342_timetest", "orders", "products_on_hand");
        while ((records = reader.poll()) != null) {
            records.forEach(record -> {
                VerifyRecord.isValid(record);
                VerifyRecord.hasNoSourceQuery(record);
                if (record.value() != null) {
                    tablesInOrder.add(getTableNameFromSourceRecord.apply(record));
                }
            });
        }
        assertArrayEquals(tablesInOrder.toArray(), tablesInOrderExpected.toArray());
    }

    private final Function<SourceRecord, String> getTableNameFromSourceRecord = sourceRecord -> ((Struct) sourceRecord.value()).getStruct("source").getString("table");

    private LinkedHashSet<String> getTableNamesInSpecifiedOrder(String... tables) {
        return new LinkedHashSet<>(Arrays.asList(tables));
    }

    @Test
    public void shouldCreateSnapshotSchemaOnly() throws Exception {
        config = simpleConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(Heartbeat.HEARTBEAT_INTERVAL, 300_000)
                .build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build());
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

        SourceRecord heartbeatRecord = null;

        while ((records = reader.poll()) != null) {
            assertThat(heartbeatRecord).describedAs("Heartbeat record must be the last one").isNull();
            if (heartbeatRecord == null &&
                    records.size() > 0 &&
                    records.get(records.size() - 1).topic().startsWith("__debezium-heartbeat")) {
                heartbeatRecord = records.get(records.size() - 1);
            }
            records.forEach(record -> {
                if (!record.topic().startsWith("__debezium-heartbeat")) {
                    assertThat(record.sourceOffset().get("snapshot")).isEqualTo(true);
                    VerifyRecord.isValid(record);
                    VerifyRecord.hasNoSourceQuery(record);
                    store.add(record);
                    schemaChanges.add(record);
                }
            });
        }
        // The last poll should always return null ...
        assertThat(records).isNull();

        // There should be schema changes ...
        assertThat(schemaChanges.recordCount()).isEqualTo(14);

        // Check the records via the store ...
        assertThat(store.collectionCount()).isEqualTo(0);

        // Check that heartbeat has arrived
        assertThat(heartbeatRecord).isNotNull();
        assertThat(heartbeatRecord.sourceOffset().get("snapshot")).isNotEqualTo(true);

        // Make sure the snapshot completed ...
        if (completed.await(10, TimeUnit.SECONDS)) {
            // completed the snapshot ...
            Testing.print("completed the snapshot");
        }
        else {
            fail("failed to complete the snapshot within 10 seconds");
        }
    }

    private long toMicroSeconds(String duration) {
        return Duration.parse(duration).toNanos() / 1_000;
    }
}

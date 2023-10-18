/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.data.KeyValueStore;
import io.debezium.data.KeyValueStore.Collection;
import io.debezium.data.SchemaChangeHistory;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig.SnapshotTablesRowCountOrder;
import io.debezium.relational.history.MemorySchemaHistory;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 *
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public class SnapshotSourceIT extends AbstractConnectorTest {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-snapshot.txt").toAbsolutePath();
    protected final UniqueDatabase DATABASE = new UniqueDatabase("logical_server_name", "connector_test_ro")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);
    protected final UniqueDatabase OTHER_DATABASE = new UniqueDatabase("logical_server_name", "connector_test", DATABASE);
    protected final UniqueDatabase BINARY_FIELD_DATABASE = new UniqueDatabase("logical_server_name", "connector_read_binary_field_test");
    protected final UniqueDatabase CONFLICT_NAMES_DATABASE = new UniqueDatabase("logical_server_name", "mysql_dbz_6533");

    protected Configuration config;

    @Rule
    public SkipTestRule skipRule = new SkipTestRule();

    @Before
    public void beforeEach() {
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        DATABASE.createAndInitialize();
        OTHER_DATABASE.createAndInitialize();
        BINARY_FIELD_DATABASE.createAndInitialize();
        CONFLICT_NAMES_DATABASE.createAndInitialize();
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    protected Configuration.Builder simpleConfig() {
        return DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE, MySqlConnectorConfig.SnapshotLockingMode.MINIMAL)
                // Explicitly enable INCLUDE_SQL_QUERY connector option. For snapshots it should have no effect as
                // source query should not included in snapshot events.
                .with(MySqlConnectorConfig.INCLUDE_SQL_QUERY, true);
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabase() throws Exception {
        snapshotOfSingleDatabase(true, false, true);
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabaseWithoutGlobalLock() throws Exception {
        snapshotOfSingleDatabase(false, false, true);
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabaseWithoutGlobalLockAndStoreOnlyCapturedTables() throws Exception {
        snapshotOfSingleDatabase(false, true, true);
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabaseNoData() throws Exception {
        snapshotOfSingleDatabase(true, false, false);
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabaseWithoutGlobalLockNoData() throws Exception {
        snapshotOfSingleDatabase(false, false, false);
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabaseWithoutGlobalLockAndStoreOnlyCapturedTablesNoData() throws Exception {
        snapshotOfSingleDatabase(false, true, false);
    }

    private void snapshotOfSingleDatabase(boolean useGlobalLock, boolean storeOnlyCapturedTables, boolean data) throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(MySqlSnapshotChangeEventSource.class);

        final Builder builder = simpleConfig()
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("customers") + "," + DATABASE.qualifiedTableName("products"))
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true);
        if (!useGlobalLock) {
            builder
                    .with(MySqlConnectorConfig.USER, "cloud")
                    .with(MySqlConnectorConfig.PASSWORD, "cloudpass")
                    .with(MySqlConnectorConfig.TEST_DISABLE_GLOBAL_LOCKING, "true")
                    .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, storeOnlyCapturedTables);
        }
        if (!data) {
            builder.with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY);
        }
        config = builder.build();

        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        // Poll for records ...
        // Testing.Print.enable();
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
        final int schemaEventsCount = storeOnlyCapturedTables ? 8 : 14;

        SourceRecords sourceRecords = consumeRecordsByTopicUntil(
                (recordsConsumed, record) -> !record.sourceOffset().containsKey("snapshot"));

        String previousRecordTable = null;
        String previousSnapshotSourceField = null;

        for (Iterator<SourceRecord> i = sourceRecords.allRecordsInOrder().iterator(); i.hasNext();) {
            final SourceRecord record = i.next();
            VerifyRecord.isValid(record);
            VerifyRecord.hasNoSourceQuery(record);
            store.add(record);
            schemaChanges.add(record);
            final String snapshotSourceField = ((Struct) record.value()).getStruct("source").getString("snapshot");
            String currentRecordTable = ((Struct) record.value()).getStruct("source").getString("table");
            if (i.hasNext()) {
                final Object snapshotOffsetField = record.sourceOffset().get("snapshot");
                assertThat(snapshotOffsetField).isEqualTo(true);

                if (Objects.equals(snapshotSourceField, "first")) {
                    assertThat(previousRecordTable).isNull();
                }
                else if (Objects.equals(snapshotSourceField, "first_in_data_collection")) {
                    assertThat(previousRecordTable).isNotEqualTo(currentRecordTable);
                }
                else if (Objects.equals(previousSnapshotSourceField, "last_in_data_collection")) {
                    assertThat(previousRecordTable).isNotEqualTo(currentRecordTable);
                }
            }
            else {
                assertThat(record.sourceOffset().get("snapshot")).isNull();
                assertThat(snapshotSourceField).isEqualTo("last");
            }

            // When the topic is the server name, it is a DDL record and not data record
            if (!record.topic().equals(DATABASE.getServerName())) {
                previousRecordTable = currentRecordTable;
                previousSnapshotSourceField = snapshotSourceField;
            }
        }

        if (storeOnlyCapturedTables) {
            assertThat(schemaChanges.ddlRecordsForDatabaseOrEmpty("").size()
                    + schemaChanges.ddlRecordsForDatabaseOrEmpty(DATABASE.getDatabaseName()).size())
                    .isEqualTo(schemaEventsCount);
            assertThat(schemaChanges.ddlRecordsForDatabaseOrEmpty("").size()
                    + schemaChanges.ddlRecordsForDatabaseOrEmpty(OTHER_DATABASE.getDatabaseName()).size())
                    .isEqualTo(1);
        }
        else {
            assertThat(schemaChanges.ddlRecordsForDatabaseOrEmpty("").size()
                    + schemaChanges.ddlRecordsForDatabaseOrEmpty(DATABASE.getDatabaseName()).size())
                    .isEqualTo(schemaEventsCount);
            assertThat(schemaChanges.ddlRecordsForDatabaseOrEmpty("").size()
                    + schemaChanges.ddlRecordsForDatabaseOrEmpty(OTHER_DATABASE.getDatabaseName()).size())
                    .isEqualTo(useGlobalLock ? 1 : 5);
        }

        if (!useGlobalLock) {
            logInterceptor.containsMessage("Table level locking is in place, the schema will be capture in two phases, now capturing:");
        }
        else {
            logInterceptor.containsMessage("Releasing global read lock to enable MySQL writes");
        }

        if (!data) {
            return;
        }

        // Check the records via the store ...
        assertThat(store.collectionCount()).isEqualTo(2);
        Collection products = store.collection(DATABASE.getDatabaseName(), productsTableName());
        assertThat(products.numberOfCreates()).isEqualTo(0);
        assertThat(products.numberOfUpdates()).isEqualTo(0);
        assertThat(products.numberOfDeletes()).isEqualTo(0);
        assertThat(products.numberOfReads()).isEqualTo(9);
        assertThat(products.numberOfTombstones()).isEqualTo(0);
        assertThat(products.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products.numberOfValueSchemaChanges()).isEqualTo(1);

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
    }

    @Test
    public void snapshotWithBackupLocksShouldNotWaitForReads() throws Exception {
        config = simpleConfig()
                .with(MySqlConnectorConfig.USER, "cloud")
                .with(MySqlConnectorConfig.PASSWORD, "cloudpass")
                .with(MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE, MySqlConnectorConfig.SnapshotLockingMode.MINIMAL_PERCONA)
                .build();

        if (!MySqlTestConnection.isPerconaServer()) {
            return; // Skip these tests for non-Percona flavours of MySQL
        }

        final MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());
        final JdbcConnection connection = db.connect();
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    connection.executeWithoutCommitting("SELECT *, SLEEP(20) FROM products_on_hand WHERE product_id=101");
                    latch.countDown();
                }
                catch (Exception e) {
                    // Do nothing.
                }
            }
        };
        t.start();

        latch.await(10, TimeUnit.SECONDS);
        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        // Poll for records ...
        // Testing.Print.enable();
        final int recordCount = 9 + 9 + 4 + 5 + 1;
        SourceRecords sourceRecords = consumeRecordsByTopic(recordCount);
        assertThat(sourceRecords.allRecordsInOrder()).hasSize(recordCount);
        connection.connection().close();
    }

    @Test
    @FixFor("DBZ-2456")
    public void shouldCreateSnapshotSelectively() throws Exception {
        config = simpleConfig()
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, "connector_(.*)_" + DATABASE.getIdentifier())
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "connector_(.*).CUSTOMERS")
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        // Poll for records ...
        // Testing.Print.enable();
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
        SourceRecords sourceRecords = consumeRecordsByTopic(4 + 4);
        sourceRecords.allRecordsInOrder().forEach(record -> {
            VerifyRecord.isValid(record);
            VerifyRecord.hasNoSourceQuery(record);
            store.add(record);
            schemaChanges.add(record);
        });

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

    @Test
    @FixFor("DBZ-3952")
    public void shouldNotFailStreamingOnNonSnapshottedTable() throws Exception {
        config = simpleConfig()
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE.getDatabaseName())
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST,
                        DATABASE.qualifiedTableName("ORDERS") + "," + DATABASE.qualifiedTableName("CUSTOMERS"))
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, DATABASE.qualifiedTableName("ORDERS"))
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        // Poll for records ...
        Testing.Print.enable();
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
        SourceRecords sourceRecords = consumeRecordsByTopic(5);
        sourceRecords.allRecordsInOrder().forEach(record -> {
            VerifyRecord.isValid(record);
            VerifyRecord.hasNoSourceQuery(record);
            store.add(record);
            schemaChanges.add(record);
        });

        // There should be no schema changes ...
        assertThat(schemaChanges.recordCount()).isEqualTo(0);

        Collection customers = store.collection(DATABASE.getDatabaseName(), "customers");
        assertThat(customers).isNull();

        Collection orders = store.collection(DATABASE.getDatabaseName(), "orders");
        assertThat(orders.numberOfReads()).isEqualTo(5);

        try (
                MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect();
                Connection jdbc = connection.connection();
                Statement statement = jdbc.createStatement()) {
            statement.executeUpdate("INSERT INTO customers VALUES (default,'John','Lazy','john.lazy@acme.com')");
        }

        // Testing.Print.enable();
        final SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.topics()).hasSize(1);
        final SourceRecord insert = streamingRecords.recordsForTopic(DATABASE.topicForTable("customers")).get(0);
        assertThat(((Struct) insert.value()).getStruct("after").getString("email")).isEqualTo("john.lazy@acme.com");
    }

    @Test
    @FixFor("DBZ-3238")
    public void shouldSnapshotCorrectlyReadFields() throws Exception {
        config = simpleConfig()
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, "connector_read_binary_field_test_" + BINARY_FIELD_DATABASE.getIdentifier())
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, BINARY_FIELD_DATABASE.qualifiedTableName("binary_field"))
                .with(MySqlConnectorConfig.ROW_COUNT_FOR_STREAMING_RESULT_SETS, "0")
                .with(MySqlConnectorConfig.SNAPSHOT_FETCH_SIZE, "101")
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForSnapshotToBeCompleted("mysql", BINARY_FIELD_DATABASE.getServerName());

        // Poll for records ...
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(BINARY_FIELD_DATABASE.getServerName() + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(BINARY_FIELD_DATABASE.getServerName());
        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        sourceRecords.allRecordsInOrder().forEach(record -> {
            VerifyRecord.isValid(record);
            VerifyRecord.hasNoSourceQuery(record);
            store.add(record);
            schemaChanges.add(record);
        });

        // There should be no schema changes ...
        assertThat(schemaChanges.recordCount()).isEqualTo(0);

        // Check the records via the store ...
        assertThat(store.databases()).contains(BINARY_FIELD_DATABASE.getDatabaseName());
        assertThat(store.collectionCount()).isEqualTo(1);

        Collection customers = store.collection(BINARY_FIELD_DATABASE.getDatabaseName(), "binary_field");
        assertThat(customers.numberOfCreates()).isEqualTo(0);
        assertThat(customers.numberOfUpdates()).isEqualTo(0);
        assertThat(customers.numberOfDeletes()).isEqualTo(0);
        assertThat(customers.numberOfReads()).isEqualTo(1);
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabaseUsingInsertEvents() throws Exception {
        config = simpleConfig()
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, "connector_(.*)_" + DATABASE.getIdentifier())
                .with("transforms", "snapshotasinsert")
                .with("transforms.snapshotasinsert.type", "io.debezium.connector.mysql.transforms.ReadToInsertEvent")
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        // Poll for records ...
        // Testing.Print.enable();
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
        SourceRecords sourceRecords = consumeRecordsByTopic(2 * (9 + 9 + 4 + 5) + 1);
        sourceRecords.allRecordsInOrder().forEach(record -> {
            VerifyRecord.isValid(record);
            VerifyRecord.hasNoSourceQuery(record);
            store.add(record);
            schemaChanges.add(record);
        });

        // There should be no schema changes ...
        assertThat(schemaChanges.recordCount()).isEqualTo(0);

        // Check the records via the store ...
        assertThat(store.databases()).containsOnly(DATABASE.getDatabaseName(), OTHER_DATABASE.getDatabaseName()); // 2 databases
        assertThat(store.collectionCount()).isEqualTo(9); // 2 databases

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
        String expected = MySqlTestConnection.isMariaDb() ? "PT517H51M04.77S" : "PT517H51M04.78S";
        assertThat(after.get("c1")).isEqualTo(toMicroSeconds(expected));
        assertThat(after.get("c2")).isEqualTo(toMicroSeconds("-PT13H14M50S"));
        assertThat(after.get("c3")).isEqualTo(toMicroSeconds("-PT733H0M0.001S"));
        assertThat(after.get("c4")).isEqualTo(toMicroSeconds("-PT1H59M59.001S"));
        assertThat(after.get("c5")).isEqualTo(toMicroSeconds("-PT838H59M58.999999S"));
    }

    private String productsTableName() throws SQLException {
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            return db.isTableIdCaseSensitive() ? "products" : "Products";
        }
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabaseWithSchemaChanges() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true).build();

        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        // Poll for records ...
        // Testing.Print.enable();
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
        // 14 schema changes
        SourceRecords sourceRecords = consumeRecordsByTopic(14 + 9 + 9 + 4 + 5 + 1);
        sourceRecords.allRecordsInOrder().forEach(record -> {
            VerifyRecord.isValid(record);
            VerifyRecord.hasNoSourceQuery(record);
            store.add(record);
            schemaChanges.add(record);
        });

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
        String expected = MySqlTestConnection.isMariaDb() ? "PT517H51M04.77S" : "PT517H51M04.78S";
        assertThat(after.get("c1")).isEqualTo(toMicroSeconds(expected));
        assertThat(after.get("c2")).isEqualTo(toMicroSeconds("-PT13H14M50S"));
        assertThat(after.get("c3")).isEqualTo(toMicroSeconds("-PT733H0M0.001S"));
        assertThat(after.get("c4")).isEqualTo(toMicroSeconds("-PT1H59M59.001S"));
        assertThat(after.get("c5")).isEqualTo(toMicroSeconds("-PT838H59M58.999999S"));
    }

    @Test(expected = DebeziumException.class)
    public void shouldCreateSnapshotSchemaOnlyRecovery_exception() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY).build();

        // Start the connector ...
        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(MySqlConnector.class, config, (success, message, error) -> {
            exception.set(error);
        });

        // a poll is required in order to get the connector to initiate the snapshot
        waitForConnectorShutdown("mysql", DATABASE.getServerName());

        throw (RuntimeException) exception.get();
    }

    @Test
    public void shouldCreateSnapshotSchemaOnlyRecovery() throws Exception {
        Configuration.Builder builder = simpleConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("customers"))
                .with(MySqlConnectorConfig.SCHEMA_HISTORY, MemorySchemaHistory.class.getName());
        config = builder.build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // Poll for records ...
        // Testing.Print.enable();
        int recordCount = 4;
        SourceRecords sourceRecords = consumeRecordsByTopic(recordCount);
        assertThat(sourceRecords.allRecordsInOrder()).hasSize(recordCount);
        stopConnector();

        builder.with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY);
        config = builder.build();
        start(MySqlConnector.class, config);

        try (
                MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect();
                Connection jdbc = connection.connection();
                Statement statement = jdbc.createStatement()) {
            statement.executeUpdate("INSERT INTO customers VALUES (default,'John','Lazy','john.lazy@acme.com')");
        }
        recordCount = 1;
        sourceRecords = consumeRecordsByTopic(recordCount);
        assertThat(sourceRecords.allRecordsInOrder()).hasSize(recordCount);
    }

    @Test
    public void shouldSnapshotTablesInOrderSpecifiedInTableIncludeList() throws Exception {
        config = simpleConfig()
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST,
                        "connector_test_ro_(.*).orders,connector_test_ro_(.*).Products,connector_test_ro_(.*).products_on_hand,connector_test_ro_(.*).dbz_342_timetest")
                .build();
        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        // Poll for records ...
        // Testing.Print.enable();
        LinkedHashSet<String> tablesInOrder = new LinkedHashSet<>();
        LinkedHashSet<String> tablesInOrderExpected = getTableNamesInSpecifiedOrder("orders", "Products", "products_on_hand", "dbz_342_timetest");
        SourceRecords sourceRecords = consumeRecordsByTopic(9 + 9 + 5 + 1);
        sourceRecords.allRecordsInOrder().forEach(record -> {
            VerifyRecord.isValid(record);
            VerifyRecord.hasNoSourceQuery(record);
            if (record.value() != null) {
                tablesInOrder.add(getTableNameFromSourceRecord.apply(record));
            }
        });
        assertArrayEquals(tablesInOrder.toArray(), tablesInOrderExpected.toArray());
    }

    @Test
    @FixFor("DBZ-6533")
    public void shouldSnapshotTablesInOrderSpecifiedInTableIncludeListWithConflictingNames() throws Exception {
        config = simpleConfig()
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, CONFLICT_NAMES_DATABASE.getDatabaseName())
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST,
                        CONFLICT_NAMES_DATABASE.qualifiedTableName("tablename") + ","
                                + CONFLICT_NAMES_DATABASE.qualifiedTableName("another") + ","
                                + CONFLICT_NAMES_DATABASE.qualifiedTableName("tablename_suffix"))
                .build();
        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        // Poll for records ...
        // Testing.Print.enable();
        LinkedHashSet<String> tablesInOrder = new LinkedHashSet<>();
        LinkedHashSet<String> tablesInOrderExpected = getTableNamesInSpecifiedOrder("tablename", "another", "tablename_suffix");
        SourceRecords sourceRecords = consumeRecordsByTopic(3);
        sourceRecords.allRecordsInOrder().forEach(record -> {
            VerifyRecord.isValid(record);
            VerifyRecord.hasNoSourceQuery(record);
            if (record.value() != null) {
                tablesInOrder.add(getTableNameFromSourceRecord.apply(record));
            }
        });
        assertArrayEquals(tablesInOrderExpected.toArray(), tablesInOrder.toArray());
    }

    @Test
    public void shouldSnapshotTablesInRowCountOrderAsc() throws Exception {
        try (
                MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect();
                Connection jdbc = connection.connection();
                Statement statement = jdbc.createStatement()) {
            statement.execute("ANALYZE TABLE Products");
            statement.execute("ANALYZE TABLE dbz_342_timetest");
        }

        config = simpleConfig()
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST,
                        "connector_test_ro_(.*).Products,connector_test_ro_(.*).dbz_342_timetest")
                .with(MySqlConnectorConfig.SNAPSHOT_TABLES_ORDER_BY_ROW_COUNT, SnapshotTablesRowCountOrder.ASCENDING)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        // Poll for records ...
        // Testing.Print.enable();
        LinkedHashSet<String> tablesInOrder = new LinkedHashSet<>();
        LinkedHashSet<String> tablesInOrderExpected = getTableNamesInSpecifiedOrder("dbz_342_timetest", "Products");
        SourceRecords sourceRecords = consumeRecordsByTopic(1 + 9);
        sourceRecords.allRecordsInOrder().forEach(record -> {
            VerifyRecord.isValid(record);
            VerifyRecord.hasNoSourceQuery(record);
            if (record.value() != null) {
                tablesInOrder.add(getTableNameFromSourceRecord.apply(record));
            }
        });
        assertArrayEquals(tablesInOrderExpected.toArray(), tablesInOrder.toArray());
    }

    @Test
    public void shouldSnapshotTablesInRowCountOrderDesc() throws Exception {
        try (
                MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect();
                Connection jdbc = connection.connection();
                Statement statement = jdbc.createStatement()) {
            statement.execute("ANALYZE TABLE Products");
            statement.execute("ANALYZE TABLE dbz_342_timetest");
        }

        config = simpleConfig()
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST,
                        "connector_test_ro_(.*).dbz_342_timetest,connector_test_ro_(.*).Products")
                .with(MySqlConnectorConfig.SNAPSHOT_TABLES_ORDER_BY_ROW_COUNT, SnapshotTablesRowCountOrder.DESCENDING)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        // Poll for records ...
        // Testing.Print.enable();
        LinkedHashSet<String> tablesInOrder = new LinkedHashSet<>();
        LinkedHashSet<String> tablesInOrderExpected = getTableNamesInSpecifiedOrder("Products", "dbz_342_timetest");
        SourceRecords sourceRecords = consumeRecordsByTopic(9 + 1);
        sourceRecords.allRecordsInOrder().forEach(record -> {
            VerifyRecord.isValid(record);
            VerifyRecord.hasNoSourceQuery(record);
            if (record.value() != null) {
                tablesInOrder.add(getTableNameFromSourceRecord.apply(record));
            }
        });
        assertArrayEquals(tablesInOrderExpected.toArray(), tablesInOrder.toArray());
    }

    @Test
    public void shouldSnapshotTablesInLexicographicalOrder() throws Exception {
        config = simpleConfig()
                .build();
        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        // Poll for records ...
        // Testing.Print.enable();
        LinkedHashSet<String> tablesInOrder = new LinkedHashSet<>();
        LinkedHashSet<String> tablesInOrderExpected = getTableNamesInSpecifiedOrder("Products", "customers", "dbz_342_timetest", "orders", "products_on_hand");
        SourceRecords sourceRecords = consumeRecordsByTopic(9 + 9 + 5 + 4 + 1);
        sourceRecords.allRecordsInOrder().forEach(record -> {
            VerifyRecord.isValid(record);
            VerifyRecord.hasNoSourceQuery(record);
            if (record.value() != null) {
                tablesInOrder.add(getTableNameFromSourceRecord.apply(record));
            }
        });
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

        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        // Poll for records ...
        // Testing.Print.enable();
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
        // 14 schema changes
        SourceRecords sourceRecords = consumeRecordsByTopic(14 + 1);
        final List<SourceRecord> allRecords = sourceRecords.allRecordsInOrder();
        for (Iterator<SourceRecord> i = allRecords.subList(0, allRecords.size() - 1).iterator(); i.hasNext();) {
            final SourceRecord record = i.next();
            VerifyRecord.isValid(record);
            VerifyRecord.hasNoSourceQuery(record);
            store.add(record);
            schemaChanges.add(record);
            if (record.topic().startsWith("__debezium-heartbeat")) {
                continue;
            }
            final String snapshotSourceField = ((Struct) record.value()).getStruct("source").getString("snapshot");
            if (i.hasNext()) {
                final Object snapshotOffsetField = record.sourceOffset().get("snapshot");
                assertThat(snapshotOffsetField).isEqualTo(true);
                assertThat(snapshotSourceField).isEqualTo("true");
            }
            else {
                assertThat(record.sourceOffset().get("snapshot")).isNull();
                assertThat(snapshotSourceField).isEqualTo("last");
            }
        }

        SourceRecord heartbeatRecord = allRecords.get(allRecords.size() - 1);

        // There should be schema changes ...
        assertThat(schemaChanges.recordCount()).isEqualTo(14);

        // Check the records via the store ...
        assertThat(store.collectionCount()).isEqualTo(0);

        // Check that heartbeat has arrived
        assertThat(heartbeatRecord.topic()).startsWith("__debezium-heartbeat");
        assertThat(heartbeatRecord).isNotNull();
        assertThat(heartbeatRecord.sourceOffset().get("snapshot")).isNull();
    }

    private long toMicroSeconds(String duration) {
        return Duration.parse(duration).toNanos() / 1_000;
    }
}

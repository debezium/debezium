/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static io.debezium.junit.EqualityCheck.LESS_THAN_OR_EQUAL;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MBeanServer;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.data.Envelope;
import io.debezium.data.KeyValueStore;
import io.debezium.data.KeyValueStore.Collection;
import io.debezium.data.SchemaChangeHistory;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch, Jiri Pechanec
 *
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public class StreamingSourceIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-binlog.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("logical_server_name", "connector_test_ro")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private static final String SET_TLS_PROTOCOLS = "database.enabledTLSProtocols";

    private Configuration config;
    private KeyValueStore store;
    private SchemaChangeHistory schemaChanges;

    @Rule
    public SkipTestRule skipRule = new SkipTestRule();

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);

        this.store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        this.schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    protected int consumeAtLeast(int minNumber) throws InterruptedException {
        return consumeAtLeast(minNumber, 20, TimeUnit.SECONDS);
    }

    protected int consumeAtLeast(int minNumber, long timeout, TimeUnit unit) throws InterruptedException {
        final SourceRecords records = consumeRecordsByTopic(minNumber);
        final int count = records.allRecordsInOrder().size();
        records.forEach(record -> {
            VerifyRecord.isValid(record);
            store.add(record);
            schemaChanges.add(record);
        });
        Testing.print("" + count + " records");
        return count;
    }

    protected long filterAtLeast(final int minNumber, final long timeout, final TimeUnit unit) throws InterruptedException {
        final long targetNumber = minNumber;
        long startTime = System.currentTimeMillis();
        while (getNumberOfEventsFiltered() < targetNumber && (System.currentTimeMillis() - startTime) < unit.toMillis(timeout)) {
            // Ignore the records polled.
            consumeRecord();
        }
        return getNumberOfEventsFiltered();
    }

    private long getNumberOfEventsFiltered() {
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            return (long) mbeanServer.getAttribute(getStreamingMetricsObjectName("mysql", DATABASE.getServerName(), "streaming"),
                    "NumberOfEventsFiltered");
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    private long getNumberOfSkippedEvents() {
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            return (long) mbeanServer.getAttribute(getStreamingMetricsObjectName("mysql", DATABASE.getServerName(), "streaming"),
                    "NumberOfSkippedEvents");
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    protected Configuration.Builder simpleConfig() {
        return DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.USER, "replicator")
                .with(MySqlConnectorConfig.PASSWORD, "replpass")
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.INCLUDE_SQL_QUERY, false)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .with(MySqlConnector.IMPLEMENTATION_PROP, "new");
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabase() throws Exception {
        // Use the DB configuration to define the connector's configuration ...
        config = simpleConfig()
                .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // Poll for records ...
        // Testing.Print.enable();
        int expected = 9 + 9 + 4 + 5 + 1; // only the inserts for our 4 tables in this database and 1 create table
        int consumed = consumeAtLeast(expected);
        assertThat(consumed).isGreaterThanOrEqualTo(expected);

        store.sourceRecords().forEach(System.out::println);
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
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabaseWithSchemaChanges() throws Exception {
        // Use the DB configuration to define the connector's configuration ...
        config = simpleConfig()
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // Poll for records ...
        // Testing.Print.enable();
        int expectedSchemaChangeCount = 5 + 2; // 5 tables plus 2 alters
        int expected = (9 + 9 + 4 + 5 + 1) + expectedSchemaChangeCount; // only the inserts for our 4 tables in this database, plus
        // schema changes
        int consumed = consumeAtLeast(expected);
        assertThat(consumed).isGreaterThanOrEqualTo(expected);

        // There should be no schema changes ...
        assertThat(schemaChanges.recordCount()).isEqualTo(expectedSchemaChangeCount);
        final List<String> expectedAffectedTables = Arrays.asList(
                null, // CREATE DATABASE
                "Products", // CREATE TABLE
                "Products", // ALTER TABLE
                "products_on_hand", // CREATE TABLE
                "customers", // CREATE TABLE
                "orders", // CREATE TABLE
                "dbz_342_timetest" // CREATE TABLE
        );
        final List<String> affectedTables = new ArrayList<>();
        schemaChanges.forEach(record -> {
            affectedTables.add(((Struct) record.value()).getStruct("source").getString("table"));
            assertThat(((Struct) record.value()).getStruct("source").get("db")).isEqualTo(DATABASE.getDatabaseName());
        });
        assertThat(affectedTables).isEqualTo(expectedAffectedTables);

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
    }

    /**
     * Setup a DATABASE_WHITELIST filter that filters all events.
     * Verify all events are properly filtered.
     * Verify numberOfFilteredEvents metric is incremented correctly.
     */
    @Test
    @FixFor("DBZ-1206")
    public void shouldFilterAllRecordsBasedOnDatabaseWhitelistFilter() throws Exception {
        // Define configuration that will ignore all events from MySQL source.
        config = simpleConfig()
                .with(MySqlConnectorConfig.DATABASE_WHITELIST, "db-does-not-exist")
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForStreamingRunning("mysql", DATABASE.getServerName(), "streaming");

        // Lets wait for at least 35 events to be filtered.
        final int expectedFilterCount = 35;
        final long numberFiltered = filterAtLeast(expectedFilterCount, 20, TimeUnit.SECONDS);

        // All events should have been filtered.
        assertThat(numberFiltered).isGreaterThanOrEqualTo(expectedFilterCount);

        // There should be no schema changes
        assertThat(schemaChanges.recordCount()).isEqualTo(0);

        // There should be no records
        assertThat(store.collectionCount()).isEqualTo(0);

        // There should be no skipped
        assertThat(getNumberOfSkippedEvents()).isEqualTo(0);
    }

    /**
     * Setup a DATABASE_INCLUDE_LIST filter that filters all events.
     * Verify all events are properly filtered.
     * Verify numberOfFilteredEvents metric is incremented correctly.
     */
    @Test
    @FixFor("DBZ-1206")
    public void shouldFilterAllRecordsBasedOnDatabaseIncludeListFilter() throws Exception {
        // Define configuration that will ignore all events from MySQL source.
        config = simpleConfig()
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, "db-does-not-exist")
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForStreamingRunning("mysql", DATABASE.getServerName(), "streaming");

        // Lets wait for at least 35 events to be filtered.
        final int expectedFilterCount = 35;
        final long numberFiltered = filterAtLeast(expectedFilterCount, 20, TimeUnit.SECONDS);

        // All events should have been filtered.
        assertThat(numberFiltered).isGreaterThanOrEqualTo(expectedFilterCount);

        // There should be no schema changes
        assertThat(schemaChanges.recordCount()).isEqualTo(0);

        // There should be no records
        assertThat(store.collectionCount()).isEqualTo(0);

        // There should be no skipped
        assertThat(getNumberOfSkippedEvents()).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-183")
    public void shouldHandleTimestampTimezones() throws Exception {
        final UniqueDatabase REGRESSION_DATABASE = new UniqueDatabase("logical_server_name", "regression_test")
                .withDbHistoryPath(DB_HISTORY_PATH);
        REGRESSION_DATABASE.createAndInitialize();

        String tableName = "dbz_85_fractest";
        config = simpleConfig().with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, REGRESSION_DATABASE.getDatabaseName())
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, REGRESSION_DATABASE.qualifiedTableName(tableName))
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        int expectedChanges = 1; // only 1 insert

        consumeAtLeast(expectedChanges);

        // Check the records via the store ...
        List<SourceRecord> sourceRecords = store.sourceRecords();
        assertThat(sourceRecords.size()).isEqualTo(1);
        // TIMESTAMP should be converted to UTC, using the DB's (or connection's) time zone
        ZonedDateTime expectedTimestamp = ZonedDateTime.of(
                LocalDateTime.parse("2014-09-08T17:51:04.780"),
                UniqueDatabase.TIMEZONE)
                .withZoneSameInstant(ZoneOffset.UTC);

        String expectedTimestampString = expectedTimestamp.format(ZonedTimestamp.FORMATTER);
        SourceRecord sourceRecord = sourceRecords.get(0);
        Struct value = (Struct) sourceRecord.value();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        String actualTimestampString = after.getString("c4");
        assertThat(actualTimestampString).isEqualTo(expectedTimestampString);
    }

    @Test
    @FixFor("DBZ-342")
    public void shouldHandleMySQLTimeCorrectly() throws Exception {
        final UniqueDatabase REGRESSION_DATABASE = new UniqueDatabase("logical_server_name", "regression_test")
                .withDbHistoryPath(DB_HISTORY_PATH);
        REGRESSION_DATABASE.createAndInitialize();

        String tableName = "dbz_342_timetest";
        config = simpleConfig().with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, REGRESSION_DATABASE.getDatabaseName())
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, REGRESSION_DATABASE.qualifiedTableName(tableName))
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        int expectedChanges = 1; // only 1 insert

        consumeAtLeast(expectedChanges);

        // Check the records via the store ...
        List<SourceRecord> sourceRecords = store.sourceRecords();
        assertThat(sourceRecords.size()).isEqualTo(1);

        SourceRecord sourceRecord = sourceRecords.get(0);
        Struct value = (Struct) sourceRecord.value();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);

        // '517:51:04.777'
        long c1 = after.getInt64("c1");
        Duration c1Time = Duration.ofNanos(c1 * 1_000);
        Duration c1ExpectedTime = toDuration("PT517H51M4.78S");
        assertEquals(c1ExpectedTime, c1Time);
        assertEquals(c1ExpectedTime.toNanos(), c1Time.toNanos());
        assertThat(c1Time.toNanos()).isEqualTo(1864264780000000L);
        assertThat(c1Time).isEqualTo(Duration.ofHours(517).plusMinutes(51).plusSeconds(4).plusMillis(780));

        // '-13:14:50'
        long c2 = after.getInt64("c2");
        Duration c2Time = Duration.ofNanos(c2 * 1_000);
        Duration c2ExpectedTime = toDuration("-PT13H14M50S");
        assertEquals(c2ExpectedTime, c2Time);
        assertEquals(c2ExpectedTime.toNanos(), c2Time.toNanos());
        assertThat(c2Time.toNanos()).isEqualTo(-47690000000000L);
        assertTrue(c2Time.isNegative());
        assertThat(c2Time).isEqualTo(Duration.ofHours(-13).minusMinutes(14).minusSeconds(50));

        // '-733:00:00.0011'
        long c3 = after.getInt64("c3");
        Duration c3Time = Duration.ofNanos(c3 * 1_000);
        Duration c3ExpectedTime = toDuration("-PT733H0M0.001S");
        assertEquals(c3ExpectedTime, c3Time);
        assertEquals(c3ExpectedTime.toNanos(), c3Time.toNanos());
        assertThat(c3Time.toNanos()).isEqualTo(-2638800001000000L);
        assertTrue(c3Time.isNegative());
        assertThat(c3Time).isEqualTo(Duration.ofHours(-733).minusMillis(1));

        // '-1:59:59.0011'
        long c4 = after.getInt64("c4");
        Duration c4Time = Duration.ofNanos(c4 * 1_000);
        Duration c4ExpectedTime = toDuration("-PT1H59M59.001S");
        assertEquals(c4ExpectedTime, c4Time);
        assertEquals(c4ExpectedTime.toNanos(), c4Time.toNanos());
        assertThat(c4Time.toNanos()).isEqualTo(-7199001000000L);
        assertTrue(c4Time.isNegative());
        assertThat(c4Time).isEqualTo(Duration.ofHours(-1).minusMinutes(59).minusSeconds(59).minusMillis(1));

        // '-838:59:58.999999'
        long c5 = after.getInt64("c5");
        Duration c5Time = Duration.ofNanos(c5 * 1_000);
        Duration c5ExpectedTime = toDuration("-PT838H59M58.999999S");
        assertEquals(c5ExpectedTime, c5Time);
        assertEquals(c5ExpectedTime.toNanos(), c5Time.toNanos());
        assertThat(c5Time.toNanos()).isEqualTo(-3020398999999000L);
        assertTrue(c5Time.isNegative());
        assertThat(c5Time).isEqualTo(Duration.ofHours(-838).minusMinutes(59).minusSeconds(58).minusNanos(999999000));
    }

    @Test(expected = ConnectException.class)
    public void shouldFailOnSchemaInconsistency() throws Exception {
        inconsistentSchema(null);
    }

    @Test
    public void shouldWarnOnSchemaInconsistency() throws Exception {
        inconsistentSchema(EventProcessingFailureHandlingMode.WARN);
    }

    @Test
    public void shouldIgnoreOnSchemaInconsistency() throws Exception {
        inconsistentSchema(EventProcessingFailureHandlingMode.SKIP);
    }

    @Test(expected = DebeziumException.class)
    @FixFor("DBZ-1208")
    public void shouldFailOnUnknownTlsProtocol() {
        final UniqueDatabase REGRESSION_DATABASE = new UniqueDatabase("logical_server_name", "regression_test")
                .withDbHistoryPath(DB_HISTORY_PATH);
        REGRESSION_DATABASE.createAndInitialize();

        config = simpleConfig()
                .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.REQUIRED)
                .with(SET_TLS_PROTOCOLS, "TLSv1.7")
                .build();

        // Start the connector ...
        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(MySqlConnector.class, config, (success, message, error) -> exception.set(error));

        throw (RuntimeException) exception.get();
    }

    @Test
    @FixFor("DBZ-1208")
    @SkipWhenDatabaseVersion(check = LESS_THAN_OR_EQUAL, major = 5, minor = 6, reason = "MySQL 5.6 does not support SSL")
    public void shouldAcceptTls12() throws Exception {
        final UniqueDatabase REGRESSION_DATABASE = new UniqueDatabase("logical_server_name", "regression_test")
                .withDbHistoryPath(DB_HISTORY_PATH);
        REGRESSION_DATABASE.createAndInitialize();

        config = simpleConfig()
                .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.REQUIRED)
                .with(SET_TLS_PROTOCOLS, "TLSv1.2")
                .build();

        // Start the connector ...
        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(MySqlConnector.class, config, (success, message, error) -> exception.set(error));

        waitForStreamingRunning("mysql", DATABASE.getServerName(), "streaming");
        assertThat(exception.get()).isNull();
    }

    private void inconsistentSchema(EventProcessingFailureHandlingMode mode) throws InterruptedException, SQLException {
        Configuration.Builder builder = simpleConfig()
                .with(DatabaseHistory.STORE_ONLY_MONITORED_TABLES_DDL, true)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("orders"));

        if (mode == null) {
            config = builder.build();
        }
        else {
            config = builder
                    .with(MySqlConnectorConfig.INCONSISTENT_SCHEMA_HANDLING_MODE, mode)
                    .build();
        }

        // Start the connector ...
        start(MySqlConnector.class, config);

        // Poll for records ...
        // Testing.Print.enable();
        int expected = 5;
        int consumed = consumeAtLeast(expected);
        assertThat(consumed).isGreaterThanOrEqualTo(expected);

        stopConnector();
        config = builder.with(MySqlConnectorConfig.TABLE_INCLUDE_LIST,
                DATABASE.qualifiedTableName("orders") + "," + DATABASE.qualifiedTableName("customers")).build();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(MySqlConnector.class, config, (success, message, error) -> exception.set(error));

        try (
                final MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());
                final JdbcConnection connection = db.connect();
                final Connection jdbc = connection.connection();
                final Statement statement = jdbc.createStatement()) {
            statement.executeUpdate("INSERT INTO customers VALUES (default,'John','Lazy','john.lazy@acme.com')");
        }

        waitForStreamingRunning("mysql", DATABASE.getServerName(), "streaming");
        stopConnector();
        final Throwable e = exception.get();
        if (e != null) {
            throw (RuntimeException) e;
        }
    }

    private Duration toDuration(String duration) {
        return Duration.parse(duration);
    }

    private String productsTableName() throws SQLException {
        try (final MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            return db.isTableIdCaseSensitive() ? "products" : "Products";
        }
    }
}

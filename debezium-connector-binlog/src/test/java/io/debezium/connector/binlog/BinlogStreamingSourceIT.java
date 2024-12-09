/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
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
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.junit.SkipTestDependingOnDatabaseRule;
import io.debezium.connector.binlog.junit.SkipWhenDatabaseIs;
import io.debezium.connector.binlog.junit.SkipWhenDatabaseIs.Type;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.Envelope;
import io.debezium.data.KeyValueStore;
import io.debezium.data.KeyValueStore.Collection;
import io.debezium.data.SchemaChangeHistory;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 * @author Jiri Pechanec
 * @author Chris Cranford
 */
@SkipWhenDatabaseIs(value = Type.MYSQL, versions = @SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6"))
public abstract class BinlogStreamingSourceIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    protected static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-binlog.txt").toAbsolutePath();
    protected final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("logical_server_name", "connector_test_ro")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    protected Configuration config;
    private KeyValueStore store;
    private SchemaChangeHistory schemaChanges;

    @Rule
    public TestRule skipRule = new SkipTestDependingOnDatabaseRule();

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);

        this.store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        this.schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
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
            return (long) mbeanServer.getAttribute(
                    getStreamingMetricsObjectName(getConnectorName(), DATABASE.getServerName(), "streaming"),
                    "NumberOfEventsFiltered");
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    private long getNumberOfSkippedEvents() {
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            return (long) mbeanServer.getAttribute(
                    getStreamingMetricsObjectName(getConnectorName(), DATABASE.getServerName(), "streaming"),
                    "NumberOfSkippedEvents");
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    protected Configuration.Builder simpleConfig() {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.USER, "replicator")
                .with(BinlogConnectorConfig.PASSWORD, "replpass")
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, false)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NEVER);
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabase() throws Exception {
        // Use the DB configuration to define the connector's configuration ...
        config = simpleConfig()
                .build();
        // Start the connector ...
        start(getConnectorClass(), config);

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
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

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
     * Setup a DATABASE_INCLUDE_LIST filter that filters all events.
     * Verify all events are properly filtered.
     * Verify numberOfFilteredEvents metric is incremented correctly.
     */
    @Test
    @FixFor("DBZ-1206")
    public void shouldFilterAllRecordsBasedOnDatabaseIncludeListFilter() throws Exception {
        // Define configuration that will ignore all events from MySQL source.
        config = simpleConfig()
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, "db-does-not-exist")
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), "streaming");

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
        final UniqueDatabase REGRESSION_DATABASE = TestHelper.getUniqueDatabase("logical_server_name", "regression_test")
                .withDbHistoryPath(SCHEMA_HISTORY_PATH);
        REGRESSION_DATABASE.createAndInitialize();

        String tableName = "dbz_85_fractest";
        config = simpleConfig().with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, REGRESSION_DATABASE.getDatabaseName())
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, REGRESSION_DATABASE.qualifiedTableName(tableName))
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        int expectedChanges = 1; // only 1 insert

        consumeAtLeast(expectedChanges);

        String dateTime = isMariaDb() ? "2014-09-08T17:51:04.77" : "2014-09-08T17:51:04.780";

        // Check the records via the store ...
        List<SourceRecord> sourceRecords = store.sourceRecords();
        assertThat(sourceRecords.size()).isEqualTo(1);
        // TIMESTAMP should be converted to UTC, using the DB's (or connection's) time zone
        ZonedDateTime expectedTimestamp = ZonedDateTime.of(
                LocalDateTime.parse(dateTime),
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
        final UniqueDatabase REGRESSION_DATABASE = TestHelper.getUniqueDatabase("logical_server_name", "regression_test")
                .withDbHistoryPath(SCHEMA_HISTORY_PATH);
        REGRESSION_DATABASE.createAndInitialize();

        String tableName = "dbz_342_timetest";
        config = simpleConfig().with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, REGRESSION_DATABASE.getDatabaseName())
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, REGRESSION_DATABASE.qualifiedTableName(tableName))
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        int expectedChanges = 1; // only 1 insert

        consumeAtLeast(expectedChanges);

        // Check the records via the store ...
        List<SourceRecord> sourceRecords = store.sourceRecords();
        assertThat(sourceRecords.size()).isEqualTo(1);

        SourceRecord sourceRecord = sourceRecords.get(0);
        Struct value = (Struct) sourceRecord.value();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);

        String durationValue = "PT517H51M4.78S";
        long timeWithNanoSeconds = 1_864_264_780_000_000L;
        int nanos = 780;

        if (isMariaDb()) {
            durationValue = "PT517H51M4.77S";
            timeWithNanoSeconds = 1_864_264_770_000_000L;
            nanos = 770;
        }

        // '517:51:04.777'
        long c1 = after.getInt64("c1");
        Duration c1Time = Duration.ofNanos(c1 * 1_000);
        Duration c1ExpectedTime = toDuration(durationValue);
        assertEquals(c1ExpectedTime, c1Time);
        assertEquals(c1ExpectedTime.toNanos(), c1Time.toNanos());
        assertThat(c1Time.toNanos()).isEqualTo(timeWithNanoSeconds);
        assertThat(c1Time).isEqualTo(Duration.ofHours(517).plusMinutes(51).plusSeconds(4).plusMillis(nanos));

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

        // '-00:20:38.000000'
        long c6 = after.getInt64("c6");
        Duration c6Time = Duration.ofNanos(c6 * 1_000);
        Duration c6ExpectedTime = toDuration("-PT00H20M38.000000S");
        assertEquals(c6ExpectedTime, c6Time);
        assertEquals(c6ExpectedTime.toNanos(), c6Time.toNanos());
        assertThat(c6Time.toNanos()).isEqualTo(-1238000000000L);
        assertTrue(c6Time.isNegative());
        assertThat(c6Time).isEqualTo(Duration.ofHours(0).negated().minusMinutes(20).minusSeconds(38).minusNanos(0));

        // '-01:01:01.000001'
        long c7 = after.getInt64("c7");
        Duration c7Time = Duration.ofNanos(c7 * 1_000);
        Duration c7ExpectedTime = toDuration("-PT01H01M01.000001S");
        assertEquals(c7ExpectedTime, c7Time);
        assertEquals(c7ExpectedTime.toNanos(), c7Time.toNanos());
        assertThat(c7Time.toNanos()).isEqualTo(-3661000001000L);
        assertTrue(c7Time.isNegative());
        assertThat(c7Time).isEqualTo(Duration.ofHours(-1).minusMinutes(1).minusSeconds(1).minusNanos(1_000));

        // '-01:01:01.000000'
        long c8 = after.getInt64("c8");
        Duration c8Time = Duration.ofNanos(c8 * 1_000);
        Duration c8ExpectedTime = toDuration("-PT01H01M01.000000S");
        assertEquals(c8ExpectedTime, c8Time);
        assertEquals(c8ExpectedTime.toNanos(), c8Time.toNanos());
        assertThat(c8Time.toNanos()).isEqualTo(-3661000000000L);
        assertTrue(c8Time.isNegative());
        assertThat(c8Time).isEqualTo(Duration.ofHours(-1).minusMinutes(1).minusSeconds(1).minusNanos(0));

        // '-01:01:00.000000'
        long c9 = after.getInt64("c9");
        Duration c9Time = Duration.ofNanos(c9 * 1_000);
        Duration c9ExpectedTime = toDuration("-PT01H01M00.000000S");
        assertEquals(c9ExpectedTime, c9Time);
        assertEquals(c9ExpectedTime.toNanos(), c9Time.toNanos());
        assertThat(c9Time.toNanos()).isEqualTo(-3660000000000L);
        assertTrue(c9Time.isNegative());
        assertThat(c9Time).isEqualTo(Duration.ofHours(-1).minusMinutes(1).minusSeconds(0).minusNanos(0));

        // '-01:00:00.000000'
        long c10 = after.getInt64("c10");
        Duration c10Time = Duration.ofNanos(c10 * 1_000);
        Duration c10ExpectedTime = toDuration("-PT01H00M00.000000S");
        assertEquals(c10ExpectedTime, c10Time);
        assertEquals(c10ExpectedTime.toNanos(), c10Time.toNanos());
        assertThat(c10Time.toNanos()).isEqualTo(-3600000000000L);
        assertTrue(c10Time.isNegative());
        assertThat(c10Time).isEqualTo(Duration.ofHours(-1).minusMinutes(0).minusSeconds(0).minusNanos(0));

        // '-00:00:00.000000'
        long c11 = after.getInt64("c11");
        Duration c11Time = Duration.ofNanos(c11 * 1_000);
        Duration c11ExpectedTime = toDuration("PT00H00M00.000000S");
        assertEquals(c11ExpectedTime, c11Time);
        assertEquals(c11ExpectedTime.toNanos(), c11Time.toNanos());
        assertThat(c11Time.toNanos()).isEqualTo(0L);
        assertTrue(c11Time.isZero());
        assertThat(c11Time).isEqualTo(Duration.ofHours(0).minusMinutes(0).minusSeconds(0).minusNanos(0));
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

    @Test()
    @FixFor("DBZ-4029")
    public void testHeartbeatActionQueryExecuted() throws Exception {
        final String HEARTBEAT_TOPIC_PREFIX_VALUE = "myheartbeat";

        config = simpleConfig()
                .with(BinlogConnectorConfig.USER, "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with(AbstractTopicNamingStrategy.DEFAULT_HEARTBEAT_TOPIC_PREFIX, HEARTBEAT_TOPIC_PREFIX_VALUE)
                .with(Heartbeat.HEARTBEAT_INTERVAL, "100")
                .with(DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY_PROPERTY_NAME,
                        String.format("INSERT INTO %s.test_heartbeat_table (text) VALUES ('test_heartbeat');",
                                DATABASE.getDatabaseName()))
                .build();

        // Create the heartbeat table
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.execute("CREATE TABLE test_heartbeat_table (text TEXT);");
        }
        // Start the connector ...
        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(getConnectorClass(), config, (success, message, error) -> exception.set(error));

        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), "streaming");

        // Confirm that the heartbeat.action.query was executed with the heartbeat
        final String slotQuery = String.format("SELECT COUNT(*) FROM %s.test_heartbeat_table;", DATABASE.getDatabaseName());
        final JdbcConnection.ResultSetMapper<Integer> slotQueryMapper = rs -> {
            rs.next();
            return rs.getInt(1);
        };

        Awaitility.await()
                .alias("Awaiting heartbeat action query insert")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(waitTimeForRecords() * 30, TimeUnit.SECONDS)
                .until(() -> {
                    try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
                        int numOfHeartbeatActions = connection.queryAndMap(slotQuery, slotQueryMapper);
                        return numOfHeartbeatActions > 0;
                    }
                });
    }

    private void inconsistentSchema(EventProcessingFailureHandlingMode mode) throws InterruptedException, SQLException {
        final LogInterceptor logInterceptor = new LogInterceptor(BinlogStreamingChangeEventSource.class);
        Configuration.Builder builder = simpleConfig()
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("orders"));

        if (mode == null) {
            config = builder.build();
        }
        else {
            config = builder
                    .with(BinlogConnectorConfig.INCONSISTENT_SCHEMA_HANDLING_MODE, mode)
                    .build();
        }

        // Start the connector ...
        start(getConnectorClass(), config);

        // Poll for records ...
        // Testing.Print.enable();
        int expected = 5;
        int consumed = consumeAtLeast(expected);
        assertThat(consumed).isGreaterThanOrEqualTo(expected);

        stopConnector();
        config = builder.with(BinlogConnectorConfig.TABLE_INCLUDE_LIST,
                DATABASE.qualifiedTableName("orders") + "," + DATABASE.qualifiedTableName("customers")).build();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(getConnectorClass(), config, (success, message, error) -> exception.set(error));

        try (
                BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect();
                Connection jdbc = connection.connection();
                Statement statement = jdbc.createStatement()) {
            if (mode == null) {
                waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), "streaming");
            }
            statement.executeUpdate("INSERT INTO customers VALUES (default,'John','Lazy','john.lazy@acme.com')");
        }

        if (mode == null) {
            Awaitility.await().atMost(Duration.ofSeconds(waitTimeForRecords()))
                    .until(() -> logInterceptor.containsMessage("Error during binlog processing."));
            waitForEngineShutdown();
        }
        else {
            waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), "streaming");
        }
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
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            return db.isTableIdCaseSensitive() ? "products" : "Products";
        }
    }
}

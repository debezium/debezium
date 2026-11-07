/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.junit.SkipWhenDatabaseIs;
import io.debezium.connector.binlog.junit.SkipWhenDatabaseIs.Type;
import io.debezium.connector.binlog.junit.SkipWhenSchemaHistoryDisabled;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.Envelope;
import io.debezium.data.KeyValueStore;
import io.debezium.data.KeyValueStore.Collection;
import io.debezium.data.SchemaChangeHistory;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.util.MetricsHelper;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.signal.channels.FileSignalChannel;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Strings;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 * @author Jiri Pechanec
 * @author Chris Cranford
 */
@SkipWhenDatabaseIs(value = Type.MYSQL, versions = @SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6"))
public abstract class BinlogStreamingSourceIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    protected static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-binlog.txt").toAbsolutePath();
    private static final String HEARTBEAT_TABLE = "test_heartbeat_table";
    private static final String HEARTBEAT_TOPIC_PREFIX = "myheartbeat";
    private static final long PAUSE_DETECTION_MILLIS = 3000;
    protected final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("logical_server_name", "connector_test_ro")
            .withBinlogMetadataKeyColumns("$DBNAME$\\.test_heartbeat_table:text;regression_test_.*\\.(dbz_85_fractest|dbz_342_timetest):c1")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    protected Configuration config;
    private KeyValueStore store;
    private SchemaChangeHistory schemaChanges;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        DATABASE.create();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);

        this.store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        this.schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
    }

    @AfterEach
    void afterEach() {
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
        return MetricsHelper.getStreamingMetric(getConnectorName(), DATABASE.getServerName(), "streaming", "NumberOfEventsFiltered");
    }

    private long getNumberOfSkippedEvents() {
        return MetricsHelper.getStreamingMetric(getConnectorName(), DATABASE.getServerName(), "streaming", "NumberOfSkippedEvents");
    }

    protected Configuration.Builder simpleConfig() {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.USER, "replicator")
                .with(BinlogConnectorConfig.PASSWORD, "replpass")
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, false)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                // Test user has no lock tables permission
                .with(BinlogConnectorConfig.SNAPSHOT_LOCKING_MODE_PROPERTY_NAME, "none");
    }

    @Test
    void shouldCreateSnapshotOfSingleDatabase() throws Exception {
        // Use the DB configuration to define the connector's configuration ...
        config = simpleConfig()
                .build();
        // Start the connector ...
        start(getConnectorClass(), config);

        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());
        DATABASE.initialize();

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
    void shouldCreateSnapshotOfSingleDatabaseWithSchemaChanges() throws Exception {
        // Use the DB configuration to define the connector's configuration ...
        config = simpleConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());
        DATABASE.initialize();

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
                "Products", // CREATE TABLE
                "Products", // ALTER TABLE
                "products_on_hand", // CREATE TABLE
                "customers", // CREATE TABLE
                "orders", // CREATE TABLE
                "dbz_342_timetest" // CREATE TABLE
        );
        final List<String> affectedTables = new ArrayList<>();
        schemaChanges.forEach(record -> {
            final Struct source = ((Struct) record.value()).getStruct("source");
            if (!Strings.isNullOrEmpty(source.getString("table"))) {
                affectedTables.add(source.getString("table"));
                assertThat(source.get("db")).isEqualTo(DATABASE.getDatabaseName());
            }
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
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());
        DATABASE.initialize();

        // Lets wait for at least 26 events to be filtered.
        final int expectedFilterCount = 26;
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
        REGRESSION_DATABASE.create();

        String tableName = "dbz_85_fractest";
        config = simpleConfig().with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, REGRESSION_DATABASE.getDatabaseName())
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, REGRESSION_DATABASE.qualifiedTableName(tableName))
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());
        DATABASE.initialize();
        REGRESSION_DATABASE.initialize();

        int expectedChanges = 1; // only 1 insert

        consumeAtLeast(expectedChanges);

        String dateTime = isMariaDb() ? "2014-09-08T17:51:04.77" : "2014-09-08T17:51:04.780";

        // Check the records via the store ...
        List<SourceRecord> sourceRecords = store.sourceRecords();
        assertThat(sourceRecords.size()).isEqualTo(1);
        // TIMESTAMP should be converted to UTC, using the DB's (or connection's) time zone
        ZonedDateTime expectedTimestamp = ZonedDateTime.of(
                LocalDateTime.parse(dateTime),
                REGRESSION_DATABASE.getTimezone())
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
        REGRESSION_DATABASE.create();

        String tableName = "dbz_342_timetest";
        config = simpleConfig().with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, REGRESSION_DATABASE.getDatabaseName())
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, REGRESSION_DATABASE.qualifiedTableName(tableName))
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());
        DATABASE.initialize();
        REGRESSION_DATABASE.initialize();

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

    @Test
    @SkipWhenSchemaHistoryDisabled(reason = "Expects the connector to fail when the row layout no longer matches the recovered schema; the binlog "
            + "metadata mode rebuilds the schema from the TABLE_MAP event that precedes every row event, so the provoked inconsistency cannot "
            + "occur")
    void shouldFailOnSchemaInconsistency() throws Exception {
        assertThrows(ConnectException.class, () -> {
            inconsistentSchema(null);
        });
    }

    @Test
    void shouldWarnOnSchemaInconsistency() throws Exception {
        inconsistentSchema(EventProcessingFailureHandlingMode.WARN);
    }

    @Test
    void shouldIgnoreOnSchemaInconsistency() throws Exception {
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

        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());
        DATABASE.initialize();

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

    /**
     * The {@code heartbeat.action.query} must be executed with every heartbeat, i.e. on every
     * {@code heartbeat.interval.ms}, even when the captured tables are completely idle.
     */
    @Test
    @FixFor("debezium/dbz#1328")
    void shouldExecuteHeartbeatActionQueryOnIdleDatabase() throws Exception {
        createHeartbeatTable();

        config = heartbeatConfig("1000").build();

        // Start the connector ...
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());

        // Only the heartbeat action query writes to the captured tables, so it has to be executed by
        // the heartbeat itself, over and over again, on every heartbeat interval.
        Awaitility.await()
                .alias("Awaiting heartbeat action query insert on an idle database")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(heartbeatActionQueryCount())
                        .as("Heartbeat action query executions within 60 heartbeat intervals")
                        .isGreaterThanOrEqualTo(5));
    }

    /**
     * The very same connector does execute the {@code heartbeat.action.query} once a captured table
     * receives a write, so only the idle case is broken.
     */
    @Test
    @FixFor("debezium/dbz#1328")
    void shouldExecuteHeartbeatActionQueryAfterCapturedTableWrite() throws Exception {
        createHeartbeatTable();
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.execute("CREATE TABLE customers (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255));");
        }

        config = heartbeatConfig("1000").build();

        // Start the connector ...
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());

        // A write to a captured table produces a binlog event, which makes the connector dispatch
        // the heartbeat, which in turn executes the heartbeat action query.
        Awaitility.await()
                .alias("Awaiting heartbeat action query insert after a captured table write")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .until(() -> {
                    try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
                        connection.execute("INSERT INTO customers (name) VALUES ('test_heartbeat');");
                    }
                    return heartbeatActionQueryCount() > 0;
                });
    }

    /**
     * With the heartbeat disabled the {@code heartbeat.action.query} must never be executed.
     */
    @Test
    @FixFor("debezium/dbz#1328")
    void shouldNotExecuteHeartbeatActionQueryWhenHeartbeatIsDisabled() throws Exception {
        createHeartbeatTable();

        config = heartbeatConfig("0").build();

        // Start the connector ...
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());

        // The heartbeat is disabled, so the action query must never be executed.
        Awaitility.await()
                .alias("Awaiting the heartbeat action query not being executed")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(15, TimeUnit.SECONDS)
                .during(5, TimeUnit.SECONDS)
                .until(() -> heartbeatActionQueryCount() == 0);
    }

    /**
     * A failing {@code heartbeat.action.query} must not stop the connector; the heartbeat records are
     * still produced.
     */
    @Test
    @FixFor("debezium/dbz#1328")
    void shouldKeepStreamingWhenHeartbeatActionQueryFails() throws Exception {
        config = simpleConfig()
                .with(BinlogConnectorConfig.USER, "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with(Heartbeat.HEARTBEAT_INTERVAL, "1000")
                .with(AbstractTopicNamingStrategy.TOPIC_HEARTBEAT_PREFIX, HEARTBEAT_TOPIC_PREFIX)
                .with(DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY_PROPERTY_NAME,
                        String.format("INSERT INTO %s.no_such_table (text) VALUES ('test_heartbeat');", DATABASE.getDatabaseName()))
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());

        // The action query fails, but the connector keeps streaming and the heartbeat records are still produced.
        final SourceRecords records = consumeRecordsByTopicUntil((recordsConsumed, record) -> record.topic().startsWith(HEARTBEAT_TOPIC_PREFIX));

        assertThat(heartbeatRecords(records)).isNotEmpty();
        assertConnectorIsRunning();
    }

    /**
     * The heartbeat records have to be produced on the interval even when the captured tables are idle.
     */
    @Test
    @FixFor("debezium/dbz#1328")
    void shouldEmitHeartbeatRecordsOnIdleDatabase() throws Exception {
        config = simpleConfig()
                .with(Heartbeat.HEARTBEAT_INTERVAL, "1000")
                .with(AbstractTopicNamingStrategy.TOPIC_HEARTBEAT_PREFIX, HEARTBEAT_TOPIC_PREFIX)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());

        // The captured tables are idle, so the heartbeat records have to be produced by the heartbeat itself.
        final AtomicInteger heartbeatRecordCount = new AtomicInteger();
        final SourceRecords records = consumeRecordsByTopicUntil((recordsConsumed, record) -> {
            if (record.topic().startsWith(HEARTBEAT_TOPIC_PREFIX)) {
                heartbeatRecordCount.incrementAndGet();
            }
            return heartbeatRecordCount.get() >= 3;
        });

        final List<SourceRecord> heartbeats = heartbeatRecords(records);
        assertThat(heartbeats).hasSizeGreaterThanOrEqualTo(3);
        heartbeats.forEach(record -> {
            assertThat(record.sourceOffset()).isNotEmpty();
            assertThat(record.sourceOffset().get(BinlogSourceInfo.BINLOG_FILENAME_OFFSET_KEY)).isNotNull();
            assertThat(record.sourceOffset().get(BinlogSourceInfo.BINLOG_POSITION_OFFSET_KEY)).isNotNull();
        });
    }

    /**
     * The two threads that dispatch the heartbeat are mutually exclusive, so the action query is executed
     * about once per interval and not once per received binlog event.
     */
    @Test
    @FixFor("debezium/dbz#1328")
    void shouldExecuteHeartbeatActionQueryOncePerIntervalWhileCapturedTableIsWrittenTo() throws Exception {
        createHeartbeatTable();

        config = heartbeatConfig("1000").build();

        // Start the connector ...
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());

        // Create the captured tables and their content ...
        DATABASE.initialize();
        consumeAtLeast(28);

        final int executionsBefore = heartbeatActionQueryCount();
        final long windowMillis = 10_000;
        final long deadline = System.currentTimeMillis() + windowMillis;
        int insert = 0;

        // Write to a captured table continuously, so that the binlog client thread keeps handling events ...
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            while (System.currentTimeMillis() < deadline) {
                connection.execute(String.format("INSERT INTO customers (first_name, last_name, email) VALUES ('John', 'Doe', 'doe%d@acme.com');", insert++));
                // Pace the writes, so that the connector is not flooded with binlog events.
                Thread.sleep(100);
            }
        }

        final int executions = heartbeatActionQueryCount() - executionsBefore;

        // The action query has to be executed about once per heartbeat interval, not twice ...
        assertThat(executions)
                .as("Heartbeat action query executions within %s heartbeat intervals", windowMillis / 1000)
                .isLessThanOrEqualTo((int) (windowMillis / 1000) + 2);
    }

    /**
     * While the streaming source is paused, neither the heartbeat record nor the heartbeat action
     * query may be produced; both have to be produced again once the streaming is resumed.
     * <p>
     * The pause is reached end to end: the {@code execute-snapshot} signal of type {@code BLOCKING} is
     * delivered through the file signal channel, which pauses the streaming, and the blocking snapshot
     * is held up by a {@code LOCK TABLES ... WRITE} on the snapshotted table.
     * <p>
     * The heartbeat action query is read-only, so it produces no binlog event of its own and the
     * database stays completely idle while the heartbeat is measured. The heartbeat record and the
     * heartbeat action query are emitted in the same guarded block, so the number of heartbeat records
     * is the number of action query executions.
     */
    @Test
    @FixFor("debezium/dbz#1328")
    void shouldNotProduceHeartbeatWhileStreamingIsPaused() throws Exception {
        final String snapshotTable = "test_pause_snapshot_table";
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.execute("CREATE TABLE " + snapshotTable + " (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255));");
            connection.execute("INSERT INTO " + snapshotTable + " (name) VALUES ('a'), ('b'), ('c');");
        }

        final Path signalsFile = Files.createTestingPath("binlog-streaming-source-signals.txt");
        Files.delete(signalsFile);

        config = pausedHeartbeatConfig("1000")
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "file")
                .with(FileSignalChannel.SIGNAL_FILE, signalsFile.toString())
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 200)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());

        final AtomicInteger heartbeatCount = new AtomicInteger();

        // The heartbeat has to run before the pause, so that there is something to compare against.
        Awaitility.await()
                .alias("Awaiting the heartbeat to run before the pause")
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> consumeHeartbeatRecords(heartbeatCount) >= 3);

        final AtomicLong lastChangeAt = new AtomicLong(System.currentTimeMillis());

        try (BinlogTestConnection lockConnection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            // Hold the snapshotted table, so that the blocking snapshot cannot complete.
            lockConnection.execute("LOCK TABLES " + snapshotTable + " WRITE");

            // Request the blocking snapshot through the file signal channel.
            java.nio.file.Files.write(signalsFile, blockingSnapshotSignal(snapshotTable).getBytes(StandardCharsets.UTF_8));

            // The heartbeat stops while the streaming source is paused, so no heartbeat record may be
            // produced for a while.
            Awaitility.await()
                    .alias("Awaiting the streaming source to be paused")
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .atMost(60, TimeUnit.SECONDS)
                    .until(() -> {
                        final int heartbeatsBefore = heartbeatCount.get();
                        if (consumeHeartbeatRecords(heartbeatCount) > heartbeatsBefore) {
                            lastChangeAt.set(System.currentTimeMillis());
                        }
                        return System.currentTimeMillis() - lastChangeAt.get() >= PAUSE_DETECTION_MILLIS;
                    });

            final int heartbeatsBeforePause = heartbeatCount.get();
            final long pauseDetectedAt = System.currentTimeMillis();

            // Keep the pause alive for a while and check that nothing is produced during it.
            Thread.sleep(2000);
            final int heartbeatsAfterPause = consumeHeartbeatRecords(heartbeatCount);

            // Releasing the lock lets the blocking snapshot finish, which resumes the streaming.
            lockConnection.execute("UNLOCK TABLES");
            final long pauseDurationMillis = System.currentTimeMillis() - pauseDetectedAt;

            // The heartbeat record, and with it the heartbeat action query, has to be produced again
            // once the streaming is resumed.
            Awaitility.await()
                    .alias("Awaiting the heartbeat to resume after the blocking snapshot")
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(consumeHeartbeatRecords(heartbeatCount))
                            .as("Heartbeat records produced after the streaming source was resumed")
                            .isGreaterThan(heartbeatsAfterPause));

            final int heartbeatsAfterResume = heartbeatCount.get();

            Testing.print(String.format(
                    "Heartbeat while the streaming source is paused: pauseDurationMillis=%d heartbeats=%d/%d/%d",
                    pauseDurationMillis, heartbeatsBeforePause, heartbeatsAfterPause, heartbeatsAfterResume));

            // Neither the heartbeat record nor the heartbeat action query may be produced while the
            // streaming source is paused ...
            assertThat(heartbeatsAfterPause)
                    .as("Heartbeat records produced while the streaming source was paused")
                    .isEqualTo(heartbeatsBeforePause);
        }
    }

    private Configuration.Builder heartbeatConfig(String heartbeatIntervalMs) {
        return simpleConfig()
                .with(BinlogConnectorConfig.USER, "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with(Heartbeat.HEARTBEAT_INTERVAL, heartbeatIntervalMs)
                .with(DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY_PROPERTY_NAME,
                        String.format("INSERT INTO %s.%s (text) VALUES ('test_heartbeat');", DATABASE.getDatabaseName(), HEARTBEAT_TABLE));
    }

    private void createHeartbeatTable() throws SQLException {
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.execute("CREATE TABLE " + HEARTBEAT_TABLE + " (text TEXT);");
        }
    }

    private int heartbeatActionQueryCount() {
        final String countQuery = String.format("SELECT COUNT(*) FROM %s.%s;", DATABASE.getDatabaseName(), HEARTBEAT_TABLE);
        final JdbcConnection.ResultSetMapper<Integer> countMapper = rs -> {
            rs.next();
            return rs.getInt(1);
        };
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            return connection.queryAndMap(countQuery, countMapper);
        }
        catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private List<SourceRecord> heartbeatRecords(SourceRecords records) {
        return records.allRecordsInOrder().stream()
                .filter(record -> record.topic().startsWith(HEARTBEAT_TOPIC_PREFIX))
                .toList();
    }

    private int consumeHeartbeatRecords(AtomicInteger heartbeatCount) throws InterruptedException {
        heartbeatCount.addAndGet(heartbeatRecords(consumeAvailableRecordsByTopic()).size());
        return heartbeatCount.get();
    }

    private Configuration.Builder pausedHeartbeatConfig(String heartbeatIntervalMs) {
        return simpleConfig()
                .with(BinlogConnectorConfig.USER, "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with(Heartbeat.HEARTBEAT_INTERVAL, heartbeatIntervalMs)
                .with(AbstractTopicNamingStrategy.TOPIC_HEARTBEAT_PREFIX, HEARTBEAT_TOPIC_PREFIX)
                // The action query is read-only, so that it produces no binlog event of its own and the
                // database stays completely idle while the heartbeat is measured.
                .with(DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY_PROPERTY_NAME, "SELECT 1");
    }

    private String blockingSnapshotSignal(String tableName) {
        return String.format(
                "{\"id\":\"binlog-streaming-source-1\",\"type\":\"execute-snapshot\",\"data\":{\"data-collections\":[\"%s\"],\"type\":\"BLOCKING\"}}",
                DATABASE.qualifiedTableName(tableName));
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

        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());
        DATABASE.initialize();

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

    /**
     * Test that verifies DML statements like REPLACE INTO (used by pt-table-checksum) are properly
     * filtered out and not processed as DDL statements. This prevents schema history corruption
     * when using tools like Percona's pt-table-checksum which generates STATEMENT-based binlog
     * entries with REPLACE INTO queries.
     */
    @Test
    @FixFor("DBZ-9428")
    public void shouldFilterDmlStatementsFromDdlProcessing() throws Exception {
        config = simpleConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());
        DATABASE.initialize();

        // Create a test table that simulates pt-table-checksum's checksums table
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // Create checksums table similar to pt-table-checksum
                connection.execute(
                        "CREATE TABLE IF NOT EXISTS checksums (" +
                                "db VARCHAR(64) NOT NULL, " +
                                "tbl VARCHAR(64) NOT NULL, " +
                                "chunk INT NOT NULL, " +
                                "chunk_time FLOAT NULL, " +
                                "chunk_index VARCHAR(200) NULL, " +
                                "lower_boundary TEXT NULL, " +
                                "upper_boundary TEXT NULL, " +
                                "this_crc CHAR(40) NOT NULL, " +
                                "this_cnt INT NOT NULL, " +
                                "master_crc CHAR(40) NULL, " +
                                "master_cnt INT NULL, " +
                                "ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, " +
                                "PRIMARY KEY (db, tbl, chunk)" +
                                ")");

                // Execute REPLACE INTO statement similar to what pt-table-checksum does
                // In ROW format, REPLACE INTO becomes INSERT (or DELETE+INSERT if key exists)
                connection.execute(
                        "REPLACE INTO checksums (db, tbl, chunk, this_crc, this_cnt) " +
                                "VALUES ('" + DATABASE.getDatabaseName() + "', 'products', 1, 'abc123', 10)");

                // Also test INSERT, UPDATE, DELETE to ensure they're all handled correctly
                connection.execute(
                        "INSERT INTO checksums (db, tbl, chunk, this_crc, this_cnt) " +
                                "VALUES ('" + DATABASE.getDatabaseName() + "', 'customers', 1, 'def456', 5)");

                connection.execute(
                        "UPDATE checksums SET this_cnt = 11 WHERE db = '" + DATABASE.getDatabaseName() + "' AND tbl = 'products'");

                connection.execute(
                        "DELETE FROM checksums WHERE db = '" + DATABASE.getDatabaseName() + "' AND tbl = 'customers'");
            }
        }

        // With SNAPSHOT_MODE.NEVER and INCLUDE_SCHEMA_CHANGES=true, the connector replays
        // the full binlog from the beginning, producing initial events from createAndInitialize():
        // 28 DML (9 products + 9 products_on_hand + 4 customers + 5 orders + 1 timetest)
        // + 7 DDL (1 create db + 5 create table + 1 alter table) = 35 initial events
        // Plus our test events: 1 DDL (CREATE TABLE checksums) + 4 DML = 5 test events
        final int initialExpected = (9 + 9 + 4 + 5 + 1) + (5 + 2);
        final int testExpected = 5; // 1 DDL (CREATE TABLE) + 4 DML (REPLACE, INSERT, UPDATE, DELETE)
        int consumed = consumeAtLeast(initialExpected + testExpected);
        assertThat(consumed).isGreaterThanOrEqualTo(initialExpected + testExpected);

        // Verify DDL event: CREATE TABLE checksums should be in schema changes
        assertThat(schemaChanges.recordCount()).isGreaterThanOrEqualTo(1);
        AtomicBoolean foundChecksumsTableDdl = new AtomicBoolean(false);
        schemaChanges.forEach(record -> {
            Struct value = (Struct) record.value();
            if (value != null) {
                Struct source = value.getStruct("source");
                if (source != null && "checksums".equals(source.getString("table"))) {
                    foundChecksumsTableDdl.set(true);
                }
            }
        });
        assertThat(foundChecksumsTableDdl.get()).as("Expected CREATE TABLE checksums DDL event").isTrue();

        // Verify DML events were processed: checksums table should have data events
        Collection checksums = store.collection(DATABASE.getDatabaseName(), "checksums");
        assertThat(checksums).as("Expected checksums collection in store").isNotNull();
        // Exactly 4 DML events: 2 creates (REPLACE + INSERT), 1 update, 1 delete
        assertThat(checksums.numberOfCreates()).as("Expected 2 creates (REPLACE + INSERT)").isEqualTo(2);
        assertThat(checksums.numberOfUpdates()).as("Expected 1 update").isEqualTo(1);
        assertThat(checksums.numberOfDeletes()).as("Expected 1 delete").isEqualTo(1);

        // The primary verification: connector is still running without schema parsing errors
        // If DML was incorrectly processed as DDL, the connector would have failed
        assertConnectorIsRunning();

        stopConnector();
    }
}

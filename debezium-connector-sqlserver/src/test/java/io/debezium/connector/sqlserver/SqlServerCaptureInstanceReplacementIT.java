/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Testing;

/**
 * Integration test for the detection of a replaced SQL Server capture instance and the migration of the table it
 * affects.
 */
public class SqlServerCaptureInstanceReplacementIT extends AbstractAsyncEngineConnectorTest {

    private static final String ORDERS = "orders";
    private static final String ITEMS = "items";
    private static final String ORDERS_TOPIC = "server1.testDB1.dbo.orders";
    private static final String ITEMS_TOPIC = "server1.testDB1.dbo.items";

    private SqlServerConnection connection;

    @BeforeEach
    void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void after() throws SQLException {
        if (connection != null) {
            try {
                // The capture job has to be stopped, or the drop of the database in the next test waits for the
                // transaction of the capture job. The next test creates a new database, whose capture job is started
                // by sp_cdc_enable_db.
                TestHelper.stopCaptureJob(connection);
            }
            finally {
                connection.close();
            }
        }
    }

    @Test
    @FixFor("debezium/dbz#2940")
    void captureInstanceReplacedBeforeTheFirstPollIsMigrated() throws Exception {
        // The connector detects a new capture instance through the LSN window, which is empty when the connector's
        // offset carries no commit LSN. A capture job that has published nothing gives the connector such an offset,
        // and the capture instance is replaced before the connector's first successful poll.
        startConnectorWithoutCommitLsn(TestHelper.defaultConfig(), ORDERS);

        // The capture instance is replaced while the connector is running but before its first successful poll
        connection.execute("ALTER TABLE orders ADD amount int");
        TestHelper.disableTableCdc(connection, "orders");
        TestHelper.enableTableCdc(connection, "orders", "orders_amount");
        connection.execute("INSERT INTO orders VALUES (2, 'paid', 20)");
        TestHelper.startCaptureJob(connection);

        SourceRecords records = consumeRecordsByTopic(1);
        assertSingleRecord(records, ORDERS_TOPIC, "amount");
        // The snapshot creates the table and the replaced capture instance migrates it once
        assertThat(TestHelper.countSchemaChanges("CREATE")).isEqualTo(1);
        assertThat(TestHelper.countSchemaChanges("ALTER")).isEqualTo(1);

        // The offset carries a commit LSN now, so the LSN window nominates the capture instance as well
        connection.execute("ALTER TABLE orders ADD quantity int");
        TestHelper.disableTableCdc(connection, "orders");
        TestHelper.enableTableCdc(connection, "orders", "orders_quantity");
        connection.execute("INSERT INTO orders VALUES (3, 'shipped', 30, 5)");

        records = consumeRecordsByTopic(1);
        assertSingleRecord(records, ORDERS_TOPIC, "amount", "quantity");
        // The capture instance is nominated twice, but the table is migrated only once
        assertThat(TestHelper.countSchemaChanges("ALTER")).isEqualTo(2);

        // The capture instance is replaced again, but the schema of the table did not change
        TestHelper.disableTableCdc(connection, "orders");
        TestHelper.enableTableCdc(connection, "orders", "orders_unchanged");
        connection.execute("INSERT INTO orders VALUES (4, 'delivered', 40, 6)");

        records = consumeRecordsByTopic(1);
        assertSingleRecord(records, ORDERS_TOPIC, "amount", "quantity");
        assertThat(TestHelper.countSchemaChanges("ALTER")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#2940")
    void droppedColumnIsNotMigrated() throws Exception {
        // A column is dropped from the captured table while the connector streams and CDC is not re-enabled on the
        // table. The capture instance keeps the dropped column, so the change table still carries it and the connector
        // must keep decoding it with the schema it has, instead of migrating the table to a schema that no longer
        // describes the change table.
        startConnectorWithoutCommitLsn(TestHelper.defaultConfig(), ORDERS);

        // The column is dropped before the connector's first successful poll, so the capture instance keeps it
        connection.execute("ALTER TABLE orders DROP COLUMN status");
        connection.execute("INSERT INTO orders (id) VALUES (1)");
        TestHelper.startCaptureJob(connection);

        final SourceRecords records = consumeRecordsByTopic(1);
        // The connector keeps the column in the schema, so it keeps decoding the change table with it
        assertSingleRecord(records, ORDERS_TOPIC);
        // The table is not migrated, so the schema keeps the dropped column and no ALTER is emitted
        assertThat(TestHelper.countSchemaChanges("ALTER")).isZero();
    }

    @ParameterizedTest(name = "the row of the items table is inserted first: {0}")
    @ValueSource(booleans = { true, false })
    @FixFor("debezium/dbz#2940")
    void twoCaptureInstancesReplacedBeforeTheFirstPollAreBothMigrated(boolean itemsFirst) throws Exception {
        // Both tables have their capture instance replaced before the connector's first successful poll, so both
        // tables are migrated before their first change is read.
        startConnectorWithoutCommitLsn(TestHelper.defaultConfig(), ORDERS, ITEMS);

        connection.execute("ALTER TABLE orders ADD amount int");
        TestHelper.disableTableCdc(connection, ORDERS);
        TestHelper.enableTableCdc(connection, ORDERS, "orders_amount");
        connection.execute("ALTER TABLE items ADD amount int");
        TestHelper.disableTableCdc(connection, ITEMS);
        TestHelper.enableTableCdc(connection, ITEMS, "items_amount");

        // The rows are inserted in both orders, because the order of the changes decides which of them the connector
        // reads first, and the change that is read first must carry the new column as well.
        if (itemsFirst) {
            connection.execute("INSERT INTO items VALUES (1, 'paid', 20)");
            connection.execute("INSERT INTO orders VALUES (1, 'paid', 10)");
        }
        else {
            connection.execute("INSERT INTO orders VALUES (1, 'paid', 10)");
            connection.execute("INSERT INTO items VALUES (1, 'paid', 20)");
        }
        TestHelper.startCaptureJob(connection);

        final SourceRecords records = consumeRecordsByTopic(2);
        // Both tables are migrated before their change is read, so both rows carry the new column
        assertSingleRecord(records, ORDERS_TOPIC, "amount");
        assertSingleRecord(records, ITEMS_TOPIC, "amount");
        assertThat(TestHelper.countSchemaChanges("ALTER")).isEqualTo(2);
    }

    @ParameterizedTest(name = "the row of the items table is inserted first: {0}")
    @ValueSource(booleans = { true, false })
    @FixFor("debezium/dbz#2940")
    void twoCaptureInstancesForEveryTableAreBothMigrated(boolean itemsFirst) throws Exception {
        // The retired capture instance is not disabled, so the connector finds two capture instances for every table
        // and the migration stays in the queue, where the LSN window is supposed to order it. The schema changes are
        // included, so that the schema change records of the migrations are part of the stream.
        startConnectorWithoutCommitLsn(TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_CHANGES, true), ORDERS, ITEMS);

        // Every table gets a second capture instance, which captures the new column
        connection.execute("ALTER TABLE orders ADD amount int");
        TestHelper.enableTableCdc(connection, ORDERS, "orders_amount");
        connection.execute("ALTER TABLE items ADD amount int");
        TestHelper.enableTableCdc(connection, ITEMS, "items_amount");

        // The rows are inserted in both orders, because the order of the changes decides which of them the connector
        // reads first, and the change that is read first must carry the new column as well.
        if (itemsFirst) {
            connection.execute("INSERT INTO items VALUES (1, 'paid', 20)");
            connection.execute("INSERT INTO orders VALUES (1, 'paid', 10)");
        }
        else {
            connection.execute("INSERT INTO orders VALUES (1, 'paid', 10)");
            connection.execute("INSERT INTO items VALUES (1, 'paid', 20)");
        }
        TestHelper.startCaptureJob(connection);

        // The rows are read from the new capture instance, because the connector stops reading the retired one at its
        // stop LSN, which is the start LSN of the new one
        final List<SourceRecord> records = consumeRecordsUntilDataRecordsAndAlter(2);
        assertSingleRecord(records, ORDERS_TOPIC, "amount");
        assertSingleRecord(records, ITEMS_TOPIC, "amount");

        // The schema change record is not ahead of the first data record that follows it, so that a restart from its
        // offset does not skip that record
        assertSchemaChangeIsBeforeTheFirstDataRecord(records);
    }

    @Test
    @FixFor("debezium/dbz#2940")
    void columnFilterOverrideDoesNotMigrateTheTable() throws Exception {
        // With the column filter override the captured columns are the columns of the source table and not the
        // columns of the change table, so they cannot tell whether the change table can be decoded with the cached
        // schema.
        startConnectorWithoutCommitLsn(TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.CDC_COLUMN_FILTER_OVERRIDE, true), ORDERS);

        // The column is added to the source table, but CDC is not re-enabled, so the change table does not capture it
        connection.execute("ALTER TABLE orders ADD amount int");
        connection.execute("INSERT INTO orders (id, status) VALUES (1, 'paid')");
        TestHelper.startCaptureJob(connection);

        // The change table does not have the new column, so the connector cannot read it. It restarts and takes a
        // new snapshot, which delivers the row and shows that the connector has polled the change table.
        consumeRecordsByTopic(1);
        // The table is not migrated, because the captured columns are the columns of the source table and not the
        // columns of the change table
        assertThat(TestHelper.countSchemaChanges("ALTER")).isZero();
    }

    @Test
    @FixFor("debezium/dbz#2940")
    void restartFromTheSchemaChangeRecordReReadsFromTheCaptureStart() throws Exception {
        // The connector streams, so that its position is a real LSN
        final Configuration config = startConnectorStreaming(ORDERS);

        // The connector reads a row, so that its position is ahead of the position of the snapshot
        connection.execute("INSERT INTO orders VALUES (1, 'paid')");
        final List<SourceRecord> firstRecords = consumeRecordsUntilDataRecord(ORDERS_TOPIC);
        assertSingleRecord(firstRecords, ORDERS_TOPIC);

        // The capture instance is replaced while the connector streams
        connection.execute("ALTER TABLE orders ADD amount int");
        TestHelper.disableTableCdc(connection, "orders");
        TestHelper.enableTableCdc(connection, "orders", "orders_amount");
        connection.execute("INSERT INTO orders VALUES (2, 'paid', 20)");

        final List<SourceRecord> consumed = consumeRecordsUntilAlterAndDataRecord(ORDERS_TOPIC);
        final SourceRecord schemaChange = firstAlterSchemaChange(consumed);
        final SourceRecord dataRecord = firstDataRecord(consumed);

        final Lsn schemaChangeCommitLsn = commitLsn(schemaChange);
        final Lsn dataCommitLsn = commitLsn(dataRecord);

        // The record carries the start LSN of the new capture instance, which is the boundary the capture instance
        // defines: the rows of the retired capture instance are below it and the rows of the new one are at or above it
        assertThat(schemaChangeCommitLsn).isEqualTo(captureInstanceStartLsn("orders_amount"));

        // The schema change record is not ahead of the record that follows it in the stream, so that a restart from its
        // offset does not skip that record
        assertThat(schemaChangeCommitLsn).isLessThanOrEqualTo(dataCommitLsn);

        // A restart from the offset of the schema change record re-reads from the start of the capture instance, so the
        // row of the new capture instance is delivered again, which is a duplicate and not a loss
        stopConnector();
        storeOffsets(config, Map.of(schemaChange.sourcePartition(), schemaChange.sourceOffset()));
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords reread = consumeRecordsByTopic(1);
        assertSingleRecord(reread, ORDERS_TOPIC, "amount");
        // The row of the retired capture instance is not read again, because it was committed before the capture
        // instance was replaced and is therefore below its start LSN
        assertThat(idOf(reread.recordsForTopic(ORDERS_TOPIC).get(0))).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#2940")
    void schemaChangeRecordCarriesTheStartLsnOfTheNewCaptureInstance() throws Exception {
        // The capture instance of the items table is not replaced, so the change tables still hold the LSN it started
        // at, which is below the position of the connector. The connector could therefore resume from its position.
        final Configuration config = startConnectorStreaming(ORDERS, ITEMS);

        // The connector reads a row, so that its position is ahead of the position of the snapshot
        connection.execute("INSERT INTO orders VALUES (1, 'paid')");
        final List<SourceRecord> firstRecords = consumeRecordsUntilDataRecord(ORDERS_TOPIC);
        assertSingleRecord(firstRecords, ORDERS_TOPIC);
        final SourceRecord firstRecord = firstDataRecord(firstRecords);
        final Lsn connectorPosition = commitLsn(firstRecord);

        // The capture instance of the orders table is replaced while the connector is stopped, so that the position of
        // the connector does not move before the migration is dispatched
        stopConnector();
        storeOffsets(config, Map.of(firstRecord.sourcePartition(), firstRecord.sourceOffset()));
        connection.execute("ALTER TABLE orders ADD amount int");
        TestHelper.disableTableCdc(connection, "orders");
        TestHelper.enableTableCdc(connection, "orders", "orders_amount");
        final Lsn captureStartLsn = captureInstanceStartLsn("orders_amount");
        connection.execute("INSERT INTO orders VALUES (2, 'paid', 20)");
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final List<SourceRecord> consumed = consumeRecordsUntilAlterAndDataRecord(ORDERS_TOPIC);
        final SourceRecord schemaChange = firstAlterSchemaChange(consumed);
        final SourceRecord dataRecord = firstDataRecord(consumed);

        final Lsn schemaChangeCommitLsn = commitLsn(schemaChange);

        // The record carries the start LSN of the new capture instance and not the position of the connector, even though
        // the connector could resume from its own position: the start LSN is the boundary the capture instance defines,
        // the rows of the retired capture instance are below it and the rows of the new one are at or above it
        assertThat(schemaChangeCommitLsn).isEqualTo(captureStartLsn);
        assertThat(schemaChangeCommitLsn).isNotEqualTo(connectorPosition);

        // The schema change record is not ahead of the record that follows it in the stream, so that a restart from its
        // offset does not skip that record
        assertThat(schemaChangeCommitLsn)
                .isLessThan(commitLsn(dataRecord));

        // A restart from the offset of the schema change record re-reads from the start of the capture instance, so the
        // row of the new capture instance is delivered again, which is a duplicate and not a loss
        stopConnector();
        storeOffsets(config, Map.of(schemaChange.sourcePartition(), schemaChange.sourceOffset()));
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords reread = consumeRecordsByTopic(1);
        assertSingleRecord(reread, ORDERS_TOPIC, "amount");
        // The row of the retired capture instance is not read again, because it was committed before the capture
        // instance was replaced and is therefore below its start LSN
        assertThat(idOf(reread.recordsForTopic(ORDERS_TOPIC).get(0))).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#2940")
    void schemaChangeRecordIsNotAheadOfTheRowsThatFollowIt() throws Exception {
        // The connector's offset carries no commit LSN. The row of the other table is committed before the capture
        // instance of the orders table is replaced, so it is below the start LSN of that capture instance and it
        // precedes the schema change record in the stream. The schema changes are included, so that the schema change
        // record is part of the stream.
        startConnectorWithoutCommitLsn(TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_CHANGES, true), ORDERS, ITEMS);

        connection.execute("INSERT INTO items VALUES (1, 'paid')");
        connection.execute("ALTER TABLE orders ADD amount int");
        TestHelper.disableTableCdc(connection, "orders");
        TestHelper.enableTableCdc(connection, "orders", "orders_amount");
        connection.execute("INSERT INTO orders VALUES (1, 'paid', 10)");
        TestHelper.startCaptureJob(connection);

        final List<SourceRecord> consumed = consumeRecordsUntilAlterAndDataRecord(ORDERS_TOPIC);
        final SourceRecord schemaChange = firstAlterSchemaChange(consumed);
        final SourceRecord itemsRecord = firstDataRecord(consumed);
        final SourceRecord ordersRecord = firstDataRecord(consumed, ORDERS_TOPIC);

        // The record carries the start LSN of the new capture instance, which is the position the record sits at in the
        // stream
        assertThat(commitLsn(schemaChange)).isEqualTo(captureInstanceStartLsn("orders_amount"));

        // The row of the items table was committed before the capture instance of the orders table was replaced, so it
        // is below the start LSN of that capture instance and it precedes the schema change record
        assertThat(itemsRecord.topic()).isEqualTo(ITEMS_TOPIC);
        assertThat(commitLsn(itemsRecord)).isLessThan(commitLsn(schemaChange));

        // The row of the orders table was committed after the capture instance was replaced, so it is at or above the
        // start LSN of that capture instance and it follows the schema change record. The schema change record is not
        // ahead of it, so a restart from its offset skips no row that followed it. What the record gives up compared to
        // a record that carries the position of the connector is the narrower re-read of such a restart, which re-reads
        // from the start of the capture instance.
        assertThat(commitLsn(schemaChange)).isLessThanOrEqualTo(commitLsn(ordersRecord));
    }

    /**
     * Creates the given tables and starts the connector with an offset that carries no commit LSN, so that the
     * connector cannot detect a replaced capture instance through the LSN window.
     */
    private void startConnectorWithoutCommitLsn(Configuration.Builder config, String... tables) throws Exception {
        createTestTables(tables);
        clearCaptureInstanceStartLsn();

        start(SqlServerConnector.class, config.with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL).build());
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();
        // The position of the connector is the end of the poll range, so the range is empty. The connector therefore
        // cannot nominate the capture instance through the LSN window, and the detector has to find it.
        final Lsn maxTransactionLsn = connection.getMaxTransactionLsn(TestHelper.TEST_DATABASE_1);
        assertThat(maxLsn()).as("The maximum LSN must be the maximum transaction LSN").isEqualTo(maxTransactionLsn);
        if (maxTransactionLsn.isAvailable()) {
            assertThat(captureInstanceStartLsn("dbo_orders"))
                    .as("The start LSN of the capture instance must be below the poll range")
                    .isLessThan(maxTransactionLsn);
        }
    }

    /**
     * Creates the given tables and starts the connector, so that it streams the changes the test makes.
     *
     * @return the configuration of the connector, which the test needs to start it again
     */
    private Configuration startConnectorStreaming(String... tables) throws Exception {
        createTestTables(tables);
        // The capture job publishes the changes of the test
        TestHelper.startCaptureJob(connection);

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();
        return config;
    }

    /**
     * Creates the test database with the given tables and enables CDC on them.
     */
    private void createTestTables(String... tables) throws SQLException {
        // The capture job has to be stopped before the database is dropped, or the drop waits for the transaction of
        // the capture job
        TestHelper.stopCaptureJob(connection);
        connection.close();
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        ensureCaptureJob();
        for (String table : tables) {
            connection.execute("CREATE TABLE " + table + " (id int primary key, status varchar(30))");
            TestHelper.enableTableCdc(connection, table);
        }
    }

    /**
     * Removes the rows of {@code cdc.lsn_time_mapping} that carry the start LSN of the capture instance, so that the
     * maximum LSN of the database is the maximum transaction LSN. The rows of the transactions are kept, because they
     * record the progress of the capture job, and deleting them makes the job capture the same transactions again.
     */
    private void clearCaptureInstanceStartLsn() throws SQLException {
        Awaitility.await("The start LSN of the capture instance is gone")
                .atMost(Duration.ofSeconds(30))
                .pollDelay(Duration.ZERO)
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    connection.execute("DELETE FROM cdc.lsn_time_mapping WHERE tran_id = 0x00");
                    return maxLsn().equals(connection.getMaxTransactionLsn(TestHelper.TEST_DATABASE_1));
                });
    }

    /**
     * Creates the CDC capture job of the test database, if the database does not have one. The job is not created by
     * {@code sp_cdc_enable_db} on every instance.
     */
    private void ensureCaptureJob() throws SQLException {
        if (!captureJobExists()) {
            connection.execute("EXEC sys.sp_cdc_add_job @job_type = N'capture'");
        }
    }

    /**
     * @return {@code true} if the test database has a CDC capture job
     */
    private boolean captureJobExists() throws SQLException {
        return connection.prepareQueryAndMap(
                "SELECT COUNT(*) FROM msdb.dbo.sysjobs WHERE name = ?",
                preparer -> preparer.setString(1, TestHelper.CAPTURE_JOB_NAME),
                connection.singleResultMapper(resultSet -> resultSet.getLong(1) > 0, "Cannot read the state of the capture job"));
    }

    /**
     * @return the maximum LSN the capture job has published, or {@link Lsn#NULL} if it has published nothing
     */
    private Lsn maxLsn() throws SQLException {
        return connection.getMaxLsn(TestHelper.TEST_DATABASE_1);
    }

    /**
     * Consumes the records until the schema change record of the ALTER and the data record of the given topic have been
     * received.
     *
     * @return the consumed records
     */
    private List<SourceRecord> consumeRecordsUntilAlterAndDataRecord(String topic) throws InterruptedException {
        return consumeRecordsUntil(records -> records.stream().anyMatch(SqlServerCaptureInstanceReplacementIT::isAlterSchemaChange)
                && records.stream().anyMatch(record -> isDataRecord(record) && topic.equals(record.topic())));
    }

    /**
     * Consumes the records until a data record of the given topic has been received.
     *
     * @return the consumed records
     */
    private List<SourceRecord> consumeRecordsUntilDataRecord(String topic) throws InterruptedException {
        return consumeRecordsUntil(records -> records.stream()
                .anyMatch(record -> isDataRecord(record) && topic.equals(record.topic())));
    }

    /**
     * Consumes the records until the given number of data records and a schema change record of an ALTER have been
     * received.
     *
     * @return the consumed records
     */
    private List<SourceRecord> consumeRecordsUntilDataRecordsAndAlter(int dataRecordCount) throws InterruptedException {
        return consumeRecordsUntil(records -> records.stream().filter(SqlServerCaptureInstanceReplacementIT::isDataRecord).count() >= dataRecordCount
                && records.stream().anyMatch(SqlServerCaptureInstanceReplacementIT::isAlterSchemaChange));
    }

    /**
     * Consumes the records until the records consumed so far satisfy the given condition.
     *
     * @param isComplete the condition that tells whether the records consumed so far are enough
     * @return the consumed records
     */
    private List<SourceRecord> consumeRecordsUntil(Predicate<List<SourceRecord>> isComplete) throws InterruptedException {
        final List<SourceRecord> consumed = new ArrayList<>();
        consumeRecordsByTopicUntil((count, record) -> {
            consumed.add(record);
            return isComplete.test(consumed);
        });
        return consumed;
    }

    /**
     * Asserts that the schema change record of the records is not ahead of the first data record that follows it, so
     * that a restart from its offset does not skip that record.
     */
    private void assertSchemaChangeIsBeforeTheFirstDataRecord(List<SourceRecord> records) {
        final Lsn schemaChangeLsn = commitLsn(firstAlterSchemaChange(records));
        final Lsn dataLsn = commitLsn(firstDataRecord(records));
        assertThat(schemaChangeLsn).isLessThan(dataLsn);
    }

    /**
     * @param captureInstance the capture instance to inspect
     * @return the start LSN of the given capture instance
     */
    private Lsn captureInstanceStartLsn(String captureInstance) throws SQLException {
        return connection.prepareQueryAndMap(
                "SELECT start_lsn FROM cdc.change_tables WHERE capture_instance = ?",
                preparer -> preparer.setString(1, captureInstance),
                connection.singleResultMapper(resultSet -> Lsn.valueOf(resultSet.getBytes(1)),
                        "Cannot read the start LSN of the capture instance"));
    }

    private static boolean isAlterSchemaChange(SourceRecord record) {
        if (!(record.value() instanceof Struct value)) {
            return false;
        }
        if (value.schema().field("tableChanges") == null) {
            return false;
        }
        final List<Struct> tableChanges = value.getArray("tableChanges");
        return tableChanges != null && !tableChanges.isEmpty() && "ALTER".equals(tableChanges.get(0).getString("type"));
    }

    private static boolean isDataRecord(SourceRecord record) {
        return record.value() instanceof Struct value && value.schema().field("after") != null;
    }

    /**
     * @return the schema change record of the ALTER among the given records
     */
    private static SourceRecord firstAlterSchemaChange(List<SourceRecord> records) {
        return records.stream()
                .filter(SqlServerCaptureInstanceReplacementIT::isAlterSchemaChange)
                .findFirst()
                .orElseThrow(() -> new AssertionError("The connector must emit the schema change record of the ALTER"));
    }

    /**
     * @return the first data record among the given records
     */
    private static SourceRecord firstDataRecord(List<SourceRecord> records) {
        return records.stream()
                .filter(SqlServerCaptureInstanceReplacementIT::isDataRecord)
                .findFirst()
                .orElseThrow(() -> new AssertionError("The connector must emit the data record"));
    }

    /**
     * @return the first data record of the given topic among the given records
     */
    private static SourceRecord firstDataRecord(List<SourceRecord> records, String topic) {
        return records.stream()
                .filter(record -> isDataRecord(record) && topic.equals(record.topic()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("The connector must emit the data record of " + topic));
    }

    /**
     * @return the commit LSN the given record carries in its source offset
     */
    private static Lsn commitLsn(SourceRecord record) {
        return Lsn.valueOf((String) record.sourceOffset().get(SourceInfo.COMMIT_LSN_KEY));
    }

    /**
     * Asserts that the topic carries exactly one record and that the value of that record carries the given optional
     * int columns.
     */
    private void assertSingleRecord(SourceRecords records, String topic, String... optionalIntColumns) {
        assertSingleRecord(records.recordsForTopic(topic), topic, optionalIntColumns);
    }

    /**
     * Asserts that exactly one of the given records belongs to the topic and that the value of that record carries the
     * given optional int columns.
     */
    private void assertSingleRecord(List<SourceRecord> records, String topic, String... optionalIntColumns) {
        final List<SourceRecord> topicRecords = records.stream()
                .filter(record -> topic.equals(record.topic()))
                .collect(Collectors.toList());
        assertThat(topicRecords).hasSize(1);
        final SchemaBuilder builder = SchemaBuilder.struct()
                .optional()
                .name(topic + ".Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("status", Schema.OPTIONAL_STRING_SCHEMA);
        for (String column : optionalIntColumns) {
            builder.field(column, Schema.OPTIONAL_INT32_SCHEMA);
        }
        final Schema valueSchema = builder.build();
        topicRecords.forEach(record -> assertSchemaMatchesStruct((Struct) ((Struct) record.value()).get("after"), valueSchema));
    }

    private static int idOf(SourceRecord record) {
        return ((Struct) ((Struct) record.value()).get("after")).getInt32("id");
    }
}

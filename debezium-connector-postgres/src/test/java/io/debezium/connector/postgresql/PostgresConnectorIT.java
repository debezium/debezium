/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static junit.framework.TestCase.assertEquals;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.postgresql.util.PSQLState;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.Version;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.postgresql.PostgresConnectorConfig.LogicalDecoder;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.junit.SkipTestDependingOnDatabaseVersionRule;
import io.debezium.connector.postgresql.junit.SkipTestDependingOnDecoderPluginNameRule;
import io.debezium.connector.postgresql.junit.SkipWhenDatabaseVersionLessThan;
import io.debezium.connector.postgresql.junit.SkipWhenDatabaseVersionLessThan.PostgresVersion;
import io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIs;
import io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIsNot;
import io.debezium.converters.CloudEventsConverterTest;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Strings;
import io.debezium.util.Testing;

/**
 * Integration test for {@link PostgresConnector} using an {@link io.debezium.embedded.EmbeddedEngine}
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresConnectorIT extends AbstractConnectorTest {

    /*
     * Specific tests that need to extend the initial DDL set should do it in a form of
     * TestHelper.execute(SETUP_TABLES_STMT + ADDITIONAL_STATEMENTS)
     */
    private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
            "INSERT INTO s2.a (aa) VALUES (1);";
    private static final String SETUP_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "DROP SCHEMA IF EXISTS s2 CASCADE;" +
            "CREATE SCHEMA s1; " +
            "CREATE SCHEMA s2; " +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s2.a (pk SERIAL, aa integer, bb varchar(20), PRIMARY KEY(pk));" +
            INSERT_STMT;
    private PostgresConnector connector;

    @Rule
    public final TestRule skipName = new SkipTestDependingOnDecoderPluginNameRule();

    @Rule
    public final TestRule skipVersion = new SkipTestDependingOnDatabaseVersionRule();

    @BeforeClass
    public static void beforeClass() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @Before
    public void before() {
        initializeConnectorTestFramework();
    }

    @After
    public void after() {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    @Test
    public void shouldValidateConnectorConfigDef() {
        connector = new PostgresConnector();
        ConfigDef configDef = connector.config();
        assertThat(configDef).isNotNull();
        PostgresConnectorConfig.ALL_FIELDS.forEach(this::validateFieldDef);
    }

    @Test
    public void shouldNotStartWithInvalidConfiguration() throws Exception {
        // use an empty configuration which should be invalid because of the lack of DB connection details
        Configuration config = Configuration.create().build();

        // we expect the engine will log at least one error, so preface it ...
        logger.info("Attempting to start the connector with an INVALID configuration, so MULTIPLE error messages & one exceptions will appear in the log");
        start(PostgresConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();
    }

    @Test
    public void shouldValidateMinimalConfiguration() throws Exception {
        Configuration config = TestHelper.defaultConfig().build();
        Config validateConfig = new PostgresConnector().validate(config.asMap());
        validateConfig.configValues().forEach(configValue -> assertTrue("Unexpected error for: " + configValue.name(),
                configValue.errorMessages().isEmpty()));
    }

    @Test
    public void shouldValidateConfiguration() throws Exception {
        // use an empty configuration which should be invalid because of the lack of DB connection details
        Configuration config = Configuration.create().build();
        PostgresConnector connector = new PostgresConnector();
        Config validatedConfig = connector.validate(config.asMap());
        // validate that the required fields have errors
        assertConfigurationErrors(validatedConfig, PostgresConnectorConfig.HOSTNAME, 1);
        assertConfigurationErrors(validatedConfig, PostgresConnectorConfig.USER, 1);
        assertConfigurationErrors(validatedConfig, PostgresConnectorConfig.DATABASE_NAME, 1);
        assertConfigurationErrors(validatedConfig, PostgresConnectorConfig.SERVER_NAME, 1);

        // validate the non required fields
        validateField(validatedConfig, PostgresConnectorConfig.PLUGIN_NAME, LogicalDecoder.DECODERBUFS.getValue());
        validateField(validatedConfig, PostgresConnectorConfig.SLOT_NAME, ReplicationConnection.Builder.DEFAULT_SLOT_NAME);
        validateField(validatedConfig, PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        validateField(validatedConfig, PostgresConnectorConfig.PORT, PostgresConnectorConfig.DEFAULT_PORT);
        validateField(validatedConfig, PostgresConnectorConfig.MAX_QUEUE_SIZE, PostgresConnectorConfig.DEFAULT_MAX_QUEUE_SIZE);
        validateField(validatedConfig, PostgresConnectorConfig.MAX_BATCH_SIZE, PostgresConnectorConfig.DEFAULT_MAX_BATCH_SIZE);
        validateField(validatedConfig, PostgresConnectorConfig.SNAPSHOT_FETCH_SIZE, null);
        validateField(validatedConfig, PostgresConnectorConfig.POLL_INTERVAL_MS, PostgresConnectorConfig.DEFAULT_POLL_INTERVAL_MILLIS);
        validateField(validatedConfig, PostgresConnectorConfig.SSL_MODE, PostgresConnectorConfig.SecureConnectionMode.DISABLED);
        validateField(validatedConfig, PostgresConnectorConfig.SSL_CLIENT_CERT, null);
        validateField(validatedConfig, PostgresConnectorConfig.SSL_CLIENT_KEY, null);
        validateField(validatedConfig, PostgresConnectorConfig.SSL_CLIENT_KEY_PASSWORD, null);
        validateField(validatedConfig, PostgresConnectorConfig.SSL_ROOT_CERT, null);
        validateField(validatedConfig, PostgresConnectorConfig.SCHEMA_WHITELIST, null);
        validateField(validatedConfig, PostgresConnectorConfig.SCHEMA_BLACKLIST, null);
        validateField(validatedConfig, PostgresConnectorConfig.TABLE_WHITELIST, null);
        validateField(validatedConfig, PostgresConnectorConfig.TABLE_BLACKLIST, null);
        validateField(validatedConfig, PostgresConnectorConfig.COLUMN_BLACKLIST, null);
        validateField(validatedConfig, PostgresConnectorConfig.MSG_KEY_COLUMNS, null);
        validateField(validatedConfig, PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL);
        validateField(validatedConfig, RelationalDatabaseConnectorConfig.SNAPSHOT_LOCK_TIMEOUT_MS,
                RelationalDatabaseConnectorConfig.DEFAULT_SNAPSHOT_LOCK_TIMEOUT_MILLIS);
        validateField(validatedConfig, PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ADAPTIVE);
        validateField(validatedConfig, PostgresConnectorConfig.DECIMAL_HANDLING_MODE, PostgresConnectorConfig.DecimalHandlingMode.PRECISE);
        validateField(validatedConfig, PostgresConnectorConfig.SSL_SOCKET_FACTORY, null);
        validateField(validatedConfig, PostgresConnectorConfig.TCP_KEEPALIVE, true);
    }

    @Test
    public void shouldValidateReplicationSlotName() throws Exception {
        Configuration config = Configuration.create()
                .with(PostgresConnectorConfig.SLOT_NAME, "xx-aa")
                .build();
        PostgresConnector connector = new PostgresConnector();
        Config validatedConfig = connector.validate(config.asMap());

        assertConfigurationErrors(validatedConfig, PostgresConnectorConfig.SLOT_NAME, 1);
    }

    @Test
    public void shouldSupportSSLParameters() throws Exception {
        // the default docker image we're testing against doesn't use SSL, so check that the connector fails to start when
        // SSL is enabled
        Configuration config = TestHelper.defaultConfig().with(PostgresConnectorConfig.SSL_MODE,
                PostgresConnectorConfig.SecureConnectionMode.REQUIRED).build();
        start(PostgresConnector.class, config, (success, msg, error) -> {
            if (TestHelper.shouldSSLConnectionFail()) {
                // we expect the task to fail at startup when we're printing the server info
                assertThat(success).isFalse();
                assertThat(error).isInstanceOf(ConnectException.class);
                Throwable cause = error.getCause();
                assertThat(cause).isInstanceOf(SQLException.class);
                assertThat(PSQLState.CONNECTION_REJECTED.getState().equals(((SQLException) cause).getSQLState()));
            }
        });
        if (TestHelper.shouldSSLConnectionFail()) {
            assertConnectorNotRunning();
        }
        else {
            assertConnectorIsRunning();
            Thread.sleep(10000);
            stopConnector();
        }
    }

    @Test
    public void shouldProduceEventsWithInitialSnapshot() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        // check the records from the snapshot
        assertRecordsFromSnapshot(2, 1, 1);

        // insert 2 new records
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 2, 2);

        // now stop the connector
        stopConnector();
        assertNoRecordsToConsume();

        // insert some more records
        TestHelper.execute(INSERT_STMT);

        // start the connector back up and check that a new snapshot has not been performed (we're running initial only mode)
        // but the 2 records that we were inserted while we were down will be retrieved
        start(PostgresConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();

        assertRecordsAfterInsert(2, 3, 3);
    }

    @Test
    @FixFor("DBZ-1174")
    public void shouldUseMicrosecondsForTransactionCommitTime() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(CommonConnectorConfig.SOURCE_STRUCT_MAKER_VERSION, Version.V1)
                .build());
        assertConnectorIsRunning();

        // check records from snapshot
        Instant inst = Instant.now();
        // Microseconds since epoch, may overflow
        final long microsSnapshot = TimeUnit.SECONDS.toMicros(inst.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(inst.getNano());
        SourceRecords actualRecords = consumeRecordsByTopic(2);
        actualRecords.forEach(sourceRecord -> assertSourceInfoMicrosecondTransactionTimestamp(sourceRecord, microsSnapshot, TimeUnit.MINUTES.toMicros(1L)));

        // insert 2 new records
        TestHelper.execute(INSERT_STMT);
        // check records from streaming
        inst = Instant.now();
        // Microseconds since epoch, may overflow
        final long microsStream = TimeUnit.SECONDS.toMicros(inst.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(inst.getNano());
        actualRecords = consumeRecordsByTopic(2);
        actualRecords.forEach(sourceRecord -> assertSourceInfoMicrosecondTransactionTimestamp(sourceRecord, microsStream, TimeUnit.MINUTES.toMicros(1L)));

        // now stop the connector
        stopConnector();
        assertNoRecordsToConsume();
    }

    @Test
    @FixFor("DBZ-1235")
    public void shouldUseMillisecondsForTransactionCommitTime() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        start(PostgresConnector.class, TestHelper.defaultConfig().build());
        assertConnectorIsRunning();

        // check records from snapshot
        Instant inst = Instant.now();
        // Milliseconds since epoch, may overflow
        final long millisSnapshot = TimeUnit.SECONDS.toMillis(inst.getEpochSecond()) + TimeUnit.NANOSECONDS.toMillis(inst.getNano());
        SourceRecords actualRecords = consumeRecordsByTopic(2);
        actualRecords.forEach(sourceRecord -> assertSourceInfoMillisecondTransactionTimestamp(sourceRecord, millisSnapshot, TimeUnit.MINUTES.toMillis(1L)));

        // insert 2 new records
        TestHelper.execute(INSERT_STMT);
        // check records from streaming
        inst = Instant.now();
        // Milliseconds since epoch, may overflow
        final long millisStream = TimeUnit.SECONDS.toMillis(inst.getEpochSecond()) + TimeUnit.NANOSECONDS.toMillis(inst.getNano());
        actualRecords = consumeRecordsByTopic(2);
        actualRecords.forEach(sourceRecord -> assertSourceInfoMillisecondTransactionTimestamp(sourceRecord, millisStream, TimeUnit.MINUTES.toMillis(1L)));

        // now stop the connector
        stopConnector();
        assertNoRecordsToConsume();
    }

    @Test
    @FixFor("DBZ-1161")
    public void shouldConsumeMessagesFromSnapshot() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        final int recordCount = 100;

        for (int i = 0; i < recordCount - 1; i++) {
            TestHelper.execute(INSERT_STMT);
        }
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.MAX_QUEUE_SIZE, recordCount / 2)
                .with(PostgresConnectorConfig.MAX_BATCH_SIZE, 10)
                .with(PostgresConnectorConfig.SCHEMA_WHITELIST, "s1");
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted();
        SourceRecords records = consumeRecordsByTopic(recordCount);
        Assertions.assertThat(records.recordsForTopic("test_server.s1.a")).hasSize(recordCount);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-997")
    public void shouldReceiveChangesForChangePKColumnDefinition() throws Exception {
        Testing.Print.enable();
        final String slotName = "pkcolumndef" + new Random().nextInt(100);
        TestHelper.create().dropReplicationSlot(slotName);
        try {
            final PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                    .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, Boolean.FALSE)
                    .with(PostgresConnectorConfig.SCHEMA_WHITELIST, "changepk")
                    .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                    .with(PostgresConnectorConfig.SLOT_NAME, slotName)
                    .build());

            final String newPkField = "newpk";
            final String topicName = topicName("changepk.test_table");

            TestHelper.execute(
                    "CREATE SCHEMA IF NOT EXISTS changepk;",
                    "DROP TABLE IF EXISTS changepk.test_table;",
                    "CREATE TABLE changepk.test_table (pk SERIAL, text TEXT, PRIMARY KEY(pk));",
                    "INSERT INTO changepk.test_table(text) VALUES ('insert');");

            start(PostgresConnector.class, config.getConfig());

            assertConnectorIsRunning();

            // wait for snapshot completion
            SourceRecords records = consumeRecordsByTopic(1);

            TestHelper.execute(
                    "ALTER TABLE changepk.test_table DROP CONSTRAINT test_table_pkey;"
                            + "ALTER TABLE changepk.test_table RENAME COLUMN pk TO newpk;"
                            + "ALTER TABLE changepk.test_table ADD PRIMARY KEY(newpk);"
                            + "INSERT INTO changepk.test_table VALUES(2, 'newpkcol')");
            records = consumeRecordsByTopic(1);

            SourceRecord insertRecord = records.recordsForTopic(topicName).get(0);
            assertEquals(topicName, insertRecord.topic());
            VerifyRecord.isValidInsert(insertRecord, "newpk", 2);

            TestHelper.execute(
                    "ALTER TABLE changepk.test_table ADD COLUMN pk2 SERIAL;"
                            + "ALTER TABLE changepk.test_table DROP CONSTRAINT test_table_pkey;"
                            + "ALTER TABLE changepk.test_table ADD PRIMARY KEY(newpk,pk2);"
                            + "INSERT INTO changepk.test_table VALUES(3, 'newpkcol', 8)");
            records = consumeRecordsByTopic(1);

            insertRecord = records.recordsForTopic(topicName).get(0);
            assertEquals(topicName, insertRecord.topic());
            VerifyRecord.isValidInsert(insertRecord, newPkField, 3);
            VerifyRecord.isValidInsert(insertRecord, "pk2", 8);

            stopConnector();

            // De-synchronize JDBC PK info and decoded event schema
            TestHelper.execute("INSERT INTO changepk.test_table VALUES(4, 'newpkcol', 20)");
            TestHelper.execute(
                    "ALTER TABLE changepk.test_table DROP CONSTRAINT test_table_pkey;"
                            + "ALTER TABLE changepk.test_table DROP COLUMN pk2;"
                            + "ALTER TABLE changepk.test_table ADD COLUMN pk3 SERIAL;"
                            + "ALTER TABLE changepk.test_table ADD PRIMARY KEY(newpk,pk3);"
                            + "INSERT INTO changepk.test_table VALUES(5, 'dropandaddpkcol',10)");

            start(PostgresConnector.class, config.getConfig());

            records = consumeRecordsByTopic(2);

            insertRecord = records.recordsForTopic(topicName).get(0);
            assertEquals(topicName, insertRecord.topic());
            VerifyRecord.isValidInsert(insertRecord, newPkField, 4);
            Struct key = (Struct) insertRecord.key();
            // The problematic record PK info is temporarily desynced
            assertThat(key.schema().field("pk2")).isNull();
            assertThat(key.schema().field("pk3")).isNull();

            insertRecord = records.recordsForTopic(topicName).get(1);
            assertEquals(topicName, insertRecord.topic());
            VerifyRecord.isValidInsert(insertRecord, newPkField, 5);
            VerifyRecord.isValidInsert(insertRecord, "pk3", 10);
            key = (Struct) insertRecord.key();
            assertThat(key.schema().field("pk2")).isNull();

            stopConnector();
            TestHelper.create().dropReplicationSlot(slotName);

            TestHelper.execute("DROP SCHEMA IF EXISTS changepk CASCADE;");
        }
        catch (Throwable t) {
            // Ideally we want tests to cleanup after themselves in the event of a failure.
            // Since this test creates a random named slot every time there is the possibility that the
            // test fails and therefore the slot is not dropped, which can be problematic for environments
            // where there are limited logical replication slots configured.
            stopConnector(null);
            TestHelper.create().dropReplicationSlot(slotName);
            throw t;
        }
    }

    @Test
    @FixFor("DBZ-1021")
    @SkipWhenDecoderPluginNameIs(value = SkipWhenDecoderPluginNameIs.DecoderPluginName.PGOUTPUT, reason = "Pgoutput will generate insert statements even for dropped tables, column optionality will default to true however")
    public void shouldIgnoreEventsForDeletedTable() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        // check the records from the snapshot
        assertRecordsFromSnapshot(2, 1, 1);
        waitForStreamingRunning();

        // insert 2 new records
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 2, 2);

        // now stop the connector
        stopConnector();
        assertNoRecordsToConsume();

        // insert some more records and deleted the table
        TestHelper.execute(INSERT_STMT);
        TestHelper.execute("DROP TABLE s1.a");

        start(PostgresConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();
        waitForStreamingRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics()).hasSize(1);
        assertThat(actualRecords.recordsForTopic(topicName("s2.a"))).hasSize(1);

        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
    }

    @Test
    @FixFor("DBZ-1021")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Pgoutput will generate insert statements even for dropped tables, column optionality will default to true however")
    public void shouldNotIgnoreEventsForDeletedTable() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        // check the records from the snapshot
        assertRecordsFromSnapshot(2, 1, 1);
        waitForStreamingRunning();

        // insert 2 new records
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 2, 2);

        // now stop the connector
        stopConnector();
        assertNoRecordsToConsume();

        // insert some more records and deleted the table
        TestHelper.execute(INSERT_STMT);
        TestHelper.execute("DROP TABLE s1.a");

        start(PostgresConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();
        waitForStreamingRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        assertThat(actualRecords.topics()).hasSize(2);
        assertThat(actualRecords.recordsForTopic(topicName("s1.a"))).hasSize(1);
        assertThat(actualRecords.recordsForTopic(topicName("s2.a"))).hasSize(1);

        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
    }

    @Test
    public void shouldIgnoreViews() throws Exception {
        TestHelper.execute(
                SETUP_TABLES_STMT +
                        "CREATE VIEW s1.myview AS SELECT * from s1.a;");
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        // check the records from the snapshot
        assertRecordsFromSnapshot(2, 1, 1);
        waitForStreamingRunning();

        // insert 2 new records
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 2, 2);

        // now stop the connector
        stopConnector();
        assertNoRecordsToConsume();

        // insert some more records
        TestHelper.execute(INSERT_STMT);

        // start the connector back up and check that a new snapshot has not been performed (we're running initial only mode)
        // but the 2 records that we were inserted while we were down will be retrieved
        start(PostgresConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();
        waitForStreamingRunning();

        assertRecordsAfterInsert(2, 3, 3);

        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
    }

    @Test
    @FixFor("DBZ-693")
    public void shouldExecuteOnConnectStatements() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.ON_CONNECT_STATEMENTS, "INSERT INTO s1.a (aa) VALUES (2); INSERT INTO s2.a (aa, bb) VALUES (2, 'hello;; world');")
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(6);
        assertKey(actualRecords.allRecordsInOrder().get(0), "pk", 1);
        assertKey(actualRecords.allRecordsInOrder().get(1), "pk", 2);

        // JdbcConnection#connection() is called multiple times during connector start-up,
        // so the given statements will be executed multiple times, resulting in multiple
        // records; here we're interested just in the first insert for s2.a
        assertValueField(actualRecords.allRecordsInOrder().get(5), "after/bb", "hello; world");

        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
    }

    @Test
    public void shouldProduceEventsWhenSnapshotsAreNeverAllowed() throws InterruptedException {
        Testing.Print.enable();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForDefaultReplicationSlotBeActive();

        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        // insert and verify 2 new records
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 2, 2);
    }

    @Test
    public void shouldNotProduceEventsWithInitialOnlySnapshot() throws InterruptedException {
        Testing.Print.enable();
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        // check the records from the snapshot
        assertRecordsFromSnapshot(2, 1, 1);

        // insert and verify that no events were received since the connector should not be streaming changes
        TestHelper.execute(INSERT_STMT);
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);
        // there shouldn't be any records
        assertNoRecordsToConsume();
    }

    @Test
    public void shouldProduceEventsWhenAlwaysTakingSnapshots() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        // check the records from the snapshot
        assertRecordsFromSnapshot(2, 1, 1);
        // insert and verify 2 new records
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 2, 2);

        // now stop the connector
        stopConnector();
        assertNoRecordsToConsume();

        // start the connector back up and check that a new snapshot has been performed
        start(PostgresConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();

        assertRecordsFromSnapshot(4, 1, 2, 1, 2);

        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
    }

    @Test
    public void shouldResumeSnapshotIfFailingMidstream() throws Exception {
        // insert another set of rows so we can stop at certain point
        CountDownLatch latch = new CountDownLatch(1);
        String setupStmt = SETUP_TABLES_STMT + INSERT_STMT;
        TestHelper.execute(setupStmt);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        EmbeddedEngine.CompletionCallback completionCallback = (success, message, error) -> {
            if (error != null) {
                latch.countDown();
            }
            else {
                fail("A controlled exception was expected....");
            }
        };
        start(PostgresConnector.class, configBuilder.build(), completionCallback, stopOnPKPredicate(2));
        // wait until we know we've raised the exception at startup AND the engine has been shutdown
        if (!latch.await(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS)) {
            fail("did not reach stop condition in time");
        }
        // wait until we know we've raised the exception at startup AND the engine has been shutdown
        assertConnectorNotRunning();
        // just drain all the records
        consumeAvailableRecords(record -> {
        });
        // stop the engine altogether
        stopConnector();
        // make sure there are no records to consume
        assertNoRecordsToConsume();
        // start the connector back up and check that it took another full snapshot since previously it was stopped midstream
        start(PostgresConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();

        // check that the snapshot was recreated
        assertRecordsFromSnapshot(4, 1, 2, 1, 2);

        // and we can stream records
        // insert and verify 2 new records
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 3, 3);

        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
    }

    @Test
    @FixFor("DBZ-1857")
    @SkipWhenDatabaseVersionLessThan(PostgresVersion.POSTGRES_10)
    public void shouldRecoverFromRetriableException() throws Exception {
        // Testing.Print.enable();
        String setupStmt = SETUP_TABLES_STMT;
        TestHelper.execute(setupStmt);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();

        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        assertRecordsFromSnapshot(2, 1, 1);

        // kill all opened connections to the database
        TestHelper.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type='walsender'");
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 2, 2);

        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
    }

    @Test
    public void shouldTakeBlacklistFiltersIntoAccount() throws Exception {
        String setupStmt = SETUP_TABLES_STMT +
                "CREATE TABLE s1.b (pk SERIAL, aa integer, bb integer, PRIMARY KEY(pk));" +
                "ALTER TABLE s1.a ADD COLUMN bb integer;" +
                "INSERT INTO s1.a (aa, bb) VALUES (2, 2);" +
                "INSERT INTO s1.a (aa, bb) VALUES (3, 3);" +
                "INSERT INTO s1.b (aa, bb) VALUES (4, 4);" +
                "INSERT INTO s2.a (aa) VALUES (5);";
        TestHelper.execute(setupStmt);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SCHEMA_BLACKLIST, "s2")
                .with(PostgresConnectorConfig.TABLE_BLACKLIST, ".+b")
                .with(PostgresConnectorConfig.COLUMN_BLACKLIST, ".+bb");

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        // check the records from the snapshot take the filters into account
        SourceRecords actualRecords = consumeRecordsByTopic(4); // 3 records in s1.a and 1 in s1.b

        assertThat(actualRecords.recordsForTopic(topicName("s2.a"))).isNullOrEmpty();
        assertThat(actualRecords.recordsForTopic(topicName("s1.b"))).isNullOrEmpty();
        List<SourceRecord> recordsForS1a = actualRecords.recordsForTopic(topicName("s1.a"));
        assertThat(recordsForS1a.size()).isEqualTo(3);
        AtomicInteger pkValue = new AtomicInteger(1);
        recordsForS1a.forEach(record -> {
            VerifyRecord.isValidRead(record, PK_FIELD, pkValue.getAndIncrement());
            assertFieldAbsent(record, "bb");
        });

        // insert some more records and verify the filtering behavior
        String insertStmt = "INSERT INTO s1.b (aa, bb) VALUES (6, 6);" +
                "INSERT INTO s2.a (aa) VALUES (7);";
        TestHelper.execute(insertStmt);
        assertNoRecordsToConsume();
    }

    @Test
    @FixFor("DBZ-1546")
    public void shouldRemoveWhiteSpaceChars() throws Exception {
        String setupStmt = SETUP_TABLES_STMT +
                "CREATE TABLE s1.b (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                "INSERT INTO s1.b (aa) VALUES (123);";

        String tableWhitelistWithWhitespace = "s1.a, s1.b";

        TestHelper.execute(setupStmt);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SCHEMA_WHITELIST, "s1")
                .with(PostgresConnectorConfig.TABLE_WHITELIST, tableWhitelistWithWhitespace);

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);

        List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s1.b"));
        assertThat(records.size()).isEqualTo(1);

        SourceRecord record = records.get(0);
        VerifyRecord.isValidRead(record, PK_FIELD, 1);

        String sourceTable = ((Struct) record.value()).getStruct("source").getString("table");
        assertThat(sourceTable).isEqualTo("b");
    }

    @Test
    @FixFor("DBZ-878")
    public void shouldReplaceInvalidTopicNameCharacters() throws Exception {
        String setupStmt = SETUP_TABLES_STMT +
                "CREATE TABLE s1.\"dbz_878_some|test@data\" (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                "INSERT INTO s1.\"dbz_878_some|test@data\" (aa) VALUES (123);";

        TestHelper.execute(setupStmt);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SCHEMA_WHITELIST, "s1")
                .with(PostgresConnectorConfig.TABLE_WHITELIST, "s1\\.dbz_878_some\\|test@data");

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(1);

        List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s1.dbz_878_some_test_data"));
        assertThat(records.size()).isEqualTo(1);

        SourceRecord record = records.get(0);
        VerifyRecord.isValidRead(record, PK_FIELD, 1);

        String sourceTable = ((Struct) record.value()).getStruct("source").getString("table");
        assertThat(sourceTable).isEqualTo("dbz_878_some|test@data");
    }

    @Test
    @FixFor("DBZ-1245")
    public void shouldNotSendEmptyOffset() throws InterruptedException, SQLException {
        final String statement = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
                "CREATE SCHEMA s1; " +
                "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));";
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.TABLE_WHITELIST, "s1.a")
                .with(Heartbeat.HEARTBEAT_INTERVAL, 10)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        // Generate empty logical decoding message
        TestHelper.execute(statement);
        waitForAvailableRecords(1000, TimeUnit.MILLISECONDS);

        SourceRecord record = consumeRecord();
        assertThat(record == null || !record.sourceOffset().isEmpty());
    }

    @Test
    @FixFor("DBZ-965")
    public void shouldRegularlyFlushLsn() throws InterruptedException, SQLException {
        final int recordCount = 10;
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.TABLE_WHITELIST, "s1.a")
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        final Set<String> flushLsn = new HashSet<>();
        try (final PostgresConnection connection = TestHelper.create()) {
            flushLsn.add(getConfirmedFlushLsn(connection));
            for (int i = 2; i <= recordCount + 2; i++) {
                TestHelper.execute(INSERT_STMT);

                final SourceRecords actualRecords = consumeRecordsByTopic(1);
                assertThat(actualRecords.topics().size()).isEqualTo(1);
                assertThat(actualRecords.recordsForTopic(topicName("s1.a")).size()).isEqualTo(1);

                // Wait max 2 seconds for LSN change
                try {
                    Awaitility.await().atMost(2, TimeUnit.SECONDS).ignoreExceptions().until(() -> flushLsn.add(getConfirmedFlushLsn(connection)));
                }
                catch (ConditionTimeoutException e) {
                    // We do not require all flushes to succeed in time
                }
            }
        }
        // Theoretically the LSN should change for each record but in reality there can be
        // unfortunate timings so let's suppose the change will happen in 75 % of cases
        Assertions.assertThat(flushLsn.size()).isGreaterThanOrEqualTo((recordCount * 3) / 4);
    }

    @Test
    @FixFor("DBZ-892")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.WAL2JSON, reason = "Only wal2json decoder emits empty events and passes them to streaming source")
    public void shouldFlushLsnOnEmptyMessage() throws InterruptedException, SQLException {
        final String DDL_STATEMENT = "CREATE TEMPORARY TABLE xx(id INT);";

        final int recordCount = 10;
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.TABLE_WHITELIST, "s1.a")
                .with(Heartbeat.HEARTBEAT_INTERVAL, 1_000)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        final Set<String> flushLsn = new HashSet<>();
        TestHelper.execute(INSERT_STMT);

        Awaitility.await().atMost(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS).until(() -> {
            final SourceRecords actualRecords = consumeRecordsByTopic(1);
            final List<SourceRecord> topicRecords = actualRecords.recordsForTopic(topicName("s1.a"));
            return topicRecords != null && topicRecords.size() == 1;
        });

        try (final PostgresConnection connection = TestHelper.create()) {
            flushLsn.add(getConfirmedFlushLsn(connection));
            for (int i = 0; i < recordCount; i++) {
                TestHelper.execute(DDL_STATEMENT);

                try {
                    // Wait max 5 seconds for LSN change caused by DDL_STATEMENT
                    Awaitility.await().atMost(5, TimeUnit.SECONDS).ignoreExceptions().until(() -> flushLsn.add(getConfirmedFlushLsn(connection)));
                }
                catch (ConditionTimeoutException e) {
                    // We do not require all flushes to succeed in time
                }
            }
        }
        // Theoretically the LSN should change for each record but in reality there can be
        // unfortunate timings so let's suppose the change will happen in 75 % of cases
        Assertions.assertThat(flushLsn.size()).isGreaterThanOrEqualTo((recordCount * 3) / 4);
    }

    @Test
    @FixFor("DBZ-1082")
    public void shouldAllowForCustomSnapshot() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(1);

        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs).isNull();

        SourceRecord record = s1recs.get(0);
        VerifyRecord.isValidRead(record, PK_FIELD, 1);

        TestHelper.execute(INSERT_STMT);
        actualRecords = consumeRecordsByTopic(2);

        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        record = s1recs.get(0);
        VerifyRecord.isValidInsert(record, PK_FIELD, 2);
        record = s2recs.get(0);
        VerifyRecord.isValidInsert(record, PK_FIELD, 2);
        stopConnector();

        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        actualRecords = consumeRecordsByTopic(4);

        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(2);
        assertThat(s2recs.size()).isEqualTo(2);
        VerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);
        VerifyRecord.isValidRead(s1recs.get(1), PK_FIELD, 2);
        VerifyRecord.isValidRead(s2recs.get(0), PK_FIELD, 1);
        VerifyRecord.isValidRead(s2recs.get(1), PK_FIELD, 2);
    }

    @Test
    @FixFor("DBZ-1035")
    public void shouldAllowForExportedSnapshot() throws Exception {
        TestHelper.dropDefaultReplicationSlot();

        // Inside RecordsSnapshotProducer, we inject a new row into s1.a with aa=5 prior to executing the
        // actual snapshot. The snapshot reference is that of what the tables looked like at the time
        // the replication slot was created.
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.EXPORTED.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        // Consume records from the snapshot
        SourceRecords actualRecords = consumeRecordsByTopic(2);

        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);

        VerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);
        VerifyRecord.isValidRead(s2recs.get(0), PK_FIELD, 1);

        // Insert 2 more rows
        // These are captured by the stream
        // NOTE: Manually tested the notion that if records were inserted between creation of replication slot and
        // the finalization of the snapshot that those records would be captured and streamed at this point.
        TestHelper.execute(INSERT_STMT);
        actualRecords = consumeRecordsByTopic(2);

        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);

        VerifyRecord.isValidInsert(s1recs.get(0), PK_FIELD, 2);
        VerifyRecord.isValidInsert(s2recs.get(0), PK_FIELD, 2);
        stopConnector();

        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.EXPORTED.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.execute(INSERT_STMT);

        actualRecords = consumeRecordsByTopic(2);

        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        VerifyRecord.isValidInsert(s1recs.get(0), PK_FIELD, 3);
        VerifyRecord.isValidInsert(s2recs.get(0), PK_FIELD, 3);
    }

    @Test
    @FixFor("DBZ-1437")
    public void shouldPeformSnapshotOnceForInitialOnlySnapshotMode() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor();

        TestHelper.dropDefaultReplicationSlot();

        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        // Lets wait for snapshot to finish before proceeding
        waitForSnapshotToBeCompleted("postgres", "test_server");

        // Lets perform some inserts, this shouldn't be captured.
        // This is because we only did snapshot and these would be added afterward.
        TestHelper.execute(INSERT_STMT);
        waitForAvailableRecords(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        // Consume stream and make sure only the records at snapshot were generated
        SourceRecords actualRecords = consumeRecordsByTopic(2);

        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        VerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);
        VerifyRecord.isValidRead(s2recs.get(0), PK_FIELD, 1);

        // Stop the connector
        stopConnector();
        assertConnectorNotRunning();

        // Restart the connector again with initial-only
        // No snapshot should be produced and no records generated
        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        // Stop the connector, verify that no snapshot was performed
        stopConnector(value -> assertThat(logInterceptor.containsMessage("Previous initial snapshot completed, no snapshot will be performed")).isTrue());
    }

    private String getConfirmedFlushLsn(PostgresConnection connection) throws SQLException {
        return connection.prepareQueryAndMap(
                "select * from pg_replication_slots where slot_name = ? and database = ? and plugin = ?", statement -> {
                    statement.setString(1, ReplicationConnection.Builder.DEFAULT_SLOT_NAME);
                    statement.setString(2, "postgres");
                    statement.setString(3, TestHelper.decoderPlugin().getPostgresPluginName());
                },
                rs -> {
                    if (rs.next()) {
                        return rs.getString("confirmed_flush_lsn");
                    }
                    else {
                        fail("No replication slot info available");
                    }
                    return null;
                });
    }

    private void assertFieldAbsent(SourceRecord record, String fieldName) {
        Struct value = (Struct) ((Struct) record.value()).get(Envelope.FieldName.AFTER);
        try {
            value.get(fieldName);
            fail("field should not be present");
        }
        catch (DataException e) {
            // expected
        }
    }

    @Test
    @Ignore
    public void testStreamingPerformance() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE);
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        final long recordsCount = 1000000;
        final int batchSize = 1000;

        batchInsertRecords(recordsCount, batchSize);
        CompletableFuture.runAsync(() -> consumeRecords(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
    }

    private void consumeRecords(long recordsCount) {
        int totalConsumedRecords = 0;
        long start = System.currentTimeMillis();
        while (totalConsumedRecords < recordsCount) {
            int consumed = super.consumeAvailableRecords(record -> {
            });
            if (consumed > 0) {
                totalConsumedRecords += consumed;
                System.out.println("consumed " + totalConsumedRecords + " records");
            }
        }
        System.out.println("total duration to ingest '" + recordsCount + "' records: " +
                Strings.duration(System.currentTimeMillis() - start));
    }

    @Test
    @Ignore
    public void testSnapshotPerformance() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE);
        final long recordsCount = 1000000;
        final int batchSize = 1000;

        batchInsertRecords(recordsCount, batchSize).get();

        // start the connector only after we've finished inserting all the records
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        CompletableFuture.runAsync(() -> consumeRecords(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
    }

    @Test
    @FixFor("DBZ-1242")
    public void testEmptySchemaWarningAfterApplyingFilters() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor();

        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY.getValue())
                .with(PostgresConnectorConfig.TABLE_WHITELIST, "my_products");

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForAvailableRecords(10 * (TestHelper.waitTimeForRecords() * 5), TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(NO_MONITORED_TABLES_WARNING)).isTrue());
    }

    @Test
    @FixFor("DBZ-1242")
    public void testNoEmptySchemaWarningAfterApplyingFilters() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor();

        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY.getValue());

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(NO_MONITORED_TABLES_WARNING)).isFalse());
    }

    @Test
    @FixFor("DBZ-1436")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Publication configuration only valid for PGOUTPUT decoder")
    public void testCustomPublicationNameUsed() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor();

        TestHelper.dropAllSchemas();
        TestHelper.dropPublication("cdc");
        TestHelper.executeDDL("postgres_create_tables.ddl");

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.PUBLICATION_NAME, "cdc");

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsMessage("Creating new publication 'cdc' for plugin 'PGOUTPUT'")).isTrue());
        assertTrue(TestHelper.publicationExists("cdc"));
    }

    @Test
    @FixFor("DBZ-1015")
    public void shouldRewriteIdentityKey() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.SCHEMA_WHITELIST, "s1,s2")
                // rewrite key from table 'a': from {pk} to {pk, aa}
                .with(PostgresConnectorConfig.MSG_KEY_COLUMNS, "(.*)1.a:pk,aa");

        start(PostgresConnector.class, configBuilder.build());
        waitForSnapshotToBeCompleted();
        SourceRecords records = consumeRecordsByTopic(2);
        records.recordsForTopic("test_server.s1.a").forEach(record -> {
            Struct key = (Struct) record.key();
            Assertions.assertThat(key.get(PK_FIELD)).isNotNull();
            Assertions.assertThat(key.get("aa")).isNotNull();
        });
        records.recordsForTopic("test_server.s2.a").forEach(record -> {
            Struct key = (Struct) record.key();
            Assertions.assertThat(key.get(PK_FIELD)).isNotNull();
            Assertions.assertThat(key.get("pk")).isNotNull();
            Assertions.assertThat(key.schema().field("aa")).isNull();
        });

        stopConnector();

    }

    @Test
    @FixFor("DBZ-1519")
    public void shouldNotIssueWarningForNoMonitoredTablesAfterApplyingFilters() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor();

        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.execute(INSERT_STMT);
        Configuration config = TestHelper.defaultConfig().with(PostgresConnectorConfig.SCHEMA_WHITELIST, "s2").build();

        // Start connector, verify that it does not log no monitored tables warning
        start(PostgresConnector.class, config);
        waitForSnapshotToBeCompleted();
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(logInterceptor.containsMessage(NO_MONITORED_TABLES_WARNING)).isFalse();
        stopConnector();

        // Restart connector, verify it does not log no monitored tables warning
        start(PostgresConnector.class, config);
        waitForStreamingRunning();
        assertThat(logInterceptor.containsMessage(NO_MONITORED_TABLES_WARNING)).isFalse();

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1684")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Publication test specifically for pgoutput")
    public void shouldCreatePublicationWhenReplicationSlotExists() throws Exception {
        // Start with a clean slate and create database objects
        TestHelper.dropAllSchemas();
        TestHelper.dropPublication();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.executeDDL("postgres_create_tables.ddl");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false)
                .build();

        // Start connector with no snapshot; by default replication slot and publication should be created
        // Wait until streaming mode begins to proceed
        start(PostgresConnector.class, config);
        waitForStreamingRunning();

        // Check that publication was created
        assertTrue(TestHelper.publicationExists());

        // Stop connector, drop publication
        stopConnector();
        TestHelper.dropPublication();

        // Create log interceptor and restart the connector, should observe publication gets re-created
        final LogInterceptor interceptor = new LogInterceptor();
        start(PostgresConnector.class, config);
        waitForStreamingRunning();

        // Check that publication was created
        assertTrue(TestHelper.publicationExists());

        // Stop Connector and check log messages
        stopConnector(value -> {
            assertThat(interceptor.containsMessage("Creating new publication 'dbz_publication' for plugin 'PGOUTPUT'")).isTrue();
        });
    }

    @Test
    @FixFor("DBZ-1685")
    public void shouldConsumeEventsWithMaskedColumns() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.MASK_COLUMN(5), "s2.a.bb");
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        assertThat(actualRecords.allRecordsInOrder().size()).isEqualTo(2);

        List<SourceRecord> recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(1);

        SourceRecord record = recordsForTopicS2.remove(0);
        VerifyRecord.isValidRead(record, PK_FIELD, 1);

        Struct value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getString("bb")).isEqualTo("*****");
        }

        // insert and verify inserts
        TestHelper.execute("INSERT INTO s2.a (aa,bb) VALUES (1, 'test');");

        actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(1);

        record = recordsForTopicS2.remove(0);
        VerifyRecord.isValidInsert(record, PK_FIELD, 2);

        value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getString("bb")).isEqualTo("*****");
        }

        // update and verify update
        TestHelper.execute("UPDATE s2.a SET aa=2, bb='hello' WHERE pk=2;");

        actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(1);

        record = recordsForTopicS2.remove(0);
        VerifyRecord.isValidUpdate(record, PK_FIELD, 2);

        value = (Struct) record.value();
        if (value.getStruct("before") != null) {
            assertThat(value.getStruct("before").getString("bb")).isEqualTo("*****");
        }
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getString("bb")).isEqualTo("*****");
        }

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1292")
    public void shouldOutputRecordsInCloudEventsFormat() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE);

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        SourceRecords snapshotRecords = consumeRecordsByTopic(2);
        List<SourceRecord> snapshot = snapshotRecords.allRecordsInOrder();

        for (SourceRecord record : snapshot) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(record, "postgresql", "test_server", false);
        }

        // insert some more records and test streaming
        TestHelper.execute(INSERT_STMT);

        Testing.Print.enable();

        final List<SourceRecord> streaming = new ArrayList<SourceRecord>();
        Awaitility.await().atMost(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS).until(() -> {
            // Should be BEGIN + END in case of empty tx or BEGIN + data in case of our TX
            final SourceRecords streamingRecords = consumeRecordsByTopic(2);
            final SourceRecord second = streamingRecords.allRecordsInOrder().get(1);
            if (!second.topic().endsWith(".transaction")) {
                streaming.add(second);
                return true;
            }
            return false;
        });

        // Should be DATA + END for the rest of TX
        SourceRecords streamingRecords = consumeRecordsByTopic(2);
        streaming.add(streamingRecords.allRecordsInOrder().get(0));

        for (SourceRecord record : streaming) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(record, true);
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(record, true);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(record, "postgresql", "test_server", true);
        }

        stopConnector();
    }

    private CompletableFuture<Void> batchInsertRecords(long recordsCount, int batchSize) {
        String insertStmt = "INSERT INTO text_table(j, jb, x, u) " +
                "VALUES ('{\"bar\": \"baz\"}'::json, '{\"bar\": \"baz\"}'::jsonb, " +
                "'<foo>bar</foo><foo>bar</foo>'::xml, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID);";
        return CompletableFuture.runAsync(() -> {
            StringBuilder stmtBuilder = new StringBuilder();
            for (int i = 0; i < recordsCount; i++) {
                stmtBuilder.append(insertStmt).append(System.lineSeparator());
                if (i > 0 && i % batchSize == 0) {
                    System.out.println("inserting batch [" + (i - batchSize) + "," + i + "]");
                    TestHelper.execute(stmtBuilder.toString());
                    stmtBuilder.delete(0, stmtBuilder.length());
                }
            }
            System.out.println("inserting batch [" + (recordsCount - batchSize) + "," + recordsCount + "]");
            TestHelper.execute(stmtBuilder.toString());
            stmtBuilder.delete(0, stmtBuilder.length());
        }).exceptionally(throwable -> {
            throw new RuntimeException(throwable);
        });
    }

    private Predicate<SourceRecord> stopOnPKPredicate(int pkValue) {
        return record -> {
            Struct key = (Struct) record.key();
            return ((Integer) key.get(PK_FIELD)) == pkValue;
        };
    }

    private void assertRecordsFromSnapshot(int expectedCount, int... pks) throws InterruptedException {
        SourceRecords actualRecords = consumeRecordsByTopic(expectedCount);
        assertThat(actualRecords.allRecordsInOrder().size()).isEqualTo(expectedCount);

        // we have 2 schemas/topics that we expect
        int expectedCountPerSchema = expectedCount / 2;

        List<SourceRecord> recordsForTopicS1 = actualRecords.recordsForTopic(topicName("s1.a"));
        assertThat(recordsForTopicS1.size()).isEqualTo(expectedCountPerSchema);
        IntStream.range(0, expectedCountPerSchema)
                .forEach(i -> VerifyRecord.isValidRead(recordsForTopicS1.remove(0), PK_FIELD, pks[i]));

        List<SourceRecord> recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(expectedCountPerSchema);
        IntStream.range(0, expectedCountPerSchema)
                .forEach(i -> VerifyRecord.isValidRead(recordsForTopicS2.remove(0), PK_FIELD, pks[i + expectedCountPerSchema]));
    }

    private void assertRecordsAfterInsert(int expectedCount, int... pks) throws InterruptedException {
        SourceRecords actualRecords = consumeRecordsByTopic(expectedCount);
        assertThat(actualRecords.topics().size()).isEqualTo(expectedCount);

        // we have 2 schemas
        int expectedCountPerSchema = expectedCount / 2;

        List<SourceRecord> recordsForTopicS1 = actualRecords.recordsForTopic(topicName("s1.a"));
        assertThat(recordsForTopicS1.size()).isEqualTo(expectedCountPerSchema);
        IntStream.range(0, expectedCountPerSchema).forEach(i -> VerifyRecord.isValidInsert(recordsForTopicS1.remove(0), PK_FIELD, pks[i]));

        List<SourceRecord> recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(expectedCountPerSchema);
        IntStream.range(0, expectedCountPerSchema).forEach(i -> VerifyRecord.isValidInsert(recordsForTopicS2.remove(0), PK_FIELD, pks[i]));
    }

    protected void assertSourceInfoMicrosecondTransactionTimestamp(SourceRecord record, long ts_usec, long tolerance_usec) {
        assertTrue(record.value() instanceof Struct);
        Struct source = ((Struct) record.value()).getStruct("source");
        // 1 minute difference is okay
        System.out.println("TS_USEC\t" + source.getInt64("ts_usec"));
        assertTrue(Math.abs(ts_usec - source.getInt64("ts_usec")) < tolerance_usec);
    }

    protected void assertSourceInfoMillisecondTransactionTimestamp(SourceRecord record, long ts_ms, long tolerance_ms) {
        assertTrue(record.value() instanceof Struct);
        Struct source = ((Struct) record.value()).getStruct("source");
        // 1 minute difference is okay
        System.out.println("TS_MS\t" + source.getInt64("ts_ms"));
        assertTrue(Math.abs(ts_ms - source.getInt64("ts_ms")) < tolerance_ms);
    }

    private <T> void validateField(Config config, Field field, T expectedValue) {
        assertNoConfigurationErrors(config, field);
        Object actualValue = configValue(config, field.name()).value();
        if (actualValue == null) {
            actualValue = field.defaultValue();
        }
        if (expectedValue == null) {
            assertThat(actualValue).isNull();
        }
        else {
            if (expectedValue instanceof EnumeratedValue) {
                assertThat(((EnumeratedValue) expectedValue).getValue()).isEqualTo(actualValue.toString());
            }
            else {
                assertThat(expectedValue).isEqualTo(actualValue);
            }
        }
    }

    private void validateFieldDef(Field expected) {
        ConfigDef configDef = connector.config();
        assertThat(configDef.names()).contains(expected.name());
        ConfigDef.ConfigKey key = configDef.configKeys().get(expected.name());
        assertThat(key).isNotNull();
        assertThat(key.name).isEqualTo(expected.name());
        assertThat(key.displayName).isEqualTo(expected.displayName());
        assertThat(key.importance).isEqualTo(expected.importance());
        assertThat(key.documentation).isEqualTo(expected.description());
        assertThat(key.type).isEqualTo(expected.type());
        assertThat(key.defaultValue).isEqualTo(expected.defaultValue());
        assertThat(key.dependents).isEqualTo(expected.dependents());
        assertThat(key.width).isNotNull();
        assertThat(key.group).isNotNull();
        assertThat(key.orderInGroup).isGreaterThan(0);
        assertThat(key.validator).isNull();
        assertThat(key.recommender).isNull();
    }

    private void waitForSnapshotToBeCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted("postgres", TestHelper.TEST_SERVER);
    }

    private void waitForStreamingRunning() throws InterruptedException {
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
    }
}

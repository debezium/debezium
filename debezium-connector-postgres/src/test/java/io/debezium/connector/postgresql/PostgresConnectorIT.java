/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static junit.framework.TestCase.assertEquals;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import javax.management.InstanceNotFoundException;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
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

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.Version;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.postgresql.PostgresConnectorConfig.LogicalDecoder;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.junit.SkipTestDependingOnDecoderPluginNameRule;
import io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIs;
import io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIsNot;
import io.debezium.converters.CloudEventsConverterTest;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.SkipWhenKafkaVersion;
import io.debezium.junit.SkipWhenKafkaVersion.KafkaVersion;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.schema.DatabaseSchema;
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
    private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "DROP SCHEMA IF EXISTS s2 CASCADE;" +
            "CREATE SCHEMA s1; " +
            "CREATE SCHEMA s2; " +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s2.a (pk SERIAL, aa integer, bb varchar(20), PRIMARY KEY(pk));";
    private static final String SETUP_TABLES_STMT = CREATE_TABLES_STMT + INSERT_STMT;
    private PostgresConnector connector;

    @Rule
    public final TestRule skipName = new SkipTestDependingOnDecoderPluginNameRule();

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
    public void shouldNotStartWithInvalidSlotConfigAndUserRoles() throws Exception {
        // Start with a clean slate and create database objects
        TestHelper.dropAllSchemas();
        TestHelper.dropPublication();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute("CREATE USER badboy WITH PASSWORD 'failing';", "GRANT ALL PRIVILEGES ON DATABASE postgres TO badboy;");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.SLOT_NAME, ReplicationConnection.Builder.DEFAULT_SLOT_NAME)
                .build();

        start(PostgresConnector.class, config);
        waitForStreamingRunning();

        Configuration failingConfig = TestHelper.defaultConfig()
                .with("name", "failingPGConnector")
                .with(PostgresConnectorConfig.DATABASE_CONFIG_PREFIX + JdbcConfiguration.USER, "badboy")
                .with(PostgresConnectorConfig.DATABASE_CONFIG_PREFIX + JdbcConfiguration.PASSWORD, "failing")
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.SLOT_NAME, ReplicationConnection.Builder.DEFAULT_SLOT_NAME)
                .build();
        List<ConfigValue> validatedConfig = new PostgresConnector().validate(failingConfig.asMap()).configValues();

        final List<String> invalidProperties = Collections.singletonList("database.user");
        validatedConfig.forEach(
                configValue -> {
                    if (!invalidProperties.contains(configValue.name())) {
                        assertTrue("Unexpected error for \"" + configValue.name() + "\": " + configValue.errorMessages(), configValue.errorMessages().isEmpty());
                    }
                });
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
        validateConfigField(validatedConfig, PostgresConnectorConfig.PLUGIN_NAME, LogicalDecoder.DECODERBUFS.getValue());
        validateConfigField(validatedConfig, PostgresConnectorConfig.SLOT_NAME, ReplicationConnection.Builder.DEFAULT_SLOT_NAME);
        validateConfigField(validatedConfig, PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        validateConfigField(validatedConfig, PostgresConnectorConfig.PORT, PostgresConnectorConfig.DEFAULT_PORT);
        validateConfigField(validatedConfig, PostgresConnectorConfig.MAX_QUEUE_SIZE, PostgresConnectorConfig.DEFAULT_MAX_QUEUE_SIZE);
        validateConfigField(validatedConfig, PostgresConnectorConfig.MAX_BATCH_SIZE, PostgresConnectorConfig.DEFAULT_MAX_BATCH_SIZE);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SNAPSHOT_FETCH_SIZE, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.POLL_INTERVAL_MS, PostgresConnectorConfig.DEFAULT_POLL_INTERVAL_MILLIS);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SSL_MODE, PostgresConnectorConfig.SecureConnectionMode.DISABLED);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SSL_CLIENT_CERT, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SSL_CLIENT_KEY, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SSL_CLIENT_KEY_PASSWORD, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SSL_ROOT_CERT, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SCHEMA_WHITELIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SCHEMA_BLACKLIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SCHEMA_EXCLUDE_LIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.TABLE_WHITELIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.TABLE_INCLUDE_LIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.TABLE_BLACKLIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.TABLE_EXCLUDE_LIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.COLUMN_BLACKLIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.COLUMN_EXCLUDE_LIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.COLUMN_WHITELIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.COLUMN_INCLUDE_LIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.MSG_KEY_COLUMNS, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL);
        validateConfigField(validatedConfig, RelationalDatabaseConnectorConfig.SNAPSHOT_LOCK_TIMEOUT_MS,
                RelationalDatabaseConnectorConfig.DEFAULT_SNAPSHOT_LOCK_TIMEOUT_MILLIS);
        validateConfigField(validatedConfig, PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ADAPTIVE);
        validateConfigField(validatedConfig, PostgresConnectorConfig.DECIMAL_HANDLING_MODE, PostgresConnectorConfig.DecimalHandlingMode.PRECISE);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SSL_SOCKET_FACTORY, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.TCP_KEEPALIVE, true);
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
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1");
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted();
        SourceRecords records = consumeRecordsByTopic(recordCount);
        Assertions.assertThat(records.recordsForTopic("test_server.s1.a")).hasSize(recordCount);
    }

    @Test
    public void shouldConsumeMessagesFromSnapshotOld() throws Exception {
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
                    .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "changepk")
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
    public void shouldReceiveChangesForChangeColumnDefault() throws Exception {
        Testing.Print.enable();
        final String slotName = "default_change" + new Random().nextInt(100);
        TestHelper.create().dropReplicationSlot(slotName);
        try {
            final PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                    .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, Boolean.FALSE)
                    .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "default_change")
                    .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                    .with(PostgresConnectorConfig.SLOT_NAME, slotName)
                    .build());

            final String topicName = topicName("default_change.test_table");

            TestHelper.execute(
                    "CREATE SCHEMA IF NOT EXISTS default_change;",
                    "DROP TABLE IF EXISTS default_change.test_table;",
                    "CREATE TABLE default_change.test_table (pk SERIAL, i INT DEFAULT 1, text TEXT DEFAULT 'foo', PRIMARY KEY(pk));",
                    "INSERT INTO default_change.test_table(i, text) VALUES (DEFAULT, DEFAULT);");

            start(PostgresConnector.class, config.getConfig());

            assertConnectorIsRunning();
            waitForSnapshotToBeCompleted();

            // check the records from the snapshot
            final SourceRecords snapshotRecords = consumeRecordsByTopic(1);

            final Integer pkExpectedDefault = 0;
            final Integer snapshotIntDefault = 1;
            final String snapshotTextDefault = "foo";
            snapshotRecords.recordsForTopic(topicName).forEach(snapshotRecord -> {
                assertValueField(snapshotRecord, "after/pk", 1);
                assertValueField(snapshotRecord, "after/i", snapshotIntDefault);
                assertValueField(snapshotRecord, "after/text", snapshotTextDefault);

                assertThat(readRecordFieldDefault(snapshotRecord, "pk")).isEqualTo(pkExpectedDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "i")).isEqualTo(snapshotIntDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "text")).isEqualTo(snapshotTextDefault);
            });

            waitForStreamingRunning();

            TestHelper.execute(
                    "INSERT INTO default_change.test_table(i, text) VALUES (DEFAULT, DEFAULT);",
                    "INSERT INTO default_change.test_table(i, text) VALUES (DEFAULT, DEFAULT);",
                    "ALTER TABLE default_change.test_table ALTER COLUMN i SET DEFAULT 2;",
                    "ALTER TABLE default_change.test_table ALTER COLUMN text SET DEFAULT 'bar';",
                    "INSERT INTO default_change.test_table(i, text) VALUES (DEFAULT, DEFAULT);",
                    "ALTER TABLE default_change.test_table ALTER COLUMN i SET DEFAULT 3;",
                    "ALTER TABLE default_change.test_table ALTER COLUMN text SET DEFAULT 'baz';",
                    "INSERT INTO default_change.test_table(i, text) VALUES (DEFAULT, DEFAULT);");

            final Integer secondIntDefault = 2;
            final String secondTextDefault = "bar";
            final Integer thirdIntDefault = 3;
            final String thirdTextDefault = "baz";

            final Integer schemaIntDefaultAfterAlter = DecoderDifferences.areDefaultValuesRefreshedEagerly() ? thirdIntDefault : snapshotIntDefault;
            final String schemaTextDefaultAfterAlter = DecoderDifferences.areDefaultValuesRefreshedEagerly() ? thirdTextDefault : snapshotTextDefault;

            // check records inserted with i=1, text='foo' default
            final SourceRecords firstBatchRecords = consumeRecordsByTopic(2);

            firstBatchRecords.recordsForTopic(topicName).forEach(snapshotRecord -> {
                assertValueField(snapshotRecord, "after/i", snapshotIntDefault);
                assertValueField(snapshotRecord, "after/text", snapshotTextDefault);

                assertThat(readRecordFieldDefault(snapshotRecord, "pk")).isEqualTo(pkExpectedDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "i")).isEqualTo(schemaIntDefaultAfterAlter);
                assertThat(readRecordFieldDefault(snapshotRecord, "text")).isEqualTo(schemaTextDefaultAfterAlter);
            });

            // check records inserted with i=2, text='bar' default
            final SourceRecords secondBatchRecords = consumeRecordsByTopic(1);

            secondBatchRecords.recordsForTopic(topicName).forEach(snapshotRecord -> {
                assertValueField(snapshotRecord, "after/i", secondIntDefault);
                assertValueField(snapshotRecord, "after/text", secondTextDefault);

                assertThat(readRecordFieldDefault(snapshotRecord, "pk")).isEqualTo(pkExpectedDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "i")).isEqualTo(schemaIntDefaultAfterAlter);
                assertThat(readRecordFieldDefault(snapshotRecord, "text")).isEqualTo(schemaTextDefaultAfterAlter);
            });

            // check records inserted with i=3, text='baz' default
            final SourceRecords thirdBatchRecords = consumeRecordsByTopic(1);

            thirdBatchRecords.recordsForTopic(topicName).forEach(snapshotRecord -> {
                assertValueField(snapshotRecord, "after/i", thirdIntDefault);
                assertValueField(snapshotRecord, "after/text", thirdTextDefault);

                assertThat(readRecordFieldDefault(snapshotRecord, "pk")).isEqualTo(pkExpectedDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "i")).isEqualTo(schemaIntDefaultAfterAlter);
                assertThat(readRecordFieldDefault(snapshotRecord, "text")).isEqualTo(schemaTextDefaultAfterAlter);
            });

            // restart the connector, starting with a new record which should have refreshed schema
            stopConnector();

            TestHelper.execute("INSERT INTO default_change.test_table(i, text) VALUES (DEFAULT, DEFAULT);");

            start(PostgresConnector.class, config.getConfig());

            assertConnectorIsRunning();

            // check that the schema defaults will be in-sync after restart refreshes schema
            final SourceRecords afterRestartRecords = consumeRecordsByTopic(1);

            afterRestartRecords.recordsForTopic(topicName).forEach(snapshotRecord -> {
                assertValueField(snapshotRecord, "after/pk", 6);
                assertValueField(snapshotRecord, "after/i", thirdIntDefault);
                assertValueField(snapshotRecord, "after/text", thirdTextDefault);

                assertThat(readRecordFieldDefault(snapshotRecord, "pk")).isEqualTo(pkExpectedDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "i")).isEqualTo(thirdIntDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "text")).isEqualTo(thirdTextDefault);
            });

            TestHelper.execute(
                    "ALTER TABLE default_change.test_table ALTER COLUMN i SET DEFAULT 4;",
                    "ALTER TABLE default_change.test_table ALTER COLUMN text SET DEFAULT 'boo';",
                    "ALTER TABLE default_change.test_table ADD COLUMN tstz TIMESTAMPTZ DEFAULT '2021-03-20 14:44:28 +1'::TIMESTAMPTZ;",
                    "INSERT INTO default_change.test_table(i, text, tstz) VALUES (DEFAULT, DEFAULT, DEFAULT);");

            // check that the schema defaults will be in-sync after column changes refreshes schema
            final Integer refreshedIntDefault = 4;
            final String refreshedTextDefault = "boo";
            final String refreshedTstzDefault = Instant.ofEpochSecond(1616247868).toString();
            final SourceRecords afterRefreshRecords = consumeRecordsByTopic(1);

            afterRefreshRecords.recordsForTopic(topicName).forEach(snapshotRecord -> {
                assertValueField(snapshotRecord, "after/pk", 7);
                assertValueField(snapshotRecord, "after/i", refreshedIntDefault);
                assertValueField(snapshotRecord, "after/text", refreshedTextDefault);
                assertValueField(snapshotRecord, "after/tstz", refreshedTstzDefault);

                assertThat(readRecordFieldDefault(snapshotRecord, "pk")).isEqualTo(pkExpectedDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "i")).isEqualTo(refreshedIntDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "text")).isEqualTo(refreshedTextDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "tstz")).isEqualTo(refreshedTstzDefault);
            });

            stopConnector();
            TestHelper.create().dropReplicationSlot(slotName);

            TestHelper.execute("DROP SCHEMA IF EXISTS default_change CASCADE;");
        }
        catch (Throwable t) {
            stopConnector(null);
            TestHelper.create().dropReplicationSlot(slotName);
            throw t;
        }
    }

    @Test
    public void showThatSchemaColumnDefaultMayApplyRetroactively() throws Exception {
        Testing.Print.enable();
        final String slotName = "default_change" + new Random().nextInt(100);
        TestHelper.create().dropReplicationSlot(slotName);
        try {
            final PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                    .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, Boolean.FALSE)
                    .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "default_change")
                    .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                    .with(PostgresConnectorConfig.SLOT_NAME, slotName)
                    .build());

            final String topicName = topicName("default_change.test_table");

            TestHelper.execute(
                    "CREATE SCHEMA IF NOT EXISTS default_change;",
                    "DROP TABLE IF EXISTS default_change.test_table;",
                    "CREATE TABLE default_change.test_table (pk SERIAL, i INT DEFAULT 1, text TEXT DEFAULT 'foo', PRIMARY KEY(pk));",
                    "INSERT INTO default_change.test_table(i, text) VALUES (DEFAULT, DEFAULT);");

            start(PostgresConnector.class, config.getConfig());

            assertConnectorIsRunning();
            waitForSnapshotToBeCompleted();

            // check the records from the snapshot
            final SourceRecords snapshotRecords = consumeRecordsByTopic(1);

            final Integer pkExpectedDefault = 0;
            final Integer initialIntDefault = 1;
            final String initialTextDefault = "foo";
            snapshotRecords.recordsForTopic(topicName).forEach(snapshotRecord -> {
                assertValueField(snapshotRecord, "after/pk", 1);
                assertValueField(snapshotRecord, "after/i", initialIntDefault);
                assertValueField(snapshotRecord, "after/text", initialTextDefault);

                assertThat(readRecordFieldDefault(snapshotRecord, "pk")).isEqualTo(pkExpectedDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "i")).isEqualTo(initialIntDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "text")).isEqualTo(initialTextDefault);
            });

            stopConnector();

            // default changes interwoven with updates while connector stopped
            TestHelper.execute("ALTER TABLE default_change.test_table ADD COLUMN bi BIGINT DEFAULT 1;",
                    "INSERT INTO default_change.test_table(i, text, bi) VALUES (DEFAULT, DEFAULT, DEFAULT);",
                    "ALTER TABLE default_change.test_table ALTER COLUMN i SET DEFAULT 2;",
                    "ALTER TABLE default_change.test_table ALTER COLUMN text SET DEFAULT 'bar';",
                    "ALTER TABLE default_change.test_table ALTER COLUMN bi SET DEFAULT 2;",
                    "ALTER TABLE default_change.test_table ADD COLUMN tstz TIMESTAMPTZ DEFAULT '2021-03-20 14:44:28 +1'::TIMESTAMPTZ;",
                    "INSERT INTO default_change.test_table(i, text, bi, tstz) VALUES (DEFAULT, DEFAULT, DEFAULT, DEFAULT);");

            start(PostgresConnector.class, config.getConfig());

            assertConnectorIsRunning();

            // check that the schema defaults will be in-sync after restart refreshes schema
            final Integer refreshedIntDefault = 2;
            final String refreshedTextDefault = "bar";
            final Long initialBigIntDefault = 1L;
            final Long refreshedBigIntDefault = 2L;
            final String refreshedTstzDefault = Instant.ofEpochSecond(1616247868).toString();
            final SourceRecords oldOfflineRecord = consumeRecordsByTopic(1);

            // record fields will have the default value that was applied on insert, but schema will show the current default value
            oldOfflineRecord.recordsForTopic(topicName).forEach(snapshotRecord -> {
                assertValueField(snapshotRecord, "after/pk", 2);
                assertValueField(snapshotRecord, "after/i", initialIntDefault);
                assertValueField(snapshotRecord, "after/text", initialTextDefault);
                assertValueField(snapshotRecord, "after/bi", initialBigIntDefault);

                assertThat(readRecordFieldDefault(snapshotRecord, "pk")).isEqualTo(pkExpectedDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "i")).isEqualTo(refreshedIntDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "text")).isEqualTo(refreshedTextDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "bi")).isEqualTo(refreshedBigIntDefault);
                assertThat(getRecordFieldFromAfter(snapshotRecord, "tstz")).isNull();
            });

            final SourceRecords latestOfflineRecord = consumeRecordsByTopic(1);

            latestOfflineRecord.recordsForTopic(topicName).forEach(snapshotRecord -> {
                assertValueField(snapshotRecord, "after/pk", 3);
                assertValueField(snapshotRecord, "after/i", refreshedIntDefault);
                assertValueField(snapshotRecord, "after/text", refreshedTextDefault);
                assertValueField(snapshotRecord, "after/bi", refreshedBigIntDefault);
                assertValueField(snapshotRecord, "after/tstz", refreshedTstzDefault);

                assertThat(readRecordFieldDefault(snapshotRecord, "pk")).isEqualTo(pkExpectedDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "i")).isEqualTo(refreshedIntDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "text")).isEqualTo(refreshedTextDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "bi")).isEqualTo(refreshedBigIntDefault);
                assertThat(readRecordFieldDefault(snapshotRecord, "tstz")).isEqualTo(refreshedTstzDefault);
            });

            stopConnector();
            TestHelper.create().dropReplicationSlot(slotName);

            TestHelper.execute("DROP SCHEMA IF EXISTS default_change CASCADE;");
        }
        catch (Throwable t) {
            stopConnector(null);
            TestHelper.create().dropReplicationSlot(slotName);
            throw t;
        }
    }

    private static Object readRecordFieldDefault(SourceRecord record, String field) {
        return getRecordFieldFromAfter(record, field).schema().defaultValue();
    }

    private static org.apache.kafka.connect.data.Field getRecordFieldFromAfter(SourceRecord record, String field) {
        return ((Struct) record.value()).getStruct("after").schema().field(field);
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
        waitForStreamingRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(6);
        assertKey(actualRecords.allRecordsInOrder().get(0), "pk", 1);
        assertKey(actualRecords.allRecordsInOrder().get(1), "pk", 2);

        // JdbcConnection#connection() is called multiple times during connector start-up,
        // so the given statements will be executed multiple times, resulting in multiple
        // records; here we're interested just in the first insert for s2.a
        assertValueField(actualRecords.allRecordsInOrder().get(5), "after/bb", "hello; world");
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
        waitForSnapshotToBeCompleted();

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
        waitForSnapshotToBeCompleted();

        assertRecordsFromSnapshot(4, 1, 2, 1, 2);
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
    }

    @Test
    @FixFor("DBZ-1857")
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 10, reason = "Database version less than 10.0")
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
    }

    @Test
    public void shouldTakeExcludeListFiltersIntoAccount() throws Exception {
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
                .with(PostgresConnectorConfig.SCHEMA_EXCLUDE_LIST, "s2")
                .with(PostgresConnectorConfig.TABLE_EXCLUDE_LIST, ".+b")
                .with(PostgresConnectorConfig.COLUMN_EXCLUDE_LIST, ".+bb");

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
    @FixFor("DBZ-1962")
    public void shouldTakeColumnIncludeListFilterIntoAccount() throws Exception {
        String setupStmt = SETUP_TABLES_STMT +
                "ALTER TABLE s1.a ADD COLUMN bb integer;" +
                "ALTER TABLE s1.a ADD COLUMN cc char(12);" +
                "INSERT INTO s1.a (aa, bb) VALUES (2, 2);";

        TestHelper.execute(setupStmt);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with("column.mask.with.5.chars", ".+cc")
                .with(PostgresConnectorConfig.COLUMN_INCLUDE_LIST, ".+aa,.+cc");

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(1);
        List<SourceRecord> recordsForS1a = actualRecords.recordsForTopic(topicName("s1.a"));
        recordsForS1a.forEach(record -> {
            assertFieldAbsent(record, "bb");

            Struct recordValue = ((Struct) record.value());
            assertThat(recordValue.getStruct("after").getString("cc")).isEqualTo("*****");
        });
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
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, tableWhitelistWithWhitespace);

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
    public void shouldRemoveWhiteSpaceCharsOld() throws Exception {
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
    @FixFor("DBZ-2118")
    public void shouldCloseTxAfterTypeQuery() throws Exception {
        String setupStmt = SETUP_TABLES_STMT;

        TestHelper.execute(setupStmt);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.b")
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true);

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        TestHelper.execute("CREATE TABLE s1.b (pk SERIAL, aa isbn, PRIMARY KEY(pk));", "INSERT INTO s1.b (aa) VALUES ('978-0-393-04002-9')");
        SourceRecords actualRecords = consumeRecordsByTopic(1);

        List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s1.b"));
        assertThat(records.size()).isEqualTo(1);

        SourceRecord record = records.get(0);
        VerifyRecord.isValidInsert(record, PK_FIELD, 1);
        final String isbn = new String(((Struct) record.value()).getStruct("after").getBytes("aa"));
        Assertions.assertThat(isbn).isEqualTo("0-393-04002-X");

        TestHelper.assertNoOpenTransactions();
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
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.dbz_878_some\\|test@data");

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
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.a")
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
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.a")
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
    @FixFor("DBZ-2660")
    public void shouldRegularlyFlushLsnWithTxMonitoring() throws InterruptedException, SQLException {
        final int recordCount = 10;
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.a")
                .with(PostgresConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        final String txTopic = topicName("transaction");
        TestHelper.execute(INSERT_STMT);
        final SourceRecords firstRecords = consumeDmlRecordsByTopic(1);
        assertThat(firstRecords.topics().size()).isEqualTo(2);
        assertThat(firstRecords.recordsForTopic(txTopic).size()).isGreaterThanOrEqualTo(2);
        Assertions.assertThat(firstRecords.recordsForTopic(txTopic).get(1).sourceOffset().containsKey("lsn_commit")).isTrue();
        stopConnector();
        assertConnectorNotRunning();

        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
        // there shouldn't be any snapshot records, only potentially transaction messages
        assertOnlyTransactionRecordsToConsume();

        final Set<String> flushLsn = new HashSet<>();
        try (final PostgresConnection connection = TestHelper.create()) {
            flushLsn.add(getConfirmedFlushLsn(connection));
            for (int i = 2; i <= recordCount + 2; i++) {
                TestHelper.execute(INSERT_STMT);

                final SourceRecords actualRecords = consumeDmlRecordsByTopic(1);
                assertThat(actualRecords.topics().size()).isEqualTo(2);
                assertThat(actualRecords.recordsForTopic(txTopic).size()).isGreaterThanOrEqualTo(2);
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
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.a")
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
    @FixFor("DBZ-2456")
    public void shouldAllowForSelectiveSnapshot() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS.name())
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "s1.a")
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        /* Snapshot must be taken only for the listed tables */
        SourceRecords actualRecords = consumeRecordsByTopic(1);
        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));

        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs).isNull();
        VerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);

        /* streaming should work normally */
        TestHelper.execute(INSERT_STMT);
        actualRecords = consumeRecordsByTopic(2);

        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));

        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        VerifyRecord.isValidInsert(s1recs.get(0), PK_FIELD, 2);
        VerifyRecord.isValidInsert(s2recs.get(0), PK_FIELD, 2);

        stopConnector();

        /* start the connector back up and make sure snapshot is being taken */
        start(PostgresConnector.class, configBuilder
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_TABLES, "s2.a")
                .build());
        assertConnectorIsRunning();

        actualRecords = consumeRecordsByTopic(2);
        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));

        assertThat(s2recs.size()).isEqualTo(2);
        assertThat(s1recs).isNull();
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
    @FixFor("DBZ-2288")
    @SkipWhenDecoderPluginNameIs(value = SkipWhenDecoderPluginNameIs.DecoderPluginName.PGOUTPUT, reason = "PgOutput needs publication for manually created slot")
    public void exportedSnapshotShouldNotSkipRecordOfParallelTx() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.createDefaultReplicationSlot();

        // Testing.Print.enable();
        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.execute(INSERT_STMT);

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.EXPORTED.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.MAX_QUEUE_SIZE, 2)
                .with(PostgresConnectorConfig.MAX_BATCH_SIZE, 1)
                .build();
        final PostgresConnection pgConnection = TestHelper.create();
        pgConnection.setAutoCommit(false);
        pgConnection.executeWithoutCommitting(INSERT_STMT);
        final AtomicBoolean inserted = new AtomicBoolean();
        start(PostgresConnector.class, config, loggingCompletion(), x -> false, x -> {
            if (!inserted.get()) {
                TestHelper.execute(INSERT_STMT);
                try {
                    pgConnection.commit();
                }
                catch (Exception e) {
                    throw new IllegalStateException(e);
                }
                inserted.set(true);
            }
        }, true);
        assertConnectorIsRunning();

        // Consume records from the snapshot
        SourceRecords actualRecords = consumeRecordsByTopic(4);

        // Consume records from concurrent transactions
        actualRecords = consumeRecordsByTopic(4);

        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(2);
        assertThat(s2recs.size()).isEqualTo(2);
    }

    @Test
    @FixFor("DBZ-2288")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Publication not supported")
    public void exportedSnapshotShouldNotSkipRecordOfParallelTxPgoutput() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.createDefaultReplicationSlot();
        TestHelper.execute("CREATE PUBLICATION dbz_publication FOR ALL TABLES;");

        // Testing.Print.enable();
        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.execute(INSERT_STMT);

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.EXPORTED.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.MAX_QUEUE_SIZE, 2)
                .with(PostgresConnectorConfig.MAX_BATCH_SIZE, 1)
                .build();
        final PostgresConnection pgConnection = TestHelper.create();
        pgConnection.setAutoCommit(false);
        pgConnection.executeWithoutCommitting(INSERT_STMT);
        final AtomicBoolean inserted = new AtomicBoolean();
        start(PostgresConnector.class, config, loggingCompletion(), x -> false, x -> {
            if (!inserted.get()) {
                TestHelper.execute(INSERT_STMT);
                try {
                    pgConnection.commit();
                }
                catch (Exception e) {
                    throw new IllegalStateException(e);
                }
                inserted.set(true);
            }
        }, true);
        assertConnectorIsRunning();

        // Consume records from the snapshot
        SourceRecords actualRecords = consumeRecordsByTopic(4);

        // Consume records from concurrent transactions
        actualRecords = consumeRecordsByTopic(4);

        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(2);
        assertThat(s2recs.size()).isEqualTo(2);
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

    @Test
    @FixFor("DBZ-2094")
    @SkipWhenDecoderPluginNameIs(value = SkipWhenDecoderPluginNameIs.DecoderPluginName.WAL2JSON, reason = "No need for db write to complete catch-up phase")
    public void shouldResumeStreamingFromSlotPositionForCustomSnapshot() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        // Perform an regular snapshot
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomStartFromStreamingTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        VerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);
        VerifyRecord.isValidRead(s2recs.get(0), PK_FIELD, 1);

        stopConnector();

        // Insert records while connector is stopped
        TestHelper.execute(INSERT_STMT);

        // Perform catch up streaming and resnapshot everything
        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomStartFromStreamingTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted();

        // Expect duplicate records from the snapshot and while streaming is running
        actualRecords = consumeRecordsByTopic(6);

        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(3);
        assertThat(s2recs.size()).isEqualTo(3);

        // Validate the first record is from streaming
        VerifyRecord.isValidInsert(s1recs.get(0), PK_FIELD, 2);
        VerifyRecord.isValidInsert(s2recs.get(0), PK_FIELD, 2);

        // Validate the rest of the records are from the snapshot
        VerifyRecord.isValidRead(s1recs.get(1), PK_FIELD, 1);
        VerifyRecord.isValidRead(s1recs.get(2), PK_FIELD, 2);
        VerifyRecord.isValidRead(s2recs.get(1), PK_FIELD, 1);
        VerifyRecord.isValidRead(s2recs.get(2), PK_FIELD, 2);

        TestHelper.assertNoOpenTransactions();
    }

    @Test
    @FixFor("DBZ-2772")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.WAL2JSON, reason = "Requires db write to complete catch-up phase")
    public void shouldResumeStreamingFromSlotPositionForCustomSnapshotWal2Json() throws Exception {
        Testing.Print.enable();
        TestHelper.execute(SETUP_TABLES_STMT);
        // Perform an regular snapshot
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomStartFromStreamingTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        VerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);
        VerifyRecord.isValidRead(s2recs.get(0), PK_FIELD, 1);

        stopConnector();
        assertConnectorNotRunning();
        // Insert records while connector is stopped
        TestHelper.execute(INSERT_STMT);

        // Perform catch up streaming and resnapshot everything
        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomStartFromStreamingTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        // Catch-up streaming might wait for a LSN after the one already process, typically for wal2json
        // It is thus necessary to write a record to increase LSN and trigger end of catchup phase
        // This looks like an issue for plug-in(s) that sends whole TX as a single record
        waitForStreamingRunning();
        TestHelper.execute("INSERT INTO s1.a (pk, aa) VALUES (1000, 1)");

        waitForSnapshotToBeCompleted();

        // Expect duplicate records from the snapshot and while streaming is running
        actualRecords = consumeRecordsByTopic(8);

        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(5);
        assertThat(s2recs.size()).isEqualTo(3);

        // Validate the first record is from streaming
        VerifyRecord.isValidInsert(s1recs.get(0), PK_FIELD, 2);
        VerifyRecord.isValidInsert(s2recs.get(0), PK_FIELD, 2);

        // Validate the catch-up complete write
        VerifyRecord.isValidInsert(s1recs.get(1), PK_FIELD, 1000);

        // Validate the rest of the records are from the snapshot
        VerifyRecord.isValidRead(s1recs.get(2), PK_FIELD, 1);
        VerifyRecord.isValidRead(s1recs.get(3), PK_FIELD, 2);
        VerifyRecord.isValidRead(s2recs.get(1), PK_FIELD, 1);
        VerifyRecord.isValidRead(s2recs.get(2), PK_FIELD, 2);

        TestHelper.assertNoOpenTransactions();
    }

    @Test
    @FixFor("DBZ-2094")
    @SkipWhenDecoderPluginNameIs(value = SkipWhenDecoderPluginNameIs.DecoderPluginName.WAL2JSON, reason = "Fails due to DBZ-3158")
    public void customSnapshotterSkipsTablesOnRestart() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor();

        TestHelper.execute(SETUP_TABLES_STMT);
        // Perform an regular snapshot using the always snapshotter
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        VerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);
        VerifyRecord.isValidRead(s2recs.get(0), PK_FIELD, 1);

        stopConnector();

        // Insert records while connector is stopped
        TestHelper.execute(INSERT_STMT);

        // Perform a custom partial snapshot
        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomPartialTableTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted();

        // wait for the second streaming phase
        waitForStreamingRunning();

        actualRecords = consumeRecordsByTopic(4);

        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(3);
        assertThat(s2recs.size()).isEqualTo(1);

        // streaming records
        VerifyRecord.isValidInsert(s1recs.get(0), PK_FIELD, 2);
        VerifyRecord.isValidInsert(s2recs.get(0), PK_FIELD, 2);

        // snapshot records
        VerifyRecord.isValidRead(s1recs.get(1), PK_FIELD, 1);
        VerifyRecord.isValidRead(s1recs.get(2), PK_FIELD, 2);

        assertNoRecordsToConsume();

        TestHelper.assertNoOpenTransactions();

        stopConnector(value -> assertThat(logInterceptor.containsMessage("For table 's2.a' the select statement was not provided, skipping table")).isTrue());
    }

    @Test
    @FixFor("DBZ-2094")
    public void customSnapshotterSkipsTablesOnRestartWithConcurrentTx() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor();

        Testing.Print.enable();
        TestHelper.execute(SETUP_TABLES_STMT);
        // Perform an regular snapshot using the always snapshotter
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        VerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);
        VerifyRecord.isValidRead(s2recs.get(0), PK_FIELD, 1);

        stopConnector();

        // Insert records while connector is stopped
        TestHelper.execute(INSERT_STMT);

        // Perform a custom partial snapshot
        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomPartialTableTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        Awaitility.await()
                .alias("Streaming was not started on time")
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .atMost(waitTimeForRecords() * 30, TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> {
                    // Required due to DBZ-3158, creates empty transaction
                    TestHelper.create().execute("vacuum full").close();
                    return (boolean) ManagementFactory.getPlatformMBeanServer()
                            .getAttribute(getSnapshotMetricsObjectName("postgres", TestHelper.TEST_SERVER), "SnapshotCompleted");
                });

        // wait for the second streaming phase
        waitForStreamingRunning();

        actualRecords = consumeRecordsByTopic(4);

        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(3);
        assertThat(s2recs.size()).isEqualTo(1);

        // streaming records
        VerifyRecord.isValidInsert(s1recs.get(0), PK_FIELD, 2);
        VerifyRecord.isValidInsert(s2recs.get(0), PK_FIELD, 2);

        // snapshot records
        VerifyRecord.isValidRead(s1recs.get(1), PK_FIELD, 1);
        VerifyRecord.isValidRead(s1recs.get(2), PK_FIELD, 2);

        assertNoRecordsToConsume();

        TestHelper.assertNoOpenTransactions();

        stopConnector(value -> assertThat(logInterceptor.containsMessage("For table 's2.a' the select statement was not provided, skipping table")).isTrue());
    }

    @Test
    @FixFor("DBZ-2608")
    public void testCustomSnapshotterSnapshotCompleteLifecycleHook() throws Exception {
        TestHelper.execute("DROP SCHEMA IF EXISTS s1 CASCADE;" +
                "CREATE SCHEMA s1; " +
                "CREATE TABLE s1.lifecycle_state (hook text, state text, PRIMARY KEY(hook));");
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomLifecycleHookTestSnapshot.class.getName())
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted();

        try (PostgresConnection connection = TestHelper.create()) {
            List<String> snapshotCompleteState = connection.queryAndMap(
                    "SELECT state FROM s1.lifecycle_state WHERE hook like 'snapshotComplete'",
                    rs -> {
                        final List<String> ret = new ArrayList<>();
                        while (rs.next()) {
                            ret.add(rs.getString(1));
                        }
                        return ret;
                    });
            assertEquals(Collections.singletonList("complete"), snapshotCompleteState);
        }
    }

    private String getConfirmedFlushLsn(PostgresConnection connection) throws SQLException {
        final String lsn = connection.prepareQueryAndMap(
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
        connection.rollback();
        return lsn;
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
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "my_products");

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForAvailableRecords(10 * (TestHelper.waitTimeForRecords() * 5), TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isTrue());
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

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isFalse());
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
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1,s2")
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
        Configuration config = TestHelper.defaultConfig().with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s2").build();

        // Start connector, verify that it does not log no monitored tables warning
        start(PostgresConnector.class, config);
        waitForSnapshotToBeCompleted();
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(logInterceptor.containsMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isFalse();
        stopConnector();

        // Restart connector, verify it does not log no monitored tables warning
        start(PostgresConnector.class, config);
        waitForStreamingRunning();
        assertThat(logInterceptor.containsMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isFalse();
    }

    @Test
    @FixFor("DBZ-2865")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.DECODERBUFS, reason = "Expected warning message is emitted by protobuf decoder")
    public void shouldClearDatabaseWarnings() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor();

        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.execute(INSERT_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.POLL_INTERVAL_MS, "10")
                .build();

        start(PostgresConnector.class, config);
        waitForSnapshotToBeCompleted();
        Awaitility.await().atMost(Duration.ofSeconds(TestHelper.waitTimeForRecords() * 6))
                .until(() -> logInterceptor.containsMessage("Server-side message: 'Exiting startup callback'"));
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
        Awaitility.await("Wait until publication is created").atMost(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS)
                .until(TestHelper::publicationExists);

        // Stop connector, drop publication
        stopConnector();
        TestHelper.dropPublication();

        // Create log interceptor and restart the connector, should observe publication gets re-created
        final LogInterceptor interceptor = new LogInterceptor();
        start(PostgresConnector.class, config);
        waitForStreamingRunning();

        // Check that publication was created
        Awaitility.await("Wait until publication is created").atMost(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS)
                .until(TestHelper::publicationExists);

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
                .with("column.mask.with.5.chars", "s2.a.bb");
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
    }

    @Test
    @FixFor("DBZ-1692")
    public void shouldConsumeEventsWithMaskedHashedColumns() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT +
                "CREATE TABLE s2.b (pk SERIAL, bb varchar(255), PRIMARY KEY(pk));");
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with("column.mask.hash.SHA-256.with.salt.CzQMA0cB5K", "s2.a.bb, s2.b.bb");
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
            assertThat(value.getStruct("after").getString("bb")).isNull();
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
            assertThat(value.getStruct("after").getString("bb")).isEqualTo("8e68c68edbbac316dfe2");
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
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getString("bb")).isEqualTo("b4d39ab0d198fb4cac8b");
        }

        // insert and verify inserts
        TestHelper.execute("INSERT INTO s2.b (bb) VALUES ('hello');");

        actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.b"));
        assertThat(recordsForTopicS2.size()).isEqualTo(1);

        record = recordsForTopicS2.remove(0);
        VerifyRecord.isValidInsert(record, PK_FIELD, 1);

        value = (Struct) record.value();
        if (value.getStruct("before") != null) {
            assertThat(value.getStruct("before").getString("bb")).isNull();
        }
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getString("bb")).isEqualTo("b4d39ab0d198fb4cac8b2f023da74f670bcaf192dcc79b5d6361b7ae6b2fafdf");
        }
    }

    @Test
    @FixFor("DBZ-1972")
    public void shouldConsumeEventsWithTruncatedColumns() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with("column.truncate.to.3.chars", "s2.a.bb");
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        assertThat(actualRecords.allRecordsInOrder().size()).isEqualTo(2);

        List<SourceRecord> recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(1);

        SourceRecord record = recordsForTopicS2.remove(0);
        VerifyRecord.isValidRead(record, PK_FIELD, 1);

        // insert and verify inserts
        TestHelper.execute("INSERT INTO s2.a (aa,bb) VALUES (1, 'test');");

        actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(1);

        record = recordsForTopicS2.remove(0);
        VerifyRecord.isValidInsert(record, PK_FIELD, 2);

        Struct value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getString("bb")).isEqualTo("tes");
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
        if (value.getStruct("before") != null && value.getStruct("before").getString("bb") != null) {
            assertThat(value.getStruct("before").getString("bb")).isEqualTo("tes");
        }
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getString("bb")).isEqualTo("hel");
        }
    }

    @Test
    @FixFor("DBZ-1292")
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void shouldOutputRecordsInCloudEventsFormat() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE);

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        SourceRecords snapshotRecords = consumeRecordsByTopic(2);
        List<SourceRecord> snapshot = snapshotRecords.allRecordsInOrder();

        for (SourceRecord record : snapshot) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(record, "postgresql", "test_server", false);
        }

        // insert some more records and test streaming
        waitForStreamingRunning();
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
    }

    @Test
    @FixFor("DBZ-1813")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Publication configuration only valid for PGOUTPUT decoder")
    public void shouldConfigureSubscriptionsForAllTablesByDefault() throws Exception {
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

        stopConnector(value -> assertTrue(
                logInterceptor.containsMessage("Creating Publication with statement 'CREATE PUBLICATION cdc FOR ALL TABLES;'") &&
                        logInterceptor.containsMessage("Creating new publication 'cdc' for plugin 'PGOUTPUT'")));
        assertTrue(TestHelper.publicationExists("cdc"));
    }

    @Test
    @FixFor("DBZ-1813")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Publication configuration only valid for PGOUTPUT decoder")
    public void shouldConfigureSubscriptionsFromTableFilters() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor();

        TestHelper.dropAllSchemas();
        TestHelper.dropPublication("cdc");
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute(SETUP_TABLES_STMT);

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.PUBLICATION_NAME, "cdc")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.numeric_table,public.text_table,s1.a,s2.a")
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, PostgresConnectorConfig.AutoCreateMode.FILTERED.getValue());

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        // check the records from the snapshot
        assertRecordsFromSnapshot(2, 1, 1);

        // insert 2 new records
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 2, 2);

        stopConnector(value -> {
            assertTrue(logInterceptor.containsMessage(
                    "Creating Publication with statement 'CREATE PUBLICATION cdc FOR TABLE \"public\".\"numeric_table\", \"public\".\"text_table\", \"s1\".\"a\", \"s2\".\"a\";'"));
            assertTrue(logInterceptor.containsMessage("Creating new publication 'cdc' for plugin 'PGOUTPUT'"));
        });

        assertTrue(TestHelper.publicationExists("cdc"));
    }

    @Test
    @FixFor("DBZ-1813")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Publication configuration only valid for PGOUTPUT decoder")
    public void shouldThrowWhenAutocreationIsDisabled() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.dropPublication("cdc");

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SLOT_NAME, "cdc")
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, PostgresConnectorConfig.AutoCreateMode.DISABLED.getValue());

        DebeziumEngine.CompletionCallback cb = (boolean success, String message, Throwable error) -> {
            assertEquals(error.getClass(), ConnectException.class);
            assertEquals(error.getMessage(), "Publication autocreation is disabled, please create one and restart the connector.");
        };

        start(PostgresConnector.class, configBuilder.build(), cb);
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);
        stopConnector();

        assertFalse(TestHelper.publicationExists("cdc"));
    }

    @Test
    @FixFor("DBZ-1813")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Publication configuration only valid for PGOUTPUT decoder")
    public void shouldProduceMessagesOnlyForConfiguredTables() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.dropPublication("cdc");
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute(SETUP_TABLES_STMT);

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.PUBLICATION_NAME, "cdc")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s2.a")
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, PostgresConnectorConfig.AutoCreateMode.FILTERED.getValue());

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        // snapshot record
        consumeRecordsByTopic(1);

        TestHelper.execute(INSERT_STMT);
        SourceRecords actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics()).hasSize(1);

        // there should be no record for s1.a
        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs).isNull();
        assertThat(s2recs).hasSize(1);

        VerifyRecord.isValidInsert(s2recs.get(0), PK_FIELD, 2);
    }

    @Test
    @FixFor("DBZ-2885")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Publication configuration only valid for PGOUTPUT decoder")
    public void shouldThrowWhenTableFiltersIsEmpty() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor();

        TestHelper.dropAllSchemas();
        TestHelper.dropPublication("cdc");
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute(SETUP_TABLES_STMT);

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.PUBLICATION_NAME, "cdc")
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, PostgresConnectorConfig.AutoCreateMode.FILTERED.getValue())
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "nonexistent.table");

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorNotRunning();
        assertTrue(logInterceptor.containsStacktraceElement("No table filters found for filtered publication cdc"));
    }

    @Test
    public void shouldEmitNoEventsForSkippedCreateOperations() throws Exception {
        Testing.Print.enable();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SKIPPED_OPERATIONS, Envelope.Operation.UPDATE.code())
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
        assertNoRecordsToConsume();

        // insert record and update it
        TestHelper.execute("INSERT into s1.a VALUES(201, 1);");
        TestHelper.execute("UPDATE s1.a SET aa=201 WHERE pk=201");
        TestHelper.execute("INSERT into s1.a VALUES(202, 2)");
        TestHelper.execute("UPDATE s1.a SET aa=202 WHERE pk=202");
        TestHelper.execute("INSERT into s1.a VALUES(203, 3)");
        TestHelper.execute("UPDATE s1.a SET aa=203 WHERE pk=203");

        SourceRecords records = consumeRecordsByTopic(3);
        List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("s1.a"));

        assertThat(recordsForTopic.size()).isEqualTo(3);
        assertInsert(recordsForTopic.get(0), PK_FIELD, 201);
        assertInsert(recordsForTopic.get(1), PK_FIELD, 202);
        assertInsert(recordsForTopic.get(2), PK_FIELD, 203);

        recordsForTopic.forEach(record -> {
            Struct value = (Struct) record.value();
            String op = value.getString("op");
            assertNotEquals(op, Envelope.Operation.UPDATE.code());
        });

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

    private List<Long> getSequence(SourceRecord record) {
        assertTrue(record.value() instanceof Struct);
        Struct source = ((Struct) record.value()).getStruct("source");
        String stringSequence = source.getString("sequence");
        ObjectMapper mapper = new ObjectMapper();
        try {
            // Sequence values are Strings, but they are all Longs for
            // Postgres sources.
            return Arrays.asList(mapper.readValue(stringSequence, Long[].class));
        }
        catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Test
    @FixFor("DBZ-2911")
    public void shouldHaveLastCommitLsn() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(CommonConnectorConfig.SOURCE_STRUCT_MAKER_VERSION, Version.V2)
                .with(PostgresConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .build());
        assertConnectorIsRunning();

        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);
        assertNoRecordsToConsume();

        final int n_inserts = 3;
        for (int i = 0; i < n_inserts; ++i) {
            TestHelper.execute(INSERT_STMT);
        }

        List<SourceRecord> records = new ArrayList<>();
        Awaitility.await("Skip empty transactions and find the data").atMost(Duration.ofSeconds(TestHelper.waitTimeForRecords() * 3)).until(() -> {
            int n_transactions = 0;
            while (n_transactions < n_inserts) {
                final List<SourceRecord> candidate = consumeRecordsByTopic(2).allRecordsInOrder();
                if (candidate.get(1).topic().contains("transaction")) {
                    // empty transaction, should be skipped
                    continue;
                }
                records.addAll(candidate);
                records.addAll(consumeRecordsByTopic(2).allRecordsInOrder());
                ++n_transactions;
            }
            return true;
        });

        assertEquals(4 * n_inserts, records.size());
        List<Long> second_transaction_sequence = getSequence(records.get(5));
        assertEquals(second_transaction_sequence.size(), 2);
        assertEquals(second_transaction_sequence.get(0), getSequence(records.get(6)).get(0));

        List<Long> third_transaction_sequence = getSequence(records.get(9));
        assertEquals(third_transaction_sequence.size(), 2);
        assertEquals(third_transaction_sequence.get(0), getSequence(records.get(10)).get(0));

        // Assert the lsn of the second transaction is less than the third.
        assertTrue(second_transaction_sequence.get(1) < third_transaction_sequence.get(1));

        // Assert that the sequences of different records in the same transaction differ
        // (Fix for DBZ-3801)
        if (DecoderDifferences.singleLsnPerTransaction()) {
            assertEquals(getSequence(records.get(5)), getSequence(records.get(6)));
        }
        else {
            assertNotEquals(getSequence(records.get(5)), getSequence(records.get(6)));
        }
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

    private <T> void validateConfigField(Config config, Field field, T expectedValue) {
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

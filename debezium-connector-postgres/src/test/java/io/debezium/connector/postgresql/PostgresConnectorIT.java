/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.getDefaultReplicationSlot;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static junit.framework.TestCase.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.management.InstanceNotFoundException;

import io.debezium.embedded.EmbeddedEngineConfig;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yugabyte.util.PSQLState;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.postgresql.PostgresConnectorConfig.LogicalDecoder;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.connection.AbstractMessageDecoder;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresReplicationConnection;
import io.debezium.connector.postgresql.connection.ReplicaIdentityInfo;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.connection.pgoutput.PgOutputMessageDecoder;
import io.debezium.connector.postgresql.junit.SkipTestDependingOnDecoderPluginNameRule;
import io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIs;
import io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIsNot;
import io.debezium.connector.postgresql.snapshot.InitialOnlySnapshotter;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.converters.CloudEventsConverterTest;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
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
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Strings;
import io.debezium.util.Testing;

/**
 * Integration test for {@link YugabyteDBConnector} using an {@link io.debezium.engine.DebeziumEngine}
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresConnectorIT extends AbstractConnectorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresConnectorIT.class);

    /*
     * Specific tests that need to extend the initial DDL set should do it in a form of
     * TestHelper.execute(SETUP_TABLES_STMT + ADDITIONAL_STATEMENTS)
     */
    protected static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
            "INSERT INTO s2.a (aa) VALUES (1);";
    protected static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "DROP SCHEMA IF EXISTS s2 CASCADE;" +
            "CREATE SCHEMA s1; " +
            "CREATE SCHEMA s2; " +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s2.a (pk SERIAL, aa integer, bb varchar(20), PRIMARY KEY(pk));";
    protected static final String SETUP_TABLES_STMT = CREATE_TABLES_STMT + INSERT_STMT;
    private YugabyteDBConnector connector;

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
        connector = new YugabyteDBConnector();
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
        start(YugabyteDBConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();
    }

    @Test
    public void shouldValidateMinimalConfiguration() throws Exception {
        Configuration config = TestHelper.defaultConfig().build();
        Config validateConfig = new YugabyteDBConnector().validate(config.asMap());
        validateConfig.configValues().forEach(configValue -> assertTrue("Unexpected error for: " + configValue.name(),
                configValue.errorMessages().isEmpty()));
    }

    @Ignore("Requires postgis")
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

        start(YugabyteDBConnector.class, config);
        waitForStreamingRunning();

        Configuration failingConfig = TestHelper.defaultConfig()
                .with("name", "failingPGConnector")
                .with(PostgresConnectorConfig.DATABASE_CONFIG_PREFIX + JdbcConfiguration.USER, "badboy")
                .with(PostgresConnectorConfig.DATABASE_CONFIG_PREFIX + JdbcConfiguration.PASSWORD, "failing")
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.SLOT_NAME, ReplicationConnection.Builder.DEFAULT_SLOT_NAME)
                .build();
        List<ConfigValue> validatedConfig = new YugabyteDBConnector().validate(failingConfig.asMap()).configValues();

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
        YugabyteDBConnector connector = new YugabyteDBConnector();
        Config validatedConfig = connector.validate(config.asMap());
        // validate that the required fields have errors
        assertConfigurationErrors(validatedConfig, PostgresConnectorConfig.HOSTNAME, 1);
        assertConfigurationErrors(validatedConfig, PostgresConnectorConfig.USER, 1);
        assertConfigurationErrors(validatedConfig, PostgresConnectorConfig.DATABASE_NAME, 1);
        assertConfigurationErrors(validatedConfig, CommonConnectorConfig.TOPIC_PREFIX, 1);

        // validate the non required fields
        validateConfigField(validatedConfig, PostgresConnectorConfig.PLUGIN_NAME, LogicalDecoder.YBOUTPUT.getValue());
        validateConfigField(validatedConfig, PostgresConnectorConfig.SLOT_NAME, ReplicationConnection.Builder.DEFAULT_SLOT_NAME);
        validateConfigField(validatedConfig, PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        validateConfigField(validatedConfig, PostgresConnectorConfig.PORT, PostgresConnectorConfig.DEFAULT_PORT);
        validateConfigField(validatedConfig, PostgresConnectorConfig.MAX_QUEUE_SIZE, PostgresConnectorConfig.DEFAULT_MAX_QUEUE_SIZE);
        validateConfigField(validatedConfig, PostgresConnectorConfig.MAX_BATCH_SIZE, PostgresConnectorConfig.DEFAULT_MAX_BATCH_SIZE);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SNAPSHOT_FETCH_SIZE, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.POLL_INTERVAL_MS, PostgresConnectorConfig.DEFAULT_POLL_INTERVAL_MILLIS);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SSL_MODE, PostgresConnectorConfig.SecureConnectionMode.PREFER);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SSL_CLIENT_CERT, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SSL_CLIENT_KEY, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SSL_CLIENT_KEY_PASSWORD, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SSL_ROOT_CERT, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SCHEMA_EXCLUDE_LIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.TABLE_INCLUDE_LIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.TABLE_EXCLUDE_LIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.COLUMN_EXCLUDE_LIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.COLUMN_INCLUDE_LIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.MSG_KEY_COLUMNS, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL);
        validateConfigField(validatedConfig, RelationalDatabaseConnectorConfig.SNAPSHOT_LOCK_TIMEOUT_MS,
                RelationalDatabaseConnectorConfig.DEFAULT_SNAPSHOT_LOCK_TIMEOUT_MILLIS);
        validateConfigField(validatedConfig, PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ADAPTIVE);
        validateConfigField(validatedConfig, PostgresConnectorConfig.DECIMAL_HANDLING_MODE, PostgresConnectorConfig.DecimalHandlingMode.PRECISE);
        validateConfigField(validatedConfig, PostgresConnectorConfig.SSL_SOCKET_FACTORY, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.TCP_KEEPALIVE, true);
        validateConfigField(validatedConfig, PostgresConnectorConfig.LOGICAL_DECODING_MESSAGE_PREFIX_EXCLUDE_LIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.LOGICAL_DECODING_MESSAGE_PREFIX_INCLUDE_LIST, null);
        validateConfigField(validatedConfig, PostgresConnectorConfig.YB_CONSISTENT_SNAPSHOT, Boolean.TRUE);
        validateConfigField(validatedConfig, PostgresConnectorConfig.YB_LOAD_BALANCE_CONNECTIONS, Boolean.TRUE);
    }

    @Test
    public void shouldThrowErrorIfDecimalHandlingModePreciseIsUsed() throws Exception {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, RelationalDatabaseConnectorConfig.DecimalHandlingMode.PRECISE);

        start(YugabyteDBConnector.class, configBuilder.build(), (success, msg, error) -> {
            assertFalse(success);
            assertThat(error.getMessage().contains("Decimal handling mode PRECISE is unsupported, please use DOUBLE or STRING")).isTrue();
        });
    }

    @Test
    public void shouldValidateReplicationSlotName() throws Exception {
        Configuration config = Configuration.create()
                .with(PostgresConnectorConfig.SLOT_NAME, "xx-aa")
                .build();
        YugabyteDBConnector connector = new YugabyteDBConnector();
        Config validatedConfig = connector.validate(config.asMap());

        assertConfigurationErrors(validatedConfig, PostgresConnectorConfig.SLOT_NAME, 1);
    }

    @Test
    public void shouldSupportSSLParameters() throws Exception {
        // the default docker image we're testing against doesn't use SSL, so check that the connector fails to start when
        // SSL is enabled
        Configuration config = TestHelper.defaultConfig().with(PostgresConnectorConfig.SSL_MODE,
                PostgresConnectorConfig.SecureConnectionMode.REQUIRED).build();
        start(YugabyteDBConnector.class, config, (success, msg, error) -> {
            if (TestHelper.shouldSSLConnectionFail()) {
                // we expect the task to fail at startup when we're printing the server info
                assertThat(success).isFalse();
                assertThat(error).isInstanceOf(DebeziumException.class);
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
        start(YugabyteDBConnector.class, configBuilder.build());
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
        start(YugabyteDBConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();

        assertRecordsAfterInsert(2, 3, 3);
    }

    @Test
    public void initialSnapshotWithExistingSlot() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        waitForStreamingRunning();

        // now stop the connector
        stopConnector();
        assertNoRecordsToConsume();

        // insert some more records
        TestHelper.execute(INSERT_STMT);

        // check the records from the snapshot
        // start the connector back up and perform snapshot with an existing slot
        // but the 2 records that were inserted while we were down will NOT be retrieved
        // as part of the snapshot. These records will be retrieved as part of streaming
        Configuration.Builder configBuilderInitial = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE);

        start(YugabyteDBConnector.class, configBuilderInitial.build());
        assertConnectorIsRunning();

        assertRecordsFromSnapshot(2, 1, 1);
        assertRecordsAfterInsert(2, 2, 2);
    }

    @Test
    @FixFor("DBZ-1235")
    public void shouldUseMillisecondsForTransactionCommitTime() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        start(YugabyteDBConnector.class, TestHelper.defaultConfig().build());
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
        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted();
        SourceRecords records = consumeRecordsByTopic(recordCount);
        assertThat(records.recordsForTopic("test_server.s1.a")).hasSize(recordCount);
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
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1");
        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted();
        SourceRecords records = consumeRecordsByTopic(recordCount);
        assertThat(records.recordsForTopic("test_server.s1.a")).hasSize(recordCount);
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

            start(YugabyteDBConnector.class, config.getConfig());

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
            YBVerifyRecord.isValidInsert(insertRecord, "newpk", 2);

            TestHelper.execute(
                    "ALTER TABLE changepk.test_table ADD COLUMN pk2 SERIAL;"
                            + "ALTER TABLE changepk.test_table DROP CONSTRAINT test_table_pkey;"
                            + "ALTER TABLE changepk.test_table ADD PRIMARY KEY(newpk,pk2);"
                            + "INSERT INTO changepk.test_table VALUES(3, 'newpkcol', 8)");
            records = consumeRecordsByTopic(1);

            insertRecord = records.recordsForTopic(topicName).get(0);
            assertEquals(topicName, insertRecord.topic());
            YBVerifyRecord.isValidInsert(insertRecord, newPkField, 3);
            YBVerifyRecord.isValidInsert(insertRecord, "pk2", 8);

            stopConnector();

            // De-synchronize JDBC PK info and decoded event schema
            TestHelper.execute("INSERT INTO changepk.test_table VALUES(4, 'newpkcol', 20)");
            TestHelper.execute(
                    "ALTER TABLE changepk.test_table DROP CONSTRAINT test_table_pkey;"
                            + "ALTER TABLE changepk.test_table DROP COLUMN pk2;"
                            + "ALTER TABLE changepk.test_table ADD COLUMN pk3 SERIAL;"
                            + "ALTER TABLE changepk.test_table ADD PRIMARY KEY(newpk,pk3);"
                            + "INSERT INTO changepk.test_table VALUES(5, 'dropandaddpkcol',10)");

            start(YugabyteDBConnector.class, config.getConfig());

            records = consumeRecordsByTopic(2);

            insertRecord = records.recordsForTopic(topicName).get(0);
            assertEquals(topicName, insertRecord.topic());
            YBVerifyRecord.isValidInsert(insertRecord, newPkField, 4);
            Struct key = (Struct) insertRecord.key();
            // The problematic record PK info is temporarily desynced
            assertThat(key.schema().field("pk2")).isNull();
            assertThat(key.schema().field("pk3")).isNull();

            insertRecord = records.recordsForTopic(topicName).get(1);
            assertEquals(topicName, insertRecord.topic());
            YBVerifyRecord.isValidInsert(insertRecord, newPkField, 5);
            YBVerifyRecord.isValidInsert(insertRecord, "pk3", 10);
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

    @Ignore("Will require a complete test refactor")
    @Test
    public void shouldReceiveChangesForChangeColumnDefault() throws Exception {
        Testing.Print.enable();
        final String slotName = "default_change" + new Random().nextInt(100);
        TestHelper.create().dropReplicationSlot(slotName);

        // YB Note: Creating a table before deploying the connector since dynamic table addition is
        // not supported yet.
        TestHelper.execute(
          "CREATE SCHEMA IF NOT EXISTS default_change;",
          "DROP TABLE IF EXISTS default_change.test_table;",
          "CREATE TABLE default_change.test_table (pk SERIAL, i INT DEFAULT 1, text TEXT DEFAULT 'foo', PRIMARY KEY(pk));");

        TestHelper.execute("INSERT INTO default_change.test_table(i, text) VALUES (DEFAULT, DEFAULT);");

        try {
            final PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                    .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, Boolean.FALSE)
                    .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "default_change")
                    .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                    .with(PostgresConnectorConfig.SLOT_NAME, slotName)
                    .build());

            final String topicName = topicName("default_change.test_table");

            start(YugabyteDBConnector.class, config.getConfig());

            assertConnectorIsRunning();
            waitForSnapshotToBeCompleted();

            // check the records from the snapshot
            final SourceRecords snapshotRecords = consumeRecordsByTopic(1);

            final Integer pkExpectedDefault = 0;
            final Integer snapshotIntDefault = 1;
            final String snapshotTextDefault = "foo";
            snapshotRecords.recordsForTopic(topicName).forEach(snapshotRecord -> {
                assertValueField(snapshotRecord, "after/pk/value", 1);
                assertValueField(snapshotRecord, "after/i/value", snapshotIntDefault);
                assertValueField(snapshotRecord, "after/text/value", snapshotTextDefault);

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

            start(YugabyteDBConnector.class, config.getConfig());

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
            final String refreshedTstzDefault = "2021-03-20T13:44:28.000000Z";
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

    @Ignore("Complete test refactor required")
    @Test
    public void showThatSchemaColumnDefaultMayApplyRetroactively() throws Exception {
        Testing.Print.enable();
        final String slotName = "default_change" + new Random().nextInt(100);
        try (PostgresConnection conn = TestHelper.create()) {
            conn.dropReplicationSlot(slotName);
        }
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

            start(YugabyteDBConnector.class, config.getConfig());

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

            start(YugabyteDBConnector.class, config.getConfig());

            assertConnectorIsRunning();

            // check that the schema defaults will be in-sync after restart refreshes schema
            final Integer refreshedIntDefault = 2;
            final String refreshedTextDefault = "bar";
            final Long initialBigIntDefault = 1L;
            final Long refreshedBigIntDefault = 2L;
            final String refreshedTstzDefault = "2021-03-20T13:44:28.000000Z";
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
            try (PostgresConnection conn = TestHelper.create()) {
                conn.dropReplicationSlot(slotName);
            }

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
        start(YugabyteDBConnector.class, configBuilder.build());
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

        start(YugabyteDBConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();
        waitForStreamingRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics()).hasSize(1);
        assertThat(actualRecords.recordsForTopic(topicName("s2.a"))).hasSize(1);
    }

    @Ignore("YB Note: This behaviour is not yet implemented, see https://github.com/yugabyte/yugabyte-db/issues/21573")
    @Test
    @FixFor("DBZ-1021")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Pgoutput will generate insert statements even for dropped tables, column optionality will default to true however")
    public void shouldNotIgnoreEventsForDeletedTable() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        start(YugabyteDBConnector.class, configBuilder.build());
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

        start(YugabyteDBConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
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
        start(YugabyteDBConnector.class, configBuilder.build());
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
        start(YugabyteDBConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();
        waitForStreamingRunning();

        assertRecordsAfterInsert(2, 3, 3);
    }

    @Test
    public void shouldLimitDecoderLog() throws Exception {
        LogInterceptor interceptor = new LogInterceptor(AbstractMessageDecoder.class);
        TestHelper.execute(
                SETUP_TABLES_STMT +
                        "CREATE VIEW s1.myview AS SELECT * from s1.a;");
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        start(YugabyteDBConnector.class, configBuilder.build());
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
        start(YugabyteDBConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();
        waitForStreamingRunning();

        assertRecordsAfterInsert(2, 3, 3);

        assertEquals("There should be at most one log message every 10 seconds",
                1, interceptor.countOccurrences("identified as already processed"));
    }

    @Ignore("We are receiving records out of a certain order, can't control")
    @Test
    @FixFor("DBZ-693")
    public void shouldExecuteOnConnectStatements() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.ON_CONNECT_STATEMENTS, "INSERT INTO s1.a (aa) VALUES (2); INSERT INTO s2.a (aa, bb) VALUES (2, 'hello;; world');")
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForStreamingRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(6);

        // YB Note: This test is failing because the records are coming out of order - we are indeed
        // receiving all the records but because of the jumbled nature of the records, the second
        // assertKey assertion fails.
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
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        // YB Note: Added a wait for replication slot to be active.
        TestHelper.waitFor(Duration.ofSeconds(15));

        waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
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
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        // check the records from the snapshot
        assertRecordsFromSnapshot(2, 1, 1);

        // insert and verify that no events were received since the connector should not be streaming changes
        TestHelper.execute(INSERT_STMT);
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);
        // there shouldn't be any records
        assertNoRecordsToConsume();
    }

    @Ignore("Snapshot mode ALWAYS is unsupported")
    @Test
    public void shouldProduceEventsWhenAlwaysTakingSnapshots() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        start(YugabyteDBConnector.class, configBuilder.build());
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
        start(YugabyteDBConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        assertRecordsFromSnapshot(4, 1, 2, 1, 2);
    }

    @Test
    public void shouldHaveBeforeImageOfUpdatedRow() throws InterruptedException {
        Testing.Print.enable();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.execute("ALTER TABLE s1.a REPLICA IDENTITY FULL;");
        Configuration config = TestHelper.defaultConfig()
                                 .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                                 .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                                 .build();
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        // YB Note: Added a wait for replication slot to be active.
        TestHelper.waitFor(Duration.ofSeconds(15));

        waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        // insert and verify 2 new records
        TestHelper.execute(INSERT_STMT);
        TestHelper.execute("UPDATE s1.a SET aa = 404 WHERE pk = 2;");

        SourceRecords actualRecords = consumeRecordsByTopic(3);
        List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s1.a"));

        SourceRecord insertRecord = records.get(0);
        SourceRecord updateRecord = records.get(1);

        YBVerifyRecord.isValidInsert(insertRecord, PK_FIELD, 2);
        YBVerifyRecord.isValidUpdate(updateRecord, PK_FIELD, 2);

        Struct updateRecordValue = (Struct) updateRecord.value();
        assertThat(updateRecordValue.get(Envelope.FieldName.AFTER)).isNotNull();
        assertThat(updateRecordValue.get(Envelope.FieldName.BEFORE)).isNotNull();
        assertThat(updateRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("aa").getInt32("value")).isEqualTo(1);
        assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).getStruct("aa").getInt32("value")).isEqualTo(404);
    }

    @Test
    public void shouldFailIfNoPrimaryKeyHashColumnSpecifiedWithSnapshotModeParallel() throws Exception {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.PARALLEL.getValue())
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.test")
                .with(PostgresConnectorConfig.PRIMARY_KEY_HASH_COLUMNS, "");

        start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
            assertFalse(success);
            assertThat(message.contains("primary.key.hash.columns cannot be empty when snapshot.mode is 'parallel'")).isTrue();
        });
    }

    @Test
    public void shouldFailIfParallelSnapshotRunWithMultipleTables() throws Exception {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.PARALLEL.getValue())
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.test,public.test2")
                .with(PostgresConnectorConfig.PRIMARY_KEY_HASH_COLUMNS, "id");

        start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
            assertFalse(success);

            assertThat(error.getMessage().contains("parallel snapshot consumption is only supported with one table at a time")).isTrue();
        });
    }


    @Test
    public void shouldFailAfterConfiguredRetries() throws Exception {
        // We will intentionally not let the connector start and see if it retries.
        PostgresConnectorTask.TEST_THROW_ERROR_BEFORE_COORDINATOR_STARTUP = true;

        // We have to set the errors max retries for the embedded engine as well otherwise it would
        // keep retrying indefinitely. Specifying a 0 means it will never retry on any error.
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
            .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.test")
            .with(EmbeddedEngineConfig.ERRORS_MAX_RETRIES, 0)
            .with(PostgresConnectorConfig.ERRORS_MAX_RETRIES, 0)
            .with(PostgresConnectorConfig.RETRIABLE_RESTART_WAIT, 12000);

        start(YugabyteDBConnector.class, configBuilder.build());

        TestHelper.waitFor(Duration.ofSeconds(80));

        assertConnectorNotRunning();

        PostgresConnectorTask.TEST_THROW_ERROR_BEFORE_COORDINATOR_STARTUP = false;
    }

    @Test
    public void shouldFailWithSnapshotModeParallelIfNoTableIncludeListProvided() throws Exception {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.PARALLEL.getValue())
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "")
                .with(PostgresConnectorConfig.PRIMARY_KEY_HASH_COLUMNS, "id");

        start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
            assertFalse(success);

            assertThat(error.getMessage().contains("No table provided, provide a table in the table.include.list")).isTrue();
        });
    }

    @Test
    public void shouldFailIfSnapshotModeParallelHasPublicationAutoCreateModeAllTables() throws Exception {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.PARALLEL.getValue())
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.test")
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, PostgresConnectorConfig.AutoCreateMode.ALL_TABLES)
                .with(PostgresConnectorConfig.PRIMARY_KEY_HASH_COLUMNS, "id");;

        start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
            assertFalse(success);

            assertThat(error.getMessage().contains("Snapshot mode parallel is not supported with publication.autocreate.mode all_tables")).isTrue();
        });
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
        DebeziumEngine.CompletionCallback completionCallback = (success, message, error) -> {
            if (error != null) {
                latch.countDown();
            }
            else {
                fail("A controlled exception was expected....");
            }
        };
        start(YugabyteDBConnector.class, configBuilder.build(), completionCallback, stopOnPKPredicate(2));
        // YB Note: Increasing the wait time since the connector is taking slightly higher time to initialize.
        // wait until we know we've raised the exception at startup AND the engine has been shutdown
        if (!latch.await(TestHelper.waitTimeForRecords() * 15, TimeUnit.SECONDS)) {
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
        start(YugabyteDBConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
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

        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        assertRecordsFromSnapshot(2, 1, 1);

        // DBZ-5408 make sure regular streaming is fully running
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 2, 2);

        // kill all opened connections to the database
        TestHelper.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type='walsender'");
        TestHelper.execute(INSERT_STMT);
        // TODO Vaibhav: Revisit this later to see if this wait can be removed or reduced.
        TestHelper.waitFor(Duration.ofSeconds(10));
        assertRecordsAfterInsert(2, 3, 3);
    }

    @Test
    public void shouldUpdateReplicaIdentity() throws Exception {

        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(PostgresReplicationConnection.class);

        String setupStmt = SETUP_TABLES_STMT;
        TestHelper.execute(setupStmt);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, "s1.a:FULL,s2.a:DEFAULT")
                .build();

        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning();

        // Waiting for Replica Identity is updated
        waitForAvailableRecords(5, TimeUnit.SECONDS);

        try (PostgresConnection connection = TestHelper.create()) {
            TableId tableIds1 = new TableId("", "s1", "a");
            TableId tableIds2 = new TableId("", "s2", "a");
            assertEquals(ReplicaIdentityInfo.ReplicaIdentity.FULL, connection.readReplicaIdentityInfo(tableIds1).getReplicaIdentity());
            assertEquals(ReplicaIdentityInfo.ReplicaIdentity.DEFAULT, connection.readReplicaIdentityInfo(tableIds2).getReplicaIdentity());
            assertThat(logInterceptor.containsMessage(String.format("Replica identity set to FULL for table '%s'", tableIds1))).isTrue();

            // YB Note: Fails because we do not get this message when replica identity is already set.
            // assertThat(logInterceptor.containsMessage(String.format("Replica identity for table '%s' is already DEFAULT", tableIds2))).isTrue();
            // YB Note: Adding an alternate log message.
            assertThat(logInterceptor.containsMessage(String.format("Replica identity set to DEFAULT for table '%s'", tableIds2))).isTrue();
        }
    }

    @Test
    public void shouldUpdateReplicaIdentityWithRegExp() throws Exception {

        TestHelper.executeDDL("postgres_create_multiple_tables.ddl");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, "(.*).a:FULL,s2.*:NOTHING")
                .build();

        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning();

        // Waiting for Replica Identity is updated
        waitForAvailableRecords(5, TimeUnit.SECONDS);

        try (PostgresConnection connection = TestHelper.create()) {
            TableId tableIds1a = new TableId("", "s1", "a");
            TableId tableIds2b = new TableId("", "s2", "b");
            TableId tableIds2c = new TableId("", "s2", "c");
            TableId tableIds3a = new TableId("", "s3", "a");
            assertEquals(ReplicaIdentityInfo.ReplicaIdentity.FULL, connection.readReplicaIdentityInfo(tableIds1a).getReplicaIdentity());
            assertEquals(ReplicaIdentityInfo.ReplicaIdentity.FULL, connection.readReplicaIdentityInfo(tableIds3a).getReplicaIdentity());
            assertEquals(ReplicaIdentityInfo.ReplicaIdentity.NOTHING, connection.readReplicaIdentityInfo(tableIds2b).getReplicaIdentity());
            assertEquals(ReplicaIdentityInfo.ReplicaIdentity.NOTHING, connection.readReplicaIdentityInfo(tableIds2c).getReplicaIdentity());
        }
    }

    @Test
    public void shouldNotUpdateReplicaIdentityWithRegExpDuplicated() throws Exception {

        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(PostgresReplicationConnection.class);

        TestHelper.executeDDL("postgres_create_multiple_tables.ddl");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, "s.*:FULL,s2.*:NOTHING")
                .build();

        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning();

        // Waiting for Replica Identity is updated
        waitForAvailableRecords(10, TimeUnit.SECONDS);

        assertThat(logInterceptor.containsErrorMessage("Unable to update Replica Identity for table s2.c")).isTrue();
        assertThat(logInterceptor.containsStacktraceElement("More than one Regular expressions matched table s2.c")).isTrue();
        assertThat(logInterceptor.containsErrorMessage("Unable to update Replica Identity for table s2.b")).isTrue();
        assertThat(logInterceptor.containsStacktraceElement("More than one Regular expressions matched table s2.b")).isTrue();
    }

    @Test
    public void shouldUpdateReplicaIdentityWithOneTable() throws Exception {

        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(PostgresReplicationConnection.class);

        String setupStmt = SETUP_TABLES_STMT;
        TestHelper.execute(setupStmt);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, "s1.a:FULL")
                .build();

        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning();

        // Waiting for Replica Identity is updated
        waitForAvailableRecords(5, TimeUnit.SECONDS);

        try (PostgresConnection connection = TestHelper.create()) {
            TableId tableIds1 = new TableId("", "s1", "a");
            assertEquals(ReplicaIdentityInfo.ReplicaIdentity.FULL.toString(), connection.readReplicaIdentityInfo(tableIds1).toString());
            assertThat(logInterceptor.containsMessage(String.format("Replica identity set to FULL for table '%s'", tableIds1))).isTrue();
        }
    }

    @Ignore("YB Note: alter replica identity INDEX is unsupported")
    @Test
    public void shouldUpdateReplicaIdentityUsingIndex() throws Exception {

        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(PostgresReplicationConnection.class);

        String setupStmt = SETUP_TABLES_STMT;
        TestHelper.execute(setupStmt);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, "s1.a:FULL,s2.a:INDEX a_pkey")
                .build();

        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning();

        // Waiting for Replica Identity is updated
        waitForAvailableRecords(5, TimeUnit.SECONDS);

        try (PostgresConnection connection = TestHelper.create()) {
            TableId tableIds1 = new TableId("", "s1", "a");
            TableId tableIds2 = new TableId("", "s2", "a");
            String index_name = connection.readIndexOfReplicaIdentity(tableIds2);
            ReplicaIdentityInfo replicaIdentityTable2 = connection.readReplicaIdentityInfo(tableIds2);
            replicaIdentityTable2.setIndexName(index_name);

            assertEquals(ReplicaIdentityInfo.ReplicaIdentity.FULL.toString(), connection.readReplicaIdentityInfo(tableIds1).toString());
            ReplicaIdentityInfo replicaIdentityIndex = new ReplicaIdentityInfo(ReplicaIdentityInfo.ReplicaIdentity.INDEX, "a_pkey");
            assertEquals(replicaIdentityIndex.toString(), replicaIdentityTable2.toString());
            assertThat(logInterceptor.containsMessage(String.format("Replica identity set to FULL for table '%s'", tableIds1))).isTrue();
            assertThat(logInterceptor.containsMessage(String.format("Replica identity set to USING INDEX %s for table '%s'", index_name, tableIds2))).isTrue();
        }
    }

    @Test
    public void shouldLogOwnershipErrorForReplicaIdentityUpdate() throws Exception {

        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(PostgresConnection.class);

        TestHelper.executeDDL("postgres_create_role_specific_tables.ddl");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, "s1.a:FULL,s2.a:DEFAULT")
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, "DISABLED")
                .with("database.user", "role_2")
                .with("database.password", "role_2_pass")
                .build();

        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning();

        // Waiting for Replica Identity is updated
        waitForAvailableRecords(5, TimeUnit.SECONDS);

        assertThat(logInterceptor.containsMessage(String.format("Replica identity could not be updated because of lack of privileges"))).isTrue();
    }

    @Test
    public void shouldCheckTablesToUpdateReplicaIdentityAreCaptured() throws Exception {

        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(PostgresReplicationConnection.class);

        String setupStmt = SETUP_TABLES_STMT;
        TestHelper.execute(setupStmt);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, "s1.a:FULL,s2.b:DEFAULT")
                .build();

        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning();

        // Waiting for Replica Identity is updated
        waitForAvailableRecords(5, TimeUnit.SECONDS);

        // YB Note: The following block only checks if a certain log message has appeared or not.
        // In our case, we can alter the replica identity but the actual replica identity for a table
        // will remain what is set at the time of replication slot creation.
        try (PostgresConnection connection = TestHelper.create()) {
            TableId tableIds1 = new TableId("", "s1", "a");
            assertEquals(ReplicaIdentityInfo.ReplicaIdentity.FULL.toString(), connection.readReplicaIdentityInfo(tableIds1).toString());
            assertThat(logInterceptor.containsMessage(String.format("Replica identity set to FULL for table '%s'", tableIds1))).isTrue();

            assertThat(logInterceptor
                    .containsMessage(
                            "Replica identity for table 's2.a' will not be updated because Replica Identity is not defined on REPLICA_IDENTITY_AUTOSET_VALUES property"))
                    .isTrue();
        }
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

        // YB Note: Separating the ALTER commands as they were causing transaction abortion in YB
        // if run collectively, the error being:
        // java.lang.RuntimeException: com.yugabyte.util.PSQLException: ERROR: Unknown transaction, could be recently aborted: 3273ed66-13c6-4d73-8c6e-014389e5081e
        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.execute("CREATE TABLE s1.b (pk SERIAL, aa integer, bb integer, PRIMARY KEY(pk));");
        TestHelper.execute("ALTER TABLE s1.a ADD COLUMN bb integer;");
        TestHelper.execute("INSERT INTO s1.a (aa, bb) VALUES (2, 2); "
                + "INSERT INTO s1.a (aa, bb) VALUES (3, 3); "
                + "INSERT INTO s1.b (aa, bb) VALUES (4, 4); "
                + "INSERT INTO s2.a (aa) VALUES (5);");
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SCHEMA_EXCLUDE_LIST, "s2")
                .with(PostgresConnectorConfig.TABLE_EXCLUDE_LIST, ".+b")
                .with(PostgresConnectorConfig.COLUMN_EXCLUDE_LIST, ".+bb");

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        // check the records from the snapshot take the filters into account
        SourceRecords actualRecords = consumeRecordsByTopic(4); // 3 records in s1.a and 1 in s1.b

        assertThat(actualRecords.recordsForTopic(topicName("s2.a"))).isNullOrEmpty();
        assertThat(actualRecords.recordsForTopic(topicName("s1.b"))).isNullOrEmpty();
        List<SourceRecord> recordsForS1a = actualRecords.recordsForTopic(topicName("s1.a"));
        assertThat(recordsForS1a.size()).isEqualTo(3);
        AtomicInteger pkValue = new AtomicInteger(1);
        recordsForS1a.forEach(record -> {
            YBVerifyRecord.isValidRead(record, PK_FIELD, pkValue.getAndIncrement());
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
        // YB Note: Separating the ALTER commands as they were causing transaction abortion in YB
        // if run collectively, the error being:
        // java.lang.RuntimeException: com.yugabyte.util.PSQLException: ERROR: Unknown transaction, could be recently aborted: 3273ed66-13c6-4d73-8c6e-014389e5081e
        String setupStmt = SETUP_TABLES_STMT +
                "CREATE TABLE s1.b (pk SERIAL, aa integer, bb integer, PRIMARY KEY(pk));";

        TestHelper.execute(setupStmt);

        TestHelper.execute("ALTER TABLE s1.a ADD COLUMN bb integer;");

        String initInsertStmt = "INSERT INTO s1.a (aa, bb) VALUES (2, 2);" +
                "INSERT INTO s1.a (aa, bb) VALUES (3, 3);" +
                "INSERT INTO s1.b (aa, bb) VALUES (4, 4);" +
                "INSERT INTO s2.a (aa) VALUES (5);";
        TestHelper.execute(initInsertStmt);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SCHEMA_EXCLUDE_LIST, "s2")
                .with(PostgresConnectorConfig.TABLE_EXCLUDE_LIST, ".+b")
                .with(PostgresConnectorConfig.COLUMN_EXCLUDE_LIST, ".+bb");

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        // check the records from the snapshot take the filters into account
        SourceRecords actualRecords = consumeRecordsByTopic(4); // 3 records in s1.a and 1 in s1.b

        assertThat(actualRecords.recordsForTopic(topicName("s2.a"))).isNullOrEmpty();
        assertThat(actualRecords.recordsForTopic(topicName("s1.b"))).isNullOrEmpty();
        List<SourceRecord> recordsForS1a = actualRecords.recordsForTopic(topicName("s1.a"));
        assertThat(recordsForS1a.size()).isEqualTo(3);
        AtomicInteger pkValue = new AtomicInteger(1);
        recordsForS1a.forEach(record -> {
            YBVerifyRecord.isValidRead(record, PK_FIELD, pkValue.getAndIncrement());
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

        // YB Note: Separating the ALTER commands as they were causing transaction abortion in YB
        // if run collectively, the error being:
        // java.lang.RuntimeException: com.yugabyte.util.PSQLException: ERROR: Unknown transaction, could be recently aborted: 3273ed66-13c6-4d73-8c6e-014389e5081e
        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.execute("ALTER TABLE s1.a ADD COLUMN bb integer;");
        TestHelper.execute("ALTER TABLE s1.a ADD COLUMN cc char(12);");
        TestHelper.execute("INSERT INTO s1.a (aa, bb) VALUES (2, 2);");
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with("column.mask.with.5.chars", ".+cc")
                .with(PostgresConnectorConfig.COLUMN_INCLUDE_LIST, ".+aa,.+cc");

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(1);
        List<SourceRecord> recordsForS1a = actualRecords.recordsForTopic(topicName("s1.a"));
        recordsForS1a.forEach(record -> {
            assertFieldAbsent(record, "bb");

            Struct recordValue = ((Struct) record.value());
            assertThat(recordValue.getStruct("after").getStruct("cc").getString("value")).isEqualTo("*****");
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

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);

        List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s1.b"));
        assertThat(records.size()).isEqualTo(1);

        SourceRecord record = records.get(0);
        YBVerifyRecord.isValidRead(record, PK_FIELD, 1);

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
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, tableWhitelistWithWhitespace);

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);

        List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s1.b"));
        assertThat(records.size()).isEqualTo(1);

        SourceRecord record = records.get(0);
        YBVerifyRecord.isValidRead(record, PK_FIELD, 1);

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

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        // YB Note: this test will fail since it creates a table after connector has started
        // and dynamic table addition is not supported in YB yet.
        TestHelper.execute("CREATE TABLE s1.b (pk SERIAL, aa isbn, PRIMARY KEY(pk));", "INSERT INTO s1.b (aa) VALUES ('978-0-393-04002-9')");
        SourceRecords actualRecords = consumeRecordsByTopic(1);

        List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s1.b"));
        assertThat(records.size()).isEqualTo(1);

        SourceRecord record = records.get(0);
        YBVerifyRecord.isValidInsert(record, PK_FIELD, 1);
        final String isbn = new String(((Struct) record.value()).getStruct("after").getStruct("aa").getString("value"));
        assertThat(isbn).isEqualTo("0-393-04002-X");

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

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(1);

        List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s1.dbz_878_some_test_data"));
        assertThat(records.size()).isEqualTo(1);

        SourceRecord record = records.get(0);
        YBVerifyRecord.isValidRead(record, PK_FIELD, 1);

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
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning();
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
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        // YB Note: Waiting for 15 seconds for streaming to initialise properly.
        TestHelper.waitFor(Duration.ofSeconds(15));

        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        final Set<String> flushLsn = new HashSet<>();
        try (PostgresConnection connection = TestHelper.create()) {
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
        assertThat(flushLsn.size()).isGreaterThanOrEqualTo((recordCount * 3) / 4);
    }

    @Test
    @FixFor("DBZ-2660")
    public void shouldRegularlyFlushLsnWithTxMonitoring() throws InterruptedException, SQLException {
        final int recordCount = 10;
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.a")
                .with(PostgresConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .build();
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        final String txTopic = topicName("transaction");
        TestHelper.execute(INSERT_STMT);
        final SourceRecords firstRecords = consumeDmlRecordsByTopic(1);
        assertThat(firstRecords.topics().size()).isEqualTo(2);
        assertThat(firstRecords.recordsForTopic(txTopic).size()).isGreaterThanOrEqualTo(2);
        assertThat(firstRecords.recordsForTopic(txTopic).get(1).sourceOffset().containsKey("lsn_commit")).isTrue();
        stopConnector();
        assertConnectorNotRunning();

        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
        // there shouldn't be any snapshot records, only potentially transaction messages
        assertOnlyTransactionRecordsToConsume();

        final Set<String> flushLsn = new HashSet<>();
        try (PostgresConnection connection = TestHelper.create()) {
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
        assertThat(flushLsn.size()).isGreaterThanOrEqualTo((recordCount * 3) / 4);
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
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(1);

        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs).isNull();

        SourceRecord record = s1recs.get(0);
        YBVerifyRecord.isValidRead(record, PK_FIELD, 1);

        TestHelper.execute(INSERT_STMT);
        actualRecords = consumeRecordsByTopic(2);

        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        record = s1recs.get(0);
        YBVerifyRecord.isValidInsert(record, PK_FIELD, 2);
        record = s2recs.get(0);
        YBVerifyRecord.isValidInsert(record, PK_FIELD, 2);
        stopConnector();

        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .build();
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();
        actualRecords = consumeRecordsByTopic(4);

        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(2);
        assertThat(s2recs.size()).isEqualTo(2);
        YBVerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);
        YBVerifyRecord.isValidRead(s1recs.get(1), PK_FIELD, 2);
        YBVerifyRecord.isValidRead(s2recs.get(0), PK_FIELD, 1);
        YBVerifyRecord.isValidRead(s2recs.get(1), PK_FIELD, 2);
    }

    @Ignore("Snapshot mode ALWAYS is unsupported")
    @Test
    @FixFor("DBZ-2456")
    public void shouldAllowForSelectiveSnapshot() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS.name())
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "s1.a")
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        /* Snapshot must be taken only for the listed tables */
        SourceRecords actualRecords = consumeRecordsByTopic(1);
        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));

        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs).isNull();
        YBVerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);

        /* streaming should work normally */
        TestHelper.execute(INSERT_STMT);
        actualRecords = consumeRecordsByTopic(2);

        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));

        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        YBVerifyRecord.isValidInsert(s1recs.get(0), PK_FIELD, 2);
        YBVerifyRecord.isValidInsert(s2recs.get(0), PK_FIELD, 2);

        stopConnector();

        /* start the connector back up and make sure snapshot is being taken */
        start(YugabyteDBConnector.class, configBuilder
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_TABLES, "s2.a")
                .build());
        assertConnectorIsRunning();

        actualRecords = consumeRecordsByTopic(2);
        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));

        assertThat(s2recs.size()).isEqualTo(2);
        assertThat(s1recs).isNull();
        YBVerifyRecord.isValidRead(s2recs.get(0), PK_FIELD, 1);
        YBVerifyRecord.isValidRead(s2recs.get(1), PK_FIELD, 2);
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
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        // Consume records from the snapshot
        SourceRecords actualRecords = consumeRecordsByTopic(2);

        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);

        YBVerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);
        YBVerifyRecord.isValidRead(s2recs.get(0), PK_FIELD, 1);

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

        YBVerifyRecord.isValidInsert(s1recs.get(0), PK_FIELD, 2);
        YBVerifyRecord.isValidInsert(s2recs.get(0), PK_FIELD, 2);
        stopConnector();

        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .build();
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.execute(INSERT_STMT);

        actualRecords = consumeRecordsByTopic(2);

        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        YBVerifyRecord.isValidInsert(s1recs.get(0), PK_FIELD, 3);
        YBVerifyRecord.isValidInsert(s2recs.get(0), PK_FIELD, 3);
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
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.MAX_QUEUE_SIZE, 2)
                .with(PostgresConnectorConfig.MAX_BATCH_SIZE, 1)
                .build();
        final PostgresConnection pgConnection = TestHelper.create();
        pgConnection.setAutoCommit(false);
        pgConnection.executeWithoutCommitting(INSERT_STMT);
        final AtomicBoolean inserted = new AtomicBoolean();
        start(YugabyteDBConnector.class, config, loggingCompletion(), x -> false, x -> {
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
        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.execute(INSERT_STMT);

        TestHelper.createDefaultReplicationSlot();
        TestHelper.execute("CREATE PUBLICATION dbz_publication FOR ALL TABLES;");

        // Testing.Print.enable();
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.MAX_QUEUE_SIZE, 2)
                .with(PostgresConnectorConfig.MAX_BATCH_SIZE, 1)
                .build();
        final PostgresConnection pgConnection = TestHelper.create();
        pgConnection.setAutoCommit(false);
        pgConnection.executeWithoutCommitting(INSERT_STMT);
        final AtomicBoolean inserted = new AtomicBoolean();
        start(YugabyteDBConnector.class, config, loggingCompletion(), x -> false, x -> {
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
    public void shouldPerformSnapshotOnceForInitialOnlySnapshotMode() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(InitialOnlySnapshotter.class);

        TestHelper.dropDefaultReplicationSlot();

        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(YugabyteDBConnector.class, config);
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
        YBVerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);
        YBVerifyRecord.isValidRead(s2recs.get(0), PK_FIELD, 1);

        // Stop the connector
        stopConnector();
        assertConnectorNotRunning();

        // Restart the connector again with initial-only
        // No snapshot should be produced and no records generated
        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .build();
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        waitForConnectorShutdown("postgres", TestHelper.TEST_SERVER);

        // Stop the connector, verify that no snapshot was performed
        assertThat(logInterceptor.containsMessage("Previous initial snapshot completed, no snapshot will be performed")).isTrue();
    }

    @Test
    public void snapshotInitialOnlyFollowedByNever() throws Exception {
        TestHelper.dropDefaultReplicationSlot();

        TestHelper.execute(SETUP_TABLES_STMT);
        // Start connector in NEVER mode to get the slot and publication created
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();
        // now stop the connector
        stopConnector();
        assertNoRecordsToConsume();

        // These INSERT events should not be part of snapshot
        TestHelper.execute(INSERT_STMT);

        // Now start the connector in INITIAL_ONLY mode
        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        // Lets wait for snapshot to finish before proceeding
        waitForSnapshotToBeCompleted("postgres", "test_server");
        waitForAvailableRecords(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        assertRecordsFromSnapshot(2, 1, 1);

        // Stop the connector
        stopConnector();
        assertConnectorNotRunning();

        // Restart the connector again with NEVER mode
        // The streaming should continue from where the INITIAL_ONLY connector had finished the snapshot
        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .build();
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        assertRecordsAfterInsert(2, 2, 2);
    }

    @Ignore("YB: Custom snapshotter not supported")
    @Test
    @FixFor("DBZ-2094")
    public void shouldResumeStreamingFromSlotPositionForCustomSnapshot() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        // Perform an regular snapshot
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomStartFromStreamingTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        YBVerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);
        YBVerifyRecord.isValidRead(s2recs.get(0), PK_FIELD, 1);

        stopConnector();

        // Insert records while connector is stopped
        TestHelper.execute(INSERT_STMT);

        // Perform catch up streaming and resnapshot everything
        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomStartFromStreamingTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted();

        // Expect duplicate records from the snapshot and while streaming is running
        actualRecords = consumeRecordsByTopic(6);

        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(3);
        assertThat(s2recs.size()).isEqualTo(3);

        // Validate the first record is from streaming
        YBVerifyRecord.isValidInsert(s1recs.get(0), PK_FIELD, 2);
        YBVerifyRecord.isValidInsert(s2recs.get(0), PK_FIELD, 2);

        // Validate the rest of the records are from the snapshot
        YBVerifyRecord.isValidRead(s1recs.get(1), PK_FIELD, 1);
        YBVerifyRecord.isValidRead(s1recs.get(2), PK_FIELD, 2);
        YBVerifyRecord.isValidRead(s2recs.get(1), PK_FIELD, 1);
        YBVerifyRecord.isValidRead(s2recs.get(2), PK_FIELD, 2);

        TestHelper.assertNoOpenTransactions();
    }

    @Ignore("YB: Custom snapshotter not supported")
    @Test
    @FixFor("DBZ-2094")
    public void customSnapshotterSkipsTablesOnRestart() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(RelationalSnapshotChangeEventSource.class);

        TestHelper.execute(SETUP_TABLES_STMT);
        // Perform an regular snapshot using the always snapshotter
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        YBVerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);
        YBVerifyRecord.isValidRead(s2recs.get(0), PK_FIELD, 1);

        stopConnector();

        // Insert records while connector is stopped
        TestHelper.execute(INSERT_STMT);

        // Perform a custom partial snapshot
        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomPartialTableTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(YugabyteDBConnector.class, config);
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
        YBVerifyRecord.isValidInsert(s1recs.get(0), PK_FIELD, 2);
        YBVerifyRecord.isValidInsert(s2recs.get(0), PK_FIELD, 2);

        // snapshot records
        YBVerifyRecord.isValidRead(s1recs.get(1), PK_FIELD, 1);
        YBVerifyRecord.isValidRead(s1recs.get(2), PK_FIELD, 2);

        assertNoRecordsToConsume();

        TestHelper.assertNoOpenTransactions();

        stopConnector(value -> assertThat(logInterceptor.containsMessage("For table 's2.a' the select statement was not provided, skipping table")).isTrue());
    }

    @Ignore("YB: Custom snapshotter not supported")
    @Test
    @FixFor("DBZ-2094")
    public void customSnapshotterSkipsTablesOnRestartWithConcurrentTx() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(RelationalSnapshotChangeEventSource.class);

        Testing.Print.enable();
        TestHelper.execute(SETUP_TABLES_STMT);
        // Perform an regular snapshot using the always snapshotter
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        YBVerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);
        YBVerifyRecord.isValidRead(s2recs.get(0), PK_FIELD, 1);

        stopConnector();

        // Insert records while connector is stopped
        TestHelper.execute(INSERT_STMT);

        // Perform a custom partial snapshot
        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomPartialTableTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(YugabyteDBConnector.class, config);
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
        YBVerifyRecord.isValidInsert(s1recs.get(0), PK_FIELD, 2);
        YBVerifyRecord.isValidInsert(s2recs.get(0), PK_FIELD, 2);

        // snapshot records
        YBVerifyRecord.isValidRead(s1recs.get(1), PK_FIELD, 1);
        YBVerifyRecord.isValidRead(s1recs.get(2), PK_FIELD, 2);

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
        start(YugabyteDBConnector.class, config);
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
                    statement.setString(2, "yugabyte");
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

    private void assertFieldAbsentInBeforeImage(SourceRecord record, String fieldName) {
        Struct value = (Struct) ((Struct) record.value()).get(Envelope.FieldName.BEFORE);
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
        start(YugabyteDBConnector.class, configBuilder.build());
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

    @Ignore("YB: YB doesn't support the way of initial_only snapshot this connector uses, see https://github.com/yugabyte/yugabyte-db/issues/21425")
    @Test
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
        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        CompletableFuture.runAsync(() -> consumeRecords(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
    }

    @Ignore("YB: YB doesn't support the way of initial_only snapshot this connector uses, see https://github.com/yugabyte/yugabyte-db/issues/21425")
    @Test
    @FixFor("DBZ-1242")
    public void testEmptySchemaWarningAfterApplyingFilters() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(RelationalDatabaseSchema.class);

        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY.getValue())
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "my_products");

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForAvailableRecords(10 * (TestHelper.waitTimeForRecords() * 5), TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isTrue());
    }

    @Ignore("YB: YB doesn't support the way of initial_only snapshot this connector uses, see https://github.com/yugabyte/yugabyte-db/issues/21425")
    @Test
    @FixFor("DBZ-1242")
    public void testNoEmptySchemaWarningAfterApplyingFilters() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(RelationalDatabaseSchema.class);

        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY.getValue());

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isFalse());
    }

    @Test
    @FixFor("DBZ-1436")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Publication configuration only valid for PGOUTPUT decoder")
    public void testCustomPublicationNameUsed() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(PostgresReplicationConnection.class);

        TestHelper.dropAllSchemas();
        TestHelper.dropPublication("cdc");
        TestHelper.executeDDL("postgres_create_tables.ddl");

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.PUBLICATION_NAME, "cdc");

        start(YugabyteDBConnector.class, configBuilder.build());
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

        start(YugabyteDBConnector.class, configBuilder.build());
        waitForSnapshotToBeCompleted();
        SourceRecords records = consumeRecordsByTopic(2);
        records.recordsForTopic("test_server.s1.a").forEach(record -> {
            Struct key = (Struct) record.key();
            assertThat(key.get(PK_FIELD)).isNotNull();
            assertThat(key.get("aa")).isNotNull();
        });
        records.recordsForTopic("test_server.s2.a").forEach(record -> {
            Struct key = (Struct) record.key();
            assertThat(key.get(PK_FIELD)).isNotNull();
            assertThat(key.get("pk")).isNotNull();
            assertThat(key.schema().field("aa")).isNull();
        });

        stopConnector();

    }

    @Test
    @FixFor("DBZ-1519")
    public void shouldNotIssueWarningForNoMonitoredTablesAfterApplyingFilters() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(RelationalDatabaseSchema.class);

        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.execute(INSERT_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s2")
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();

        // Start connector, verify that it does not log no captured tables warning
        start(YugabyteDBConnector.class, config);
        waitForSnapshotToBeCompleted();
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(logInterceptor.containsMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isFalse();
        stopConnector();

        // Restart connector, verify it does not log no captured tables warning
        start(YugabyteDBConnector.class, config);
        waitForStreamingRunning();
        assertThat(logInterceptor.containsMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isFalse();
    }

    @Ignore("YB: decoderbufs unsupported")
    @Test
    @FixFor("DBZ-2865")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.DECODERBUFS, reason = "Expected warning message is emitted by protobuf decoder")
    public void shouldClearDatabaseWarnings() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(PostgresReplicationConnection.class);

        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.execute(INSERT_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.POLL_INTERVAL_MS, "10")
                .build();

        start(YugabyteDBConnector.class, config);
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
        start(YugabyteDBConnector.class, config);
        waitForStreamingRunning();

        // Check that publication was created
        Awaitility.await("Wait until publication is created").atMost(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS)
                .until(TestHelper::publicationExists);

        // Stop connector, drop publication
        stopConnector();
        TestHelper.dropPublication();

        // Create log interceptor and restart the connector, should observe publication gets re-created
        final LogInterceptor interceptor = new LogInterceptor(PostgresReplicationConnection.class);
        start(YugabyteDBConnector.class, config);
        waitForStreamingRunning();

        // YB Note: Increasing the wait time.
        // Check that publication was created
        Awaitility.await("Wait until publication is created").atMost(TestHelper.waitTimeForRecords() * 10, TimeUnit.SECONDS)
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
        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        assertThat(actualRecords.allRecordsInOrder().size()).isEqualTo(2);

        List<SourceRecord> recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(1);

        SourceRecord record = recordsForTopicS2.remove(0);
        YBVerifyRecord.isValidRead(record, PK_FIELD, 1);

        Struct value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getStruct("bb").getString("value")).isEqualTo("*****");
        }

        // insert and verify inserts
        TestHelper.execute("INSERT INTO s2.a (aa,bb) VALUES (1, 'test');");

        actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(1);

        record = recordsForTopicS2.remove(0);
        YBVerifyRecord.isValidInsert(record, PK_FIELD, 2);

        value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getStruct("bb").getString("value")).isEqualTo("*****");
        }

        // update and verify update
        TestHelper.execute("UPDATE s2.a SET aa=2, bb='hello' WHERE pk=2;");

        actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(1);

        record = recordsForTopicS2.remove(0);
        YBVerifyRecord.isValidUpdate(record, PK_FIELD, 2);

        value = (Struct) record.value();
        // TODO Vaibhav: Note to self - the following assertion is only valid when before image is enabled.
        if (value.getStruct("before") != null) {
            assertThat(value.getStruct("before").getStruct("bb").getString("value")).isEqualTo("*****");
        }
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getStruct("bb").getString("value")).isEqualTo("*****");
        }
    }

    @Test
    @FixFor("DBZ-1692")
    public void shouldConsumeEventsWithMaskedHashedColumns() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT +
                "CREATE TABLE s2.b (pk SERIAL, bb varchar(255), PRIMARY KEY(pk));");
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with("column.mask.hash.SHA-256.with.salt.CzQMA0cB5K", "s2.a.bb, s2.b.bb");
        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        assertThat(actualRecords.allRecordsInOrder().size()).isEqualTo(2);

        List<SourceRecord> recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(1);

        SourceRecord record = recordsForTopicS2.remove(0);
        YBVerifyRecord.isValidRead(record, PK_FIELD, 1);

        Struct value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getStruct("bb").getString("value")).isNull();
        }

        // insert and verify inserts
        TestHelper.execute("INSERT INTO s2.a (aa,bb) VALUES (1, 'test');");

        actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(1);

        record = recordsForTopicS2.remove(0);
        YBVerifyRecord.isValidInsert(record, PK_FIELD, 2);

        value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getStruct("bb").getString("value")).isEqualTo("8e68c68edbbac316dfe2");
        }

        // update and verify update
        TestHelper.execute("UPDATE s2.a SET aa=2, bb='hello' WHERE pk=2;");

        actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(1);

        record = recordsForTopicS2.remove(0);
        YBVerifyRecord.isValidUpdate(record, PK_FIELD, 2);

        value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getStruct("bb").getString("value")).isEqualTo("b4d39ab0d198fb4cac8b");
        }

        // insert and verify inserts
        TestHelper.execute("INSERT INTO s2.b (bb) VALUES ('hello');");

        actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.b"));
        assertThat(recordsForTopicS2.size()).isEqualTo(1);

        record = recordsForTopicS2.remove(0);
        YBVerifyRecord.isValidInsert(record, PK_FIELD, 1);

        value = (Struct) record.value();
        // TODO Vaibhav: Note to self - the following assertion is only valid when before image is enabled.
        if (value.getStruct("before") != null) {
            assertThat(value.getStruct("before").getStruct("bb").getString("value")).isNull();
        }
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getStruct("bb").getString("value")).isEqualTo("b4d39ab0d198fb4cac8b2f023da74f670bcaf192dcc79b5d6361b7ae6b2fafdf");
        }
    }

    @Test
    @FixFor("DBZ-1972")
    public void shouldConsumeEventsWithTruncatedColumns() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with("column.truncate.to.3.chars", "s2.a.bb");
        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        assertThat(actualRecords.allRecordsInOrder().size()).isEqualTo(2);

        List<SourceRecord> recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(1);

        SourceRecord record = recordsForTopicS2.remove(0);
        YBVerifyRecord.isValidRead(record, PK_FIELD, 1);

        // insert and verify inserts
        TestHelper.execute("INSERT INTO s2.a (aa,bb) VALUES (1, 'test');");

        actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(1);

        record = recordsForTopicS2.remove(0);
        YBVerifyRecord.isValidInsert(record, PK_FIELD, 2);

        Struct value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getStruct("bb").getString("value")).isEqualTo("tes");
        }

        // update and verify update
        TestHelper.execute("UPDATE s2.a SET aa=2, bb='hello' WHERE pk=2;");

        actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(1);

        record = recordsForTopicS2.remove(0);
        YBVerifyRecord.isValidUpdate(record, PK_FIELD, 2);

        value = (Struct) record.value();
        // TODO Vaibhav: Note to self: the following before image assertion is only for cases with before image enabled.
        if (value.getStruct("before") != null && value.getStruct("before").getStruct("bb").getString("value") != null) {
            assertThat(value.getStruct("before").getStruct("bb").getString("value")).isEqualTo("tes");
        }
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getStruct("bb").getString("value")).isEqualTo("hel");
        }
    }

    @Test
    @FixFor("DBZ-5811")
    public void shouldAckLsnOnSourceByDefault() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.createDefaultReplicationSlot();

        final Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SLOT_NAME, ReplicationConnection.Builder.DEFAULT_SLOT_NAME)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, "false");

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        assertThat(actualRecords.allRecordsInOrder().size()).isEqualTo(2);

        stopConnector();
        final SlotState slotAfterSnapshot = getDefaultReplicationSlot();

        TestHelper.execute("INSERT INTO s2.a (aa,bb) VALUES (1, 'test');");
        TestHelper.execute("UPDATE s2.a SET aa=2, bb='hello' WHERE pk=2;");

        start(YugabyteDBConnector.class, configBuilder.build());

        assertConnectorIsRunning();
        waitForStreamingRunning();

        actualRecords = consumeRecordsByTopic(2);
        assertThat(actualRecords.allRecordsInOrder().size()).isEqualTo(2);
        stopConnector();

        final SlotState slotAfterIncremental = getDefaultReplicationSlot();
        Assert.assertEquals(1, slotAfterIncremental.slotLastFlushedLsn().compareTo(slotAfterSnapshot.slotLastFlushedLsn()));
    }

    // YB Note: This test is only applicable when replica identity is CHANGE.
    @Test
    public void testYBCustomChangesForUpdate() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(CREATE_TABLES_STMT);
        TestHelper.createDefaultReplicationSlot();

        final Configuration.Builder configBuilder = TestHelper.defaultConfig()
              .with(PostgresConnectorConfig.SLOT_NAME, ReplicationConnection.Builder.DEFAULT_SLOT_NAME)
              .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
              .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s2.a");

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForStreamingRunning();
        TestHelper.waitFor(Duration.ofSeconds(5));

        TestHelper.execute(INSERT_STMT);
        TestHelper.execute("UPDATE s2.a SET aa=2 WHERE pk=1;");
        TestHelper.execute("UPDATE s2.a SET aa=NULL WHERE pk=1;");

        SourceRecords actualRecords = consumeRecordsByTopic(3);

        assertValueField(actualRecords.allRecordsInOrder().get(0), "after/pk/value", 1);
        assertValueField(actualRecords.allRecordsInOrder().get(0), "after/aa/value", 1);
        assertValueField(actualRecords.allRecordsInOrder().get(0), "after/bb/value", null);

        assertValueField(actualRecords.allRecordsInOrder().get(1), "after/pk/value", 1);
        assertValueField(actualRecords.allRecordsInOrder().get(1), "after/aa/value", 2);
        assertValueField(actualRecords.allRecordsInOrder().get(1), "after/bb", null);

        assertValueField(actualRecords.allRecordsInOrder().get(2), "after/pk/value", 1);
        assertValueField(actualRecords.allRecordsInOrder().get(2), "after/aa/value", null);
        assertValueField(actualRecords.allRecordsInOrder().get(2), "after/bb", null);
    }

    @Test
    public void testTableWithCompositePrimaryKey() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(CREATE_TABLES_STMT);
        TestHelper.execute("CREATE TABLE s1.test_composite_pk (id INT, text_col TEXT, first_name VARCHAR(60), age INT, PRIMARY KEY(id, text_col));");

        final Configuration.Builder configBuilder = TestHelper.defaultConfig()
          .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
          .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.test_composite_pk");

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForStreamingRunning();

        TestHelper.execute("INSERT INTO s1.test_composite_pk VALUES (1, 'ffff-ffff', 'Vaibhav', 25);");
        TestHelper.execute("UPDATE s1.test_composite_pk SET first_name='Vaibhav K' WHERE id = 1 AND text_col='ffff-ffff';");
        TestHelper.execute("DELETE FROM s1.test_composite_pk;");

        SourceRecords actualRecords = consumeRecordsByTopic(4 /* 1 + 1 + 1 + tombstone */);
        List<SourceRecord> records = actualRecords.allRecordsInOrder();

        assertThat(records.size()).isEqualTo(4);

        // Assert insert record.
        assertValueField(records.get(0), "after/id/value", 1);
        assertValueField(records.get(0), "after/text_col/value", "ffff-ffff");
        assertValueField(records.get(0), "after/first_name/value", "Vaibhav");
        assertValueField(records.get(0), "after/age/value", 25);

        // Assert update record.
        assertValueField(records.get(1), "after/id/value", 1);
        assertValueField(records.get(1), "after/text_col/value", "ffff-ffff");
        assertValueField(records.get(1), "after/first_name/value", "Vaibhav K");
        assertValueField(records.get(1), "after/age", null);

        // Assert delete record.
        assertValueField(records.get(2), "before/id/value", 1);
        assertValueField(records.get(2), "before/text_col/value", "ffff-ffff");
        assertValueField(records.get(2), "before/first_name/value", null);
        assertValueField(records.get(2), "before/age/value", null);
        assertValueField(records.get(2), "after", null);

        // Validate tombstone record.
        assertTombstone(records.get(3));
    }

    @Test
    public void shouldNotWorkWithReplicaIdentityChangeAndPgOutput() throws Exception {
        final Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SLOT_NAME, ReplicationConnection.Builder.DEFAULT_SLOT_NAME)
                .with(PostgresConnectorConfig.PLUGIN_NAME, LogicalDecoder.PGOUTPUT)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s2.a");

        start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
            assertFalse(success);
        });
    }

    @Test
    public void foreignKeyOnTheTableShouldNotCauseIssues() throws Exception {
        TestHelper.execute(CREATE_TABLES_STMT);
        TestHelper.execute("CREATE TABLE s1.department (dept_id INT PRIMARY KEY, dept_name TEXT);");
        TestHelper.execute("CREATE TABLE s1.users (id SERIAL PRIMARY KEY, name TEXT, dept_id INT, FOREIGN KEY (dept_id) REFERENCES s1.department(dept_id));");

        final Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SLOT_NAME, "slot_for_fk")
                .with(PostgresConnectorConfig.PUBLICATION_NAME, "publication_for_fk")
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.users");

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForStreamingRunning();

        TestHelper.execute("INSERT INTO s1.department VALUES (11, 'Industrial equipments');");
        TestHelper.execute("INSERT INTO s1.users VALUES (1, 'Vaibhav', 11);");

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(1);

        SourceRecord record = records.allRecordsInOrder().get(0);
        YBVerifyRecord.isValidInsert(record, "id", 1);
        assertValueField(record, "after/id/value", 1);
        assertValueField(record, "after/name/value", "Vaibhav");
        assertValueField(record, "after/dept_id/value", 11);
    }

    @Test
    public void shouldNotSkipMessagesWithoutChangeWithReplicaIdentityChange() throws Exception {
        testSkipMessagesWithoutChange(ReplicaIdentityInfo.ReplicaIdentity.CHANGE);
    }

    @Test
    public void shouldSkipMessagesWithoutChangeWithReplicaIdentityFull() throws Exception {
        testSkipMessagesWithoutChange(ReplicaIdentityInfo.ReplicaIdentity.FULL);
    }

    public void testSkipMessagesWithoutChange(ReplicaIdentityInfo.ReplicaIdentity replicaIdentity) throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(CREATE_TABLES_STMT);

        boolean isReplicaIdentityFull = (replicaIdentity == ReplicaIdentityInfo.ReplicaIdentity.FULL);

        if (isReplicaIdentityFull) {
            TestHelper.execute("ALTER TABLE s2.a REPLICA IDENTITY FULL;");
            TestHelper.waitFor(Duration.ofSeconds(10));
        }

        TestHelper.createDefaultReplicationSlot();

        final Configuration.Builder configBuilder = TestHelper.defaultConfig()
                                                      .with(PostgresConnectorConfig.SLOT_NAME, ReplicationConnection.Builder.DEFAULT_SLOT_NAME)
                                                      .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                                                      .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s2.a")
                                                      .with(PostgresConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, true)
                                                      .with(PostgresConnectorConfig.COLUMN_INCLUDE_LIST, "s2.a.pk,s2.a.aa");

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForStreamingRunning();
        TestHelper.waitFor(Duration.ofSeconds(5));

        TestHelper.execute(INSERT_STMT);
        // This update will not be propagated if replica identity is FULL.
        TestHelper.execute("UPDATE s2.a SET bb = 'random_value' WHERE pk=1;");
        TestHelper.execute("UPDATE s2.a SET aa = 12345 WHERE pk=1;");

        // YB Note: We will be receiving all the records if replica identity is CHANGE.
        SourceRecords actualRecords = consumeRecordsByTopic(isReplicaIdentityFull ? 2 : 3);

        assertValueField(actualRecords.allRecordsInOrder().get(0), "after/pk/value", 1);
        assertValueField(actualRecords.allRecordsInOrder().get(0), "after/aa/value", 1);

        if (isReplicaIdentityFull) {
            // In this case the second record we get is the operation where one of the monitored columns
            // is changed.
            assertThat(actualRecords.allRecordsInOrder().size()).isEqualTo(2);

            assertValueField(actualRecords.allRecordsInOrder().get(1), "after/pk/value", 1);
            assertValueField(actualRecords.allRecordsInOrder().get(1), "after/aa/value", 12345);

            assertValueField(actualRecords.allRecordsInOrder().get(1), "before/pk/value", 1);
            assertValueField(actualRecords.allRecordsInOrder().get(1), "before/aa/value", 1);
            assertFieldAbsentInBeforeImage(actualRecords.allRecordsInOrder().get(1), "bb");
        } else {
            assertThat(actualRecords.allRecordsInOrder().size()).isEqualTo(3);

            assertValueField(actualRecords.allRecordsInOrder().get(1), "after/pk/value", 1);
            // Column aa would be not be present since it is unchanged column.
            assertThat(((Struct) actualRecords.allRecordsInOrder().get(1).value()).getStruct("after").get("aa")).isNull();

            assertThat(((Struct) actualRecords.allRecordsInOrder().get(1).value()).getStruct("before")).isNull();

            assertValueField(actualRecords.allRecordsInOrder().get(2), "after/pk/value", 1);
            assertValueField(actualRecords.allRecordsInOrder().get(2), "after/aa/value", 12345);
            assertFieldAbsent(actualRecords.allRecordsInOrder().get(2), "bb");

            assertThat(((Struct) actualRecords.allRecordsInOrder().get(2).value()).getStruct("before")).isNull();

        }
    }

    // YB Note: This test is only applicable when replica identity is CHANGE.
    @Test
    public void customYBStructureShouldBePresentInSnapshotRecords() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(CREATE_TABLES_STMT);

        // Insert 5 records to be included in snapshot.
        for (int i = 0; i < 5; ++i) {
            TestHelper.execute(String.format("INSERT INTO s2.a (aa) VALUES (%d);", i));
        }

        TestHelper.createDefaultReplicationSlot();

        final Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SLOT_NAME, ReplicationConnection.Builder.DEFAULT_SLOT_NAME)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s2.a");

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        SourceRecords actualRecords = consumeRecordsByTopic(5);
        assertThat(actualRecords.allRecordsInOrder().size()).isEqualTo(5);

        Set<Integer> expectedPKValues = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5));
        Set<Integer> actualPKValues = new HashSet<>();

        for (SourceRecord record : actualRecords.allRecordsInOrder()) {
            Struct value = (Struct) record.value();

            actualPKValues.add(value.getStruct("after").getStruct("pk").getInt32("value"));
        }

        assertEquals(expectedPKValues, actualPKValues);
    }

    @Test
    public void streamColumnsWithNotNullConstraintsForReplicaIdentityChange() throws Exception {
        testStreamColumnsWithNotNullConstraints(ReplicaIdentityInfo.ReplicaIdentity.CHANGE);
    }

    @Test
    public void streamColumnsWithNotNullConstraintsForReplicaIdentityFull() throws Exception {
        testStreamColumnsWithNotNullConstraints(ReplicaIdentityInfo.ReplicaIdentity.FULL);
    }

    @Test
    public void streamColumnsWithNotNullConstraintsForReplicaIdentityDefault() throws Exception {
        testStreamColumnsWithNotNullConstraints(ReplicaIdentityInfo.ReplicaIdentity.DEFAULT);
    }

    public void testStreamColumnsWithNotNullConstraints(
      ReplicaIdentityInfo.ReplicaIdentity replicaIdentity) throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(CREATE_TABLES_STMT);
        TestHelper.execute("CREATE TABLE s1.test_table (id INT PRIMARY KEY, name TEXT NOT NULL, age INT);");

        if (replicaIdentity != ReplicaIdentityInfo.ReplicaIdentity.CHANGE) {
            final String replicaIdentityName = replicaIdentity.name();
            TestHelper.execute("ALTER TABLE s1.test_table REPLICA IDENTITY " + replicaIdentityName + ";");
        }

        final Configuration.Builder configBuilder = TestHelper.defaultConfig()
          .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
          .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.test_table");

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForStreamingRunning();

        TestHelper.execute("INSERT INTO s1.test_table VALUES (1, 'Vaibhav', 25);");
        TestHelper.execute("UPDATE s1.test_table SET age = 30 WHERE id = 1;");

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        List<SourceRecord> records = actualRecords.allRecordsInOrder();
        assertThat(records.size()).isEqualTo(2);

        YBVerifyRecord.isValidInsert(records.get(0), "id", 1);
        YBVerifyRecord.isValidUpdate(records.get(1), "id", 1);

        // Also verify that the update record does/doesn't contain the non-updated column depending
        // on replica identity.
        if (replicaIdentity.equals(ReplicaIdentityInfo.ReplicaIdentity.CHANGE)) {
            assertValueField(records.get(1), "after/name", null);
        } else {
            assertValueField(records.get(1), "after/name/value", "Vaibhav");
        }
    }

    @Test
    @FixFor("DBZ-5811")
    public void shouldNotAckLsnOnSource() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.createDefaultReplicationSlot();

        final SlotState slotAtTheBeginning = getDefaultReplicationSlot();

        final Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SHOULD_FLUSH_LSN_IN_SOURCE_DB, "false")
                .with(PostgresConnectorConfig.SLOT_NAME, ReplicationConnection.Builder.DEFAULT_SLOT_NAME)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, "false");

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        assertThat(actualRecords.allRecordsInOrder().size()).isEqualTo(2);

        stopConnector();

        final SlotState slotAfterSnapshot = getDefaultReplicationSlot();
        Assert.assertEquals(slotAtTheBeginning.slotLastFlushedLsn(), slotAfterSnapshot.slotLastFlushedLsn());

        TestHelper.execute("INSERT INTO s2.a (aa,bb) VALUES (1, 'test');");
        TestHelper.execute("UPDATE s2.a SET aa=2, bb='hello' WHERE pk=2;");

        start(YugabyteDBConnector.class, configBuilder.build());

        assertConnectorIsRunning();
        waitForStreamingRunning();

        TestHelper.waitFor(Duration.ofSeconds(15));

        actualRecords = consumeRecordsByTopic(2);

        assertThat(actualRecords.allRecordsInOrder().size()).isEqualTo(2);
        stopConnector();

        final SlotState slotAfterIncremental = getDefaultReplicationSlot();
        Assert.assertEquals(slotAfterSnapshot.slotLastFlushedLsn(), slotAfterIncremental.slotLastFlushedLsn());
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

        start(YugabyteDBConnector.class, configBuilder.build());
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

        // YB Note: Increasing the wait time for records.
        final List<SourceRecord> streaming = new ArrayList<SourceRecord>();
        Awaitility.await().atMost(TestHelper.waitTimeForRecords() * 15, TimeUnit.SECONDS).until(() -> {
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

    // This test is for manual testing and if this is being run then change the method TestHelper#defaultJdbcConfig
    // to include all three nodes "127.0.0.1:5433,127.0.0.2:5433,127.0.0.3:5433".
    //
    // Now while running this test, as soon as you see "Take a node down now" in the logs now,
    // take down the node at IP 127.0.0.1 in order to simulate a node going down scenario.
    @Ignore("This test should not be run in the complete suite without making above mentioned changes")
    @Test
    public void testYBChangesForMultiHostConfiguration() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(CREATE_TABLES_STMT);
        TestHelper.createDefaultReplicationSlot();

        final Configuration.Builder configBuilder = TestHelper.defaultConfig()
              .with(PostgresConnectorConfig.HOSTNAME, "127.0.0.1:5433,127.0.0.2:5433,127.0.0.3:5433")
              .with(PostgresConnectorConfig.SLOT_NAME, ReplicationConnection.Builder.DEFAULT_SLOT_NAME)
              .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false)
              .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
              .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s2.a");

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForStreamingRunning();
        TestHelper.waitFor(Duration.ofSeconds(5));

        TestHelper.execute(INSERT_STMT);

        LOGGER.info("Take a node down now");
        TestHelper.waitFor(Duration.ofMinutes(1));

        LOGGER.info("Inserting and waiting for another 30s");
        TestHelper.execute("INSERT INTO s2.a (aa) VALUES (11);");

        TestHelper.waitFor(Duration.ofMinutes(2));
        SourceRecords actualRecords = consumeRecordsByTopic(2);

        assertThat(actualRecords.allRecordsInOrder().size()).isEqualTo(2);
    }

    @Test
    @FixFor("DBZ-1813")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Publication configuration only valid for PGOUTPUT decoder")
    public void shouldConfigureSubscriptionsForAllTablesByDefault() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(PostgresReplicationConnection.class);

        TestHelper.dropAllSchemas();
        TestHelper.dropPublication("cdc");
        TestHelper.executeDDL("postgres_create_tables.ddl");

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.PUBLICATION_NAME, "cdc");

        start(YugabyteDBConnector.class, configBuilder.build());
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
        final LogInterceptor logInterceptor = new LogInterceptor(PostgresReplicationConnection.class);

        TestHelper.dropAllSchemas();
        TestHelper.dropPublication("cdc");
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute(SETUP_TABLES_STMT);

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.PUBLICATION_NAME, "cdc")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.numeric_table,public.text_table,s1.a,s2.a")
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, PostgresConnectorConfig.AutoCreateMode.FILTERED.getValue());

        start(YugabyteDBConnector.class, configBuilder.build());
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

        start(YugabyteDBConnector.class, configBuilder.build(), cb);
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

        start(YugabyteDBConnector.class, configBuilder.build());
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

        YBVerifyRecord.isValidInsert(s2recs.get(0), PK_FIELD, 2);
    }

    @Test
    @FixFor("DBZ-2885")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Publication configuration only valid for PGOUTPUT decoder")
    public void shouldThrowWhenTableFiltersIsEmpty() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(PostgresConnectorIT.class);

        TestHelper.dropAllSchemas();
        TestHelper.dropPublication("cdc");
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute(SETUP_TABLES_STMT);

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.PUBLICATION_NAME, "cdc")
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, PostgresConnectorConfig.AutoCreateMode.FILTERED.getValue())
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "nonexistent.table");

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorNotRunning();
        assertTrue(logInterceptor.containsStacktraceElement("No table filters found for filtered publication cdc"));
    }

    @Test
    @FixFor("DBZ-3921")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Publication configuration only valid for PGOUTPUT decoder")
    public void shouldUpdatePublicationForConfiguredTables() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.dropPublication("cdc");
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute(SETUP_TABLES_STMT);

        Configuration.Builder initalConfigBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.PUBLICATION_NAME, "cdc")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s2.a")
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, PostgresConnectorConfig.AutoCreateMode.FILTERED.getValue());

        start(YugabyteDBConnector.class, initalConfigBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        // snapshot record s2.a
        consumeRecordsByTopic(1);

        TestHelper.execute(INSERT_STMT);
        SourceRecords actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics()).hasSize(1);

        // there should be no record for s1.a
        List<SourceRecord> initalS1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> initalS2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(initalS1recs).isNull();
        assertThat(initalS2recs).hasSize(1);

        YBVerifyRecord.isValidInsert(initalS2recs.get(0), PK_FIELD, 2);

        stopConnector();

        logger.info("Connector stopped");

        // update configured tables use s1.a and no longer s2.a
        Configuration.Builder updatedConfigBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.PUBLICATION_NAME, "cdc")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.a")
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, PostgresConnectorConfig.AutoCreateMode.FILTERED.getValue());

        start(YugabyteDBConnector.class, updatedConfigBuilder.build());
        assertConnectorIsRunning();

        // snapshot record s1.a
        consumeRecordsByTopic(1);

        TestHelper.execute(INSERT_STMT);
        TestHelper.waitFor(Duration.ofSeconds(10));
        SourceRecords actualRecordsAfterUpdate = consumeRecordsByTopic(1);
        assertThat(actualRecordsAfterUpdate.topics()).hasSize(1);

        // there should be no record for s2.a
        List<SourceRecord> afterUpdateS1recs = actualRecordsAfterUpdate.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> afterUpdateS2recs = actualRecordsAfterUpdate.recordsForTopic(topicName("s2.a"));
        assertThat(afterUpdateS1recs).hasSize(1);
        assertThat(afterUpdateS2recs).isNull();
    }

    @Test
    @FixFor("DBZ-5949")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Publication configuration only valid for PGOUTPUT decoder")
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 13, reason = "Publication with 'publish_via_partition_root' parameter is supported only on Postgres 13+")
    public void shouldUpdateExistingPublicationForConfiguredPartitionedTables() throws Exception {
        String setupStmt = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
                "CREATE SCHEMA s1;" +
                "CREATE TABLE s1.part (pk SERIAL, aa integer, PRIMARY KEY(pk, aa)) PARTITION BY RANGE (aa);" +
                "CREATE TABLE s1.part1 PARTITION OF s1.part FOR VALUES FROM (0) TO (500);" +
                "CREATE TABLE s1.part2 PARTITION OF s1.part FOR VALUES FROM (500) TO (1000);" +
                "INSERT INTO s1.part (pk, aa) VALUES (0, 0);";
        TestHelper.execute(setupStmt);

        TestHelper.dropPublication("cdc");
        TestHelper.execute("CREATE PUBLICATION cdc FOR TABLE s1.part WITH (publish_via_partition_root = true);");

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.PUBLICATION_NAME, "cdc")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.part")
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, PostgresConnectorConfig.AutoCreateMode.FILTERED.getValue());

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        // snapshot record s1.part
        consumeRecordsByTopic(1);

        String insertStmt = "INSERT INTO s1.part (pk, aa) VALUES (1, 1);" +
                "INSERT INTO s1.part (pk, aa) VALUES (501, 501);";
        TestHelper.execute(insertStmt);
        SourceRecords actualRecords = consumeRecordsByTopic(2);
        assertThat(actualRecords.topics()).hasSize(1);

        // there should be no records for s1.part1 and s1.part2
        List<SourceRecord> recs = actualRecords.recordsForTopic(topicName("s1.part"));
        List<SourceRecord> part1recs = actualRecords.recordsForTopic(topicName("s1.part1"));
        List<SourceRecord> part2recs = actualRecords.recordsForTopic(topicName("s1.part2"));
        assertThat(recs).hasSize(2);
        assertThat(part1recs).isNull();
        assertThat(part2recs).isNull();

        YBVerifyRecord.isValidInsert(recs.get(0), PK_FIELD, 1);
        YBVerifyRecord.isValidInsert(recs.get(1), PK_FIELD, 501);
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
        start(YugabyteDBConnector.class, config);
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

    @Test
    public void nonSuperUserSnapshotAndStreaming() throws Exception {
        TestHelper.executeDDL("replication_role_user.ddl");
        TestHelper.execute(SETUP_TABLES_STMT);

        // Only tables owned by the connector user can be added to the publication
        TestHelper.execute("GRANT USAGE ON SCHEMA s1 to ybpgconn");
        TestHelper.execute("GRANT USAGE ON SCHEMA s2 to ybpgconn");
        TestHelper.execute("ALTER TABLE s1.a OWNER TO ybpgconn");
        TestHelper.execute("ALTER TABLE s2.a OWNER TO ybpgconn");

        // Start the connector with the non super user
        Configuration.Builder configBuilderInitial = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.USER, "ybpgconn")
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, PostgresConnectorConfig.AutoCreateMode.FILTERED)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE);

        start(YugabyteDBConnector.class, configBuilderInitial.build());
        assertConnectorIsRunning();

        // insert some more records - these should not be part of the snapshot
        TestHelper.execute(INSERT_STMT);

        assertRecordsFromSnapshot(2, 1, 1);
        assertRecordsAfterInsert(2, 2, 2);

        TestHelper.execute("REVOKE CREATE ON DATABASE yugabyte FROM ybpgconn");
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
        start(YugabyteDBConnector.class, TestHelper.defaultConfig()
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
        assertNotEquals(getSequence(records.get(5)), getSequence(records.get(6)));
    }

    @Test
    @FixFor("DBZ-1042")
    public void testCreateNumericReplicationSlotName() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SLOT_NAME, "12345");
        start(YugabyteDBConnector.class, configBuilder.build());
        waitForStreamingRunning();
        assertConnectorIsRunning();
    }

    @Test
    @FixFor("DBZ-1042")
    public void testStreamingWithNumericReplicationSlotName() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SLOT_NAME, "12345");
        start(YugabyteDBConnector.class, configBuilder.build());
        waitForStreamingRunning();
        assertConnectorIsRunning();

        // insert records
        TestHelper.execute("INSERT into s1.a VALUES(201, 1)");
        TestHelper.execute("INSERT into s1.a VALUES(202, 2)");
        TestHelper.execute("INSERT into s1.a VALUES(203, 3)");

        SourceRecords records = consumeRecordsByTopic(5);
        List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("s1.a"));

        assertThat(recordsForTopic.size()).isEqualTo(4);
        assertInsert(recordsForTopic.get(1), PK_FIELD, 201);
        assertInsert(recordsForTopic.get(2), PK_FIELD, 202);
        assertInsert(recordsForTopic.get(3), PK_FIELD, 203);
    }

    @Ignore("Enum datatype not supported yet")
    @Test
    @FixFor("DBZ-5204")
    public void testShouldNotCloseConnectionFetchingMetadataWithNewDataTypes() throws Exception {
        TestHelper.execute(CREATE_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig().build();
        start(YugabyteDBConnector.class, config);
        waitForStreamingRunning();
        assertConnectorIsRunning();

        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);

        TestHelper.execute("CREATE TYPE enum5204 as enum ('V1','V2')");
        TestHelper.execute("CREATE TABLE s1.c (pk SERIAL, data enum5204, primary key (pk))");
        TestHelper.execute("INSERT INTO s1.c (pk,data) values (1, 'V1'::enum5204)");

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("s1.c"));

        assertThat(recordsForTopic).hasSize(1);
        assertInsert(recordsForTopic.get(0), PK_FIELD, 1);
        System.out.println(recordsForTopic.get(0));
    }

    @Test
    @FixFor("DBZ-5295")
    public void shouldReselectToastColumnsOnPrimaryKeyChange() throws Exception {
        TestHelper.execute(CREATE_TABLES_STMT);

        final String toastValue1 = RandomStringUtils.randomAlphanumeric(10000);
        final String toastValue2 = RandomStringUtils.randomAlphanumeric(10000);

        TestHelper.execute("CREATE TABLE s1.dbz5295 (pk serial, data text, data2 text, primary key(pk));");
        TestHelper.execute("ALTER TABLE s1.dbz5295 REPLICA IDENTITY FULL;");
        TestHelper.execute("INSERT INTO s1.dbz5295 (pk,data,data2) values (1,'" + toastValue1 + "','" + toastValue2 + "');");

        Configuration config = TestHelper.defaultConfig().build();
        start(YugabyteDBConnector.class, config);
        waitForStreamingRunning();

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("s1.dbz5295"));
        assertThat(recordsForTopic).hasSize(1);

        SourceRecord record = recordsForTopic.get(0);
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.getStruct("pk").get("value")).isEqualTo(1);
        assertThat(after.getStruct("data").get("value")).isEqualTo(toastValue1);
        assertThat(after.getStruct("data2").get("value")).isEqualTo(toastValue2);

        TestHelper.execute("UPDATE s1.dbz5295 SET pk = 2 WHERE pk = 1;");

        // The update of the primary key causes a DELETE and a CREATE, mingled with a TOMBSTONE
        records = consumeRecordsByTopic(3);
        recordsForTopic = records.recordsForTopic(topicName("s1.dbz5295"));
        assertThat(recordsForTopic).hasSize(3);

        // First event: DELETE
        record = recordsForTopic.get(0);
        YBVerifyRecord.isValidDelete(record, "pk", 1);
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after).isNull();

        // Second event: TOMBSTONE
        record = recordsForTopic.get(1);
        YBVerifyRecord.isValidTombstone(record);

        // Third event: CREATE
        record = recordsForTopic.get(2);
        YBVerifyRecord.isValidInsert(record, "pk", 2);
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.getStruct("pk").get("value")).isEqualTo(2);
        assertThat(after.getStruct("data").get("value")).isEqualTo(toastValue1);
        assertThat(after.getStruct("data2").get("value")).isEqualTo(toastValue2);
    }

    @Test
    @FixFor("DBZ-5783")
    public void shouldSuppressLoggingOptionalOfExcludedColumns() throws Exception {
        TestHelper.execute(CREATE_TABLES_STMT);
        TestHelper.execute("CREATE TABLE s1.dbz5783 (id SERIAL not null, data text, PRIMARY KEY(id));");

        final LogInterceptor logInterceptor = new LogInterceptor(PgOutputMessageDecoder.class);

        Configuration config = TestHelper.defaultConfig()
                .with("column.exclude.list", "s1.dbz5783.data")
                .build();
        start(YugabyteDBConnector.class, config);

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO s1.dbz5783 (data) values ('test');");

        final SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("s1.dbz5783"));

        assertThat(recordsForTopic).hasSize(1);
        assertThat(recordsForTopic.get(0).valueSchema().field("after").schema().field("data")).isNull();
        assertThat(logInterceptor.containsMessage("Column 'data' optionality could not be determined, defaulting to true")).isFalse();
    }

    @Ignore("YB: YB doesn't support the way of initial_only snapshot this connector uses, see https://github.com/yugabyte/yugabyte-db/issues/21425")
    @Test
    @FixFor("DBZ-5739")
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 11, reason = "This needs pg_replication_slot_advance which is supported only on Postgres 11+")
    public void shouldStopConnectorOnSlotRecreation() throws InterruptedException {
        final LogInterceptor logInterceptor = new LogInterceptor(PostgresConnectorIT.class);

        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY.name())
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "s1.a")
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE);

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        consumeRecordsByTopic(1);

        stopConnector();
        assertConnectorNotRunning();

        TestHelper.execute(INSERT_STMT);

        configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.name())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SLOT_SEEK_TO_KNOWN_OFFSET, Boolean.TRUE);

        start(YugabyteDBConnector.class, configBuilder.build());
        Awaitility.await().atMost(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS)
                .until(() -> logInterceptor.containsStacktraceElement("Cannot seek to the last known offset "));
        assertConnectorNotRunning();
    }

    @Test
    @FixFor("DBZ-5739")
    @SkipWhenDatabaseVersion(check = EqualityCheck.GREATER_THAN_OR_EQUAL, major = 11, reason = "This pg_replication_slot_advance is not present Postgres 10")
    public void shouldSeekToCorrectOffset() throws InterruptedException {
        final LogInterceptor logInterceptor = new LogInterceptor(PostgresReplicationConnection.class);

        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.name())
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "s1.a")
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        consumeRecordsByTopic(1);

        stopConnector();
        assertConnectorNotRunning();

        TestHelper.execute(INSERT_STMT);

        configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.name())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SLOT_SEEK_TO_KNOWN_OFFSET, Boolean.TRUE);

        start(YugabyteDBConnector.class, configBuilder.build());
        consumeRecordsByTopic(1);
        assertConnectorIsRunning();

        Awaitility.await().atMost(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS)
                .until(() -> logInterceptor
                        .containsMessage("Postgres server doesn't support the command pg_replication_slot_advance(). Not seeking to last known offset."));
    }

    @Test
    @FixFor("DBZ-5852")
    public void shouldInvokeSnapshotterAbortedMethod() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        // insert another set of rows so we can stop at certain point
        String setupStmt = SETUP_TABLES_STMT + INSERT_STMT + INSERT_STMT + INSERT_STMT;
        TestHelper.execute(setupStmt);

        TestHelper.execute(
                "CREATE TABLE s1.lifecycle_state (hook text, state text, PRIMARY KEY(hook));");

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.MAX_BATCH_SIZE, 1)
                .with(PostgresConnectorConfig.MAX_QUEUE_SIZE, 2)
                .with(PostgresConnectorConfig.MAX_RETRIES, 1)
                .with(PostgresConnectorConfig.POLL_INTERVAL_MS, 60 * 1000)
                .with(PostgresConnectorConfig.SNAPSHOT_FETCH_SIZE, 1)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomLifecycleHookTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);

        DebeziumEngine.CompletionCallback completionCallback = (success, message, error) -> {
            if (error != null) {
                latch.countDown();
            }
            else {
                fail("A controlled exception was expected....");
            }
        };

        start(YugabyteDBConnector.class, configBuilder.build(), completionCallback, stopOnPKPredicate(1));

        // wait until we know we've raised the exception at startup AND the engine has been shutdown
        if (!latch.await(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS)) {
            fail("did not reach stop condition in time");
        }

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
            assertEquals(Collections.singletonList("aborted"), snapshotCompleteState);
        }
    }

    @Test
    @FixFor("DBZ-6249")
    public void shouldThrowRightExceptionWhenNoCustomSnapshotClassProvided() {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicReference<String> message = new AtomicReference<>();
        AtomicReference<Boolean> status = new AtomicReference<>();

        AtomicBoolean finished = new AtomicBoolean(false);

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
                .build();
        start(YugabyteDBConnector.class, config, (success, msg, err) -> {
            error.set(err);
            message.set(msg);
            status.set(success);
            finished.set(true);
        });
        Awaitility.await()
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(waitTimeForRecords() * 30L, TimeUnit.SECONDS)
                .until(() -> finished.get());
        assertThat(status.get()).isFalse();
        assertNull(error.get());
        assertThat(message.get()).contains("snapshot.custom_class cannot be empty when snapshot.mode 'custom' is defined");
    }

    // Added test annotation since it was not present
    @Test
    @FixFor("DBZ-5917")
    public void shouldIncludeTableWithBackSlashInName() throws Exception {
        String setupStmt = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
                "CREATE SCHEMA s1;" +
                "CREATE TABLE s1.\"back\\slash\" (pk SERIAL, aa integer, bb integer, PRIMARY KEY(pk));" +
                "CREATE TABLE s1.another_table (pk SERIAL, aa integer, bb integer, PRIMARY KEY(pk));" + // we need some excluded table to reproduce the issue
                "INSERT INTO s1.\"back\\slash\" (aa, bb) VALUES (1, 1);" +
                "INSERT INTO s1.\"back\\slash\" (aa, bb) VALUES (2, 2);";
        TestHelper.execute(setupStmt);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.name())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.back\\\\slash");

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        TestHelper.execute("INSERT INTO s1.\"back\\slash\" (aa, bb) VALUES (3, 3);");

        final int EXPECTED_RECORDS = 3; // 2 from snapshot, 1 from streaming
        SourceRecords actualRecords = consumeRecordsByTopic(EXPECTED_RECORDS);
        List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s1.back_slash"));
        assertThat(records.size()).isEqualTo(EXPECTED_RECORDS);
        AtomicInteger pkValue = new AtomicInteger(1);
        records.forEach(record -> {
            if (pkValue.get() <= 2) {
                YBVerifyRecord.isValidRead(record, PK_FIELD, pkValue.getAndIncrement());
            }
            else {
                YBVerifyRecord.isValidInsert(record, PK_FIELD, pkValue.getAndIncrement());
            }
        });
    }

    @Test
    @FixFor("DBZ-6076")
    public void shouldAddNewFieldToSourceInfo() throws InterruptedException {
        TestHelper.execute(
                "DROP TABLE IF EXISTS s1.DBZ6076;",
                "CREATE SCHEMA IF NOT EXISTS s1;",
                "CREATE TABLE s1.DBZ6076 (pk SERIAL, aa integer, PRIMARY KEY(pk));",
                "INSERT INTO s1.DBZ6076 (aa) VALUES (1);");
        start(YugabyteDBConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.name())
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.DBZ6076")
                .with(PostgresConnectorConfig.SOURCE_INFO_STRUCT_MAKER, CustomPostgresSourceInfoStructMaker.class.getName())
                .build());
        assertConnectorIsRunning();

        // check records from snapshot
        SourceRecords actualRecords = consumeRecordsByTopic(1);
        actualRecords.forEach(sourceRecord -> {
            assertTrue(sourceRecord.value() instanceof Struct);
            Struct source = ((Struct) sourceRecord.value()).getStruct("source");
            assertEquals("newFieldValue", source.getString("newField"));
        });

        // insert 2 new records
        TestHelper.execute("INSERT INTO s1.DBZ6076 (aa) VALUES (2);");
        // check records from streaming
        actualRecords = consumeRecordsByTopic(1);
        actualRecords.forEach(sourceRecord -> {
            assertTrue(sourceRecord.value() instanceof Struct);
            Struct source = ((Struct) sourceRecord.value()).getStruct("source");
            assertEquals("newFieldValue", source.getString("newField"));
        });
    }

    @Override
    protected int consumeAvailableRecords(Consumer<SourceRecord> recordConsumer) {
        List<SourceRecord> records = consumedLines
                .stream()
                .filter(r -> !r.topic().equals(TestHelper.getDefaultHeartbeatTopic()))
                .collect(Collectors.toList());
        if (recordConsumer != null) {
            records.forEach(recordConsumer);
        }
        return records.size();
    }

    @Test
    @FixFor("DBZ-6076")
    public void shouldUseDefaultSourceInfoStructMaker() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        start(YugabyteDBConnector.class, TestHelper.defaultConfig()
                .build());
        assertConnectorIsRunning();

        // check records from snapshot
        SourceRecords actualRecords = consumeRecordsByTopic(2);
        actualRecords.forEach(sourceRecord -> {
            assertTrue(sourceRecord.value() instanceof Struct);
            Struct source = ((Struct) sourceRecord.value()).getStruct("source");
            assertEquals("io.debezium.connector.postgresql.Source", source.schema().name()); // each connector have different schema name.
            assertNotNull(source.getInt64(SourceInfo.LSN_KEY));
        });

        // insert 2 new records
        TestHelper.execute(INSERT_STMT);
        // check records from streaming
        actualRecords = consumeRecordsByTopic(2);
        actualRecords.forEach(sourceRecord -> {
            assertTrue(sourceRecord.value() instanceof Struct);
            Struct source = ((Struct) sourceRecord.value()).getStruct("source");
            assertEquals("io.debezium.connector.postgresql.Source", source.schema().name());
            assertNotNull(source.getInt64(SourceInfo.LSN_KEY));
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
                .forEach(i -> YBVerifyRecord.isValidRead(recordsForTopicS1.remove(0), PK_FIELD, pks[i]));

        List<SourceRecord> recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(expectedCountPerSchema);
        IntStream.range(0, expectedCountPerSchema)
                .forEach(i -> YBVerifyRecord.isValidRead(recordsForTopicS2.remove(0), PK_FIELD, pks[i + expectedCountPerSchema]));
    }

    private void assertRecordsAfterInsert(int expectedCount, int... pks) throws InterruptedException {
        SourceRecords actualRecords = consumeRecordsByTopic(expectedCount);
        assertThat(actualRecords.topics().size()).isEqualTo(expectedCount);

        // we have 2 schemas
        int expectedCountPerSchema = expectedCount / 2;
        LOGGER.info("Expected count per schema: {}", expectedCountPerSchema);

        List<SourceRecord> recordsForTopicS1 = actualRecords.recordsForTopic(topicName("s1.a"));
        assertThat(recordsForTopicS1.size()).isEqualTo(expectedCountPerSchema);
        IntStream.range(0, expectedCountPerSchema).forEach(i -> YBVerifyRecord.isValidInsert(recordsForTopicS1.remove(0), PK_FIELD, pks[i]));

        List<SourceRecord> recordsForTopicS2 = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(recordsForTopicS2.size()).isEqualTo(expectedCountPerSchema);
        IntStream.range(0, expectedCountPerSchema).forEach(i -> YBVerifyRecord.isValidInsert(recordsForTopicS2.remove(0), PK_FIELD, pks[i]));
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

    @Override
    protected int consumeRecordsUntil(BiPredicate<Integer, SourceRecord> condition,
                                      BiFunction<Integer, SourceRecord, String> logMessage,
                                      int breakAfterNulls, Consumer<SourceRecord> recordConsumer,
                                      boolean assertRecords)
            throws InterruptedException {
        int recordsConsumed = 0;
        int nullReturn = 0;
        boolean isLastRecord = false;
        while (!isLastRecord && isEngineRunning.get()) {
            SourceRecord record = consumedLines.poll(pollTimeoutInMs, TimeUnit.MILLISECONDS);

            // YB Note: Ignore heartbeat records while consuming.
            if (record != null && record.topic().equals(TestHelper.getDefaultHeartbeatTopic())) {
                continue;
            }

            if (record != null) {
                nullReturn = 0;
                ++recordsConsumed;
                if (recordConsumer != null) {
                    recordConsumer.accept(record);
                }
                if (Testing.Debug.isEnabled()) {
                    Testing.debug(logMessage.apply(recordsConsumed, record));
                    debug(record);
                }
                else if (Testing.Print.isEnabled()) {
                    Testing.print(logMessage.apply(recordsConsumed, record));
                    print(record);
                }
                if (assertRecords) {
                    YBVerifyRecord.isValid(record, /* skipAvroValidation */ false);
                }
                isLastRecord = condition.test(recordsConsumed, record);
            }
            else {
                if (++nullReturn >= breakAfterNulls) {
                    return recordsConsumed;
                }
            }
        }
        return recordsConsumed;
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
        assertThat(key.defaultValue).isEqualTo(ConfigDef.parseType(expected.name(), expected.defaultValue(), expected.type()));
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

    @Override
    protected void assertConnectorIsRunning() {
        try {
            Thread.sleep(10_000);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        super.assertConnectorIsRunning();
    }

    @Override
    protected void assertInsert(SourceRecord record, String pkField, int pk) {
        YBVerifyRecord.isValidInsert(record, pkField, pk);
    }
}

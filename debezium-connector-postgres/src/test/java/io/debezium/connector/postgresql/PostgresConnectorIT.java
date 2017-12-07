/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode.ALWAYS;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode.INITIAL;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode.INITIAL_ONLY;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode.NEVER;
import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.List;
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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.util.PSQLState;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.postgresql.PostgresConnectorConfig.LogicalDecoder;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.util.Strings;

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
                                                    "CREATE TABLE s2.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                                                    INSERT_STMT;
    private PostgresConnector connector;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @Before
    public void before() {
        initializeConnectorTestFramework();
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
        assertConfigurationErrors(validatedConfig, PostgresConnectorConfig.PASSWORD, 1);
        assertConfigurationErrors(validatedConfig, PostgresConnectorConfig.DATABASE_NAME, 1);

        // validate the non required fields
        validateField(validatedConfig, PostgresConnectorConfig.PLUGIN_NAME, LogicalDecoder.DECODERBUFS.getValue());
        validateField(validatedConfig, PostgresConnectorConfig.SLOT_NAME, ReplicationConnection.Builder.DEFAULT_SLOT_NAME);
        validateField(validatedConfig, PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        validateField(validatedConfig, PostgresConnectorConfig.PORT, PostgresConnectorConfig.DEFAULT_PORT);
        validateField(validatedConfig, PostgresConnectorConfig.SERVER_NAME, null);
        validateField(validatedConfig, PostgresConnectorConfig.TOPIC_SELECTION_STRATEGY, PostgresConnectorConfig.TopicSelectionStrategy.TOPIC_PER_TABLE);
        validateField(validatedConfig, PostgresConnectorConfig.MAX_QUEUE_SIZE, PostgresConnectorConfig.DEFAULT_MAX_QUEUE_SIZE);
        validateField(validatedConfig, PostgresConnectorConfig.MAX_BATCH_SIZE, PostgresConnectorConfig.DEFAULT_MAX_BATCH_SIZE);
        validateField(validatedConfig, PostgresConnectorConfig.ROWS_FETCH_SIZE, PostgresConnectorConfig.DEFAULT_ROWS_FETCH_SIZE);
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
        validateField(validatedConfig, PostgresConnectorConfig.SNAPSHOT_MODE, INITIAL);
        validateField(validatedConfig, PostgresConnectorConfig.SNAPSHOT_LOCK_TIMEOUT_MS, PostgresConnectorConfig.DEFAULT_SNAPSHOT_LOCK_TIMEOUT_MILLIS);
        validateField(validatedConfig, PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ADAPTIVE);
        validateField(validatedConfig, PostgresConnectorConfig.DECIMAL_HANDLING_MODE, PostgresConnectorConfig.DecimalHandlingMode.PRECISE);
        validateField(validatedConfig, PostgresConnectorConfig.SSL_SOCKET_FACTORY, null);
        validateField(validatedConfig, PostgresConnectorConfig.TCP_KEEPALIVE, null);
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
                assertThat(PSQLState.CONNECTION_REJECTED).isEqualTo(new PSQLState(((SQLException)cause).getSQLState()));
            }
        });
        if (TestHelper.shouldSSLConnectionFail()) {
            assertConnectorNotRunning();
        } else {
            assertConnectorIsRunning();
            Thread.sleep(10000);
            stopConnector();
        }
    }

    @Test
    public void shouldProduceEventsWithInitialSnapshot() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                                               .with(PostgresConnectorConfig.SNAPSHOT_MODE, INITIAL.getValue())
                                               .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        //check the records from the snapshot
        assertRecordsFromSnapshot(2, 1, 1);

        // insert 2 new records
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 2, 2);

        //now stop the connector
        stopConnector();
        assertNoRecordsToConsume();

        //insert some more records
        TestHelper.execute(INSERT_STMT);

        //start the connector back up and check that a new snapshot has not been performed (we're running initial only mode)
        //but the 2 records that we were inserted while we were down will be retrieved
        start(PostgresConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();

        assertRecordsAfterInsert(2, 3, 3);
    }

    @Test
    public void shouldIgnoreViews() throws Exception {
        TestHelper.execute(
                SETUP_TABLES_STMT +
                "CREATE VIEW s1.myview AS SELECT * from s1.a;"
        );
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                                               .with(PostgresConnectorConfig.SNAPSHOT_MODE, INITIAL.getValue())
                                               .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        //check the records from the snapshot
        assertRecordsFromSnapshot(2, 1, 1);

        // insert 2 new records
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 2, 2);

        //now stop the connector
        stopConnector();
        assertNoRecordsToConsume();

        //insert some more records
        TestHelper.execute(INSERT_STMT);

        //start the connector back up and check that a new snapshot has not been performed (we're running initial only mode)
        //but the 2 records that we were inserted while we were down will be retrieved
        start(PostgresConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();

        assertRecordsAfterInsert(2, 3, 3);
    }

    @Test
    public void shouldProduceEventsWhenSnapshotsAreNeverAllowed() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                                         .with(PostgresConnectorConfig.SNAPSHOT_MODE, NEVER.getValue())
                                         .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                                         .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        // insert and verify 2 new records
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 2, 2);
    }

    @Test
    public void shouldNotProduceEventsWithInitialOnlySnapshot() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                                         .with(PostgresConnectorConfig.SNAPSHOT_MODE, INITIAL_ONLY.getValue())
                                         .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                                         .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        //check the records from the snapshot
        assertRecordsFromSnapshot(2, 1, 1);

        // insert and verify that no events were received since the connector should not be streaming changes
        TestHelper.execute(INSERT_STMT);
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);
        // there shouldn't be any  records
        assertNoRecordsToConsume();
    }

    @Test
    public void shouldProduceEventsWhenAlwaysTakingSnapshots() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                                               .with(PostgresConnectorConfig.SNAPSHOT_MODE, ALWAYS.getValue())
                                               .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        //check the records from the snapshot
        assertRecordsFromSnapshot(2, 1, 1);

        // insert and verify 2 new records
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 2, 2);

        //now stop the connector
        stopConnector();
        assertNoRecordsToConsume();

        //start the connector back up and check that a new snapshot has been performed
        start(PostgresConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();

        assertRecordsFromSnapshot(4, 1, 2, 1, 2);
    }

    @Test
    public void shouldResumeSnapshotIfFailingMidstream() throws Exception {
        // insert another set of rows so we can stop at certain point
        CountDownLatch latch = new CountDownLatch(1);
        String setupStmt = SETUP_TABLES_STMT + INSERT_STMT;
        TestHelper.execute(setupStmt);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                                                        .with(PostgresConnectorConfig.SNAPSHOT_MODE, INITIAL.getValue())
                                                        .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        EmbeddedEngine.CompletionCallback completionCallback = (success, message, error) -> {
            if (error != null) {
                latch.countDown();
            } else {
                fail("A controlled exception was expected....");
            }
        };
        start(PostgresConnector.class, configBuilder.build(), completionCallback, stopOnPKPredicate(2));
        // wait until we know we've raised the exception at startup AND the engine has been shutdown
        if (!latch.await(10, TimeUnit.SECONDS)) {
            fail("did not reach stop condition in time");
        }
        // wait until we know we've raised the exception at startup AND the engine has been shutdown
        assertConnectorNotRunning();
        // just drain all the records
        consumeAvailableRecords(record->{});
        // stop the engine altogether
        stopConnector();
        // make sure there are no records to consume
        assertNoRecordsToConsume();
        // start the connector back up and check that it took another full snapshot since previously it was stopped midstream
        start(PostgresConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();

        //check that the snapshot was recreated
        assertRecordsFromSnapshot(4, 1, 2, 1, 2);

        //and we can stream records
        // insert and verify 2 new records
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 3, 3);
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
                                                        .with(PostgresConnectorConfig.SNAPSHOT_MODE, INITIAL.getValue())
                                                        .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                                                        .with(PostgresConnectorConfig.SCHEMA_BLACKLIST, "s2")
                                                        .with(PostgresConnectorConfig.TABLE_BLACKLIST, ".+b")
                                                        .with(PostgresConnectorConfig.COLUMN_BLACKLIST, ".+bb");

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        //check the records from the snapshot take the filters into account
        SourceRecords actualRecords = consumeRecordsByTopic(4); //3 records in s1.a and 1 in s1.b

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
        String insertStmt =  "INSERT INTO s1.b (aa, bb) VALUES (6, 6);" +
                             "INSERT INTO s2.a (aa) VALUES (7);";
        TestHelper.execute(insertStmt);
        assertNoRecordsToConsume();
    }

    private void assertFieldAbsent(SourceRecord record, String fieldName) {
        Struct value = (Struct) ((Struct) record.value()).get(Envelope.FieldName.AFTER);
        try {
            value.get(fieldName);
            fail("field should not be present");
        } catch (DataException e) {
            //expected
        }
    }

    @Test
    @Ignore
    public void testStreamingPerformance() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                                                        .with(PostgresConnectorConfig.SNAPSHOT_MODE, NEVER.getValue())
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
            int consumed = super.consumeAvailableRecords(record -> {});
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
                                                        .with(PostgresConnectorConfig.SNAPSHOT_MODE, INITIAL_ONLY.getValue())
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

    private CompletableFuture<Void> batchInsertRecords(long recordsCount, int batchSize) {
        String insertStmt = "INSERT INTO text_table(j, jb, x, u) " +
                            "VALUES ('{\"bar\": \"baz\"}'::json, '{\"bar\": \"baz\"}'::jsonb, " +
                            "'<foo>bar</foo><foo>bar</foo>'::xml, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID);";
        return CompletableFuture.runAsync(() -> {
            StringBuilder stmtBuilder = new StringBuilder();
            for (int i = 0; i < recordsCount; i++) {
                stmtBuilder.append(insertStmt).append(System.lineSeparator());
                if (i > 0  && i % batchSize == 0) {
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

    private void assertRecordsFromSnapshot(int expectedCount, int...pks) throws InterruptedException {
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

    private void assertRecordsAfterInsert(int expectedCount, int...pks) throws InterruptedException {
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

    private <T> void validateField(Config config, Field field, T expectedValue) {
        assertNoConfigurationErrors(config, field);
        Object actualValue = configValue(config, field.name()).value();
        if (actualValue == null) {
            actualValue = field.defaultValue();
        }
        if (expectedValue == null) {
            assertThat(actualValue).isNull();
        } else {
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
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Collect;

/**
 * Integration test for {@link RecordsSnapshotProducerIT}
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class RecordsSnapshotProducerIT extends AbstractRecordsProducerTest {

    private RecordsSnapshotProducer snapshotProducer;
    private PostgresTaskContext context;

    @Before
    public void before() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("init_postgis.ddl");
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.executeDDL("postgis_create_tables.ddl");

        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .build());
        TopicSelector<TableId> selector = PostgresTopicSelector.create(config);
        context = new PostgresTaskContext(
                config,
                TestHelper.getSchema(config),
                selector
        );
    }

    @After
    public void after() throws Exception {
        if (snapshotProducer != null) {
            snapshotProducer.stop();
        }
    }

    @Test
    public void shouldGenerateSnapshotsForDefaultDatatypes() throws Exception {
        snapshotProducer = new RecordsSnapshotProducer(context, new SourceInfo(TestHelper.TEST_SERVER, TestHelper.TEST_DATABASE), false, false);

        TestConsumer consumer = testConsumer(ALL_STMTS.size(), "public", "Quoted__");

        //insert data for each of different supported types
        String statementsBuilder = ALL_STMTS.stream().collect(Collectors.joining(";" + System.lineSeparator())) + ";";
        TestHelper.execute(statementsBuilder);

        //then start the producer and validate all records are there
        snapshotProducer.start(consumer, e -> {});
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        Map<String, List<SchemaAndValueField>> expectedValuesByTopicName = super.schemaAndValuesByTopicName();
        consumer.process(record -> assertReadRecord(record, expectedValuesByTopicName));

        // check the offset information for each record
        while (!consumer.isEmpty()) {
            SourceRecord record = consumer.remove();
            assertRecordOffsetAndSnapshotSource(record, true, consumer.isEmpty());
            assertSourceInfo(record);
        }
    }

    @Test
    public void shouldGenerateSnapshotsForCustomDatatypes() throws Exception {
        final PostgresConnectorConfig config = new PostgresConnectorConfig(
                TestHelper.defaultConfig()
                    .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
                    .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                    .build()
        );
        context = new PostgresTaskContext(
                config,
                TestHelper.getSchema(config),
                PostgresTopicSelector.create(config)
        );
        snapshotProducer = new RecordsSnapshotProducer(context, new SourceInfo(TestHelper.TEST_SERVER, TestHelper.TEST_DATABASE), false, false);

        final TestConsumer consumer = testConsumer(1, "public");

        TestHelper.execute(INSERT_CUSTOM_TYPES_STMT);

        //then start the producer and validate all records are there
        snapshotProducer.start(consumer, e -> {});
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValuesByTopicName = Collect.hashMapOf("public.custom_table", schemasAndValuesForCustomTypes());
        consumer.process(record -> assertReadRecord(record, expectedValuesByTopicName));
    }

    @Test
    public void shouldGenerateSnapshotAndContinueStreaming() throws Exception {
        // PostGIS must not be used
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");

        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        TopicSelector<TableId> selector = PostgresTopicSelector.create(config);
        context = new PostgresTaskContext(
                config,
                TestHelper.getSchema(config),
                selector
        );

        String insertStmt = "INSERT INTO s1.a (aa) VALUES (1);" +
                            "INSERT INTO s2.a (aa) VALUES (1);";

        String statements = "CREATE SCHEMA s1; " +
                            "CREATE SCHEMA s2; " +
                            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                            "CREATE TABLE s2.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                            insertStmt;
        TestHelper.execute(statements);

        snapshotProducer = new RecordsSnapshotProducer(context, new SourceInfo(TestHelper.TEST_SERVER, TestHelper.TEST_DATABASE), true, false);
        TestConsumer consumer = testConsumer(2, "s1", "s2");
        snapshotProducer.start(consumer, e -> {});

        // first make sure we get the initial records from both schemas...
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        consumer.clear();

        // then insert some more data and check that we get it back
        TestHelper.execute(insertStmt);
        consumer.expects(2);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        SourceRecord first = consumer.remove();
        VerifyRecord.isValidInsert(first, PK_FIELD, 2);
        assertEquals(topicName("s1.a"), first.topic());
        assertRecordOffsetAndSnapshotSource(first, false, false);
        assertSourceInfo(first, "test_database", "s1", "a");

        SourceRecord second = consumer.remove();
        VerifyRecord.isValidInsert(second, PK_FIELD, 2);
        assertEquals(topicName("s2.a"), second.topic());
        assertRecordOffsetAndSnapshotSource(second, false, false);
        assertSourceInfo(second, "test_database", "s2", "a");

        // now shut down the producers and insert some more records
        snapshotProducer.stop();
        TestHelper.execute(insertStmt);

        // start a new producer back up, take a new snapshot (we expect all the records to be read back)
        int expectedRecordsCount = 6;
        consumer = testConsumer(expectedRecordsCount, "s1", "s2");
        snapshotProducer = new RecordsSnapshotProducer(context, new SourceInfo(TestHelper.TEST_SERVER, TestHelper.TEST_DATABASE), true, false);
        snapshotProducer.start(consumer, e -> {});
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        AtomicInteger counter = new AtomicInteger(0);
        consumer.process(record -> {
            int counterVal = counter.getAndIncrement();
            int expectedPk = (counterVal % 3) + 1; //each table has 3 entries keyed 1-3
            VerifyRecord.isValidRead(record, PK_FIELD, expectedPk);
            assertRecordOffsetAndSnapshotSource(record, true, counterVal == (expectedRecordsCount - 1));
            assertSourceInfo(record);
        });
        consumer.clear();

        // now insert two more records and check that we only get those back from the stream
        TestHelper.execute(insertStmt);
        consumer.expects(2);

        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        first = consumer.remove();
        VerifyRecord.isValidInsert(first, PK_FIELD, 4);
        assertRecordOffsetAndSnapshotSource(first, false, false);
        assertSourceInfo(first, "test_database", "s1", "a");

        second = consumer.remove();
        VerifyRecord.isValidInsert(second, PK_FIELD, 4);
        assertRecordOffsetAndSnapshotSource(second, false, false);
        assertSourceInfo(second, "test_database", "s2", "a");
    }

    @Test
    @FixFor("DBZ-859")
    public void shouldGenerateSnapshotAndSendHeartBeat() throws Exception {
        // PostGIS must not be used
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE TABLE t1 (pk SERIAL, aa integer, PRIMARY KEY(pk)); INSERT INTO t1 VALUES (default, 11)");

        PostgresConnectorConfig config = new PostgresConnectorConfig(
                TestHelper.defaultConfig()
                    .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
                    .with(PostgresConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                    .with(Heartbeat.HEARTBEAT_INTERVAL, 300_000)
                    .build()
        );
        TopicSelector<TableId> selector = PostgresTopicSelector.create(config);
        context = new PostgresTaskContext(
                config,
                TestHelper.getSchema(config),
                selector
        );

        snapshotProducer = new RecordsSnapshotProducer(context, new SourceInfo(TestHelper.TEST_SERVER, TestHelper.TEST_DATABASE), true, false);
        TestConsumer consumer = testConsumer(2);
        snapshotProducer.start(consumer, e -> {});

        // Make sure we get the table schema record and heartbeat record
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        final SourceRecord first = consumer.remove();
        VerifyRecord.isValidRead(first, PK_FIELD, 1);
        assertRecordOffsetAndSnapshotSource(first, true, true);
        final SourceRecord second = consumer.remove();
        assertThat(second.topic()).startsWith("__debezium-heartbeat");
        assertRecordOffsetAndSnapshotSource(second, false, false);

        // now shut down the producers and insert some more records
        snapshotProducer.stop();
    }

    @Test
    public void shouldDoAPartialRecoveryWithXmin() throws Exception {
        // this tests works by
        // 1. creating some writes before on the previous slot
        // 2. drop the previous slot
        // 3. create a new slot
        // 4. use the xmin in first slot and show it only fetches records >= that xmin
        //
        // However, we "fake" the first slot and first xmin as it is hard to force psql to advance
        // the xmin of a slot. We do this by simply creating a record and using the xmin of that
        // record as the lower bound for our new slot
        //
        TestHelper.dropAllSchemas();
        // create the table and insert first fake record
        TestHelper.execute("CREATE TABLE t1 (pk SERIAL, aa integer, PRIMARY KEY(pk)); INSERT INTO t1 VALUES (default, 11)");
        // instead of using slot xmin, cheat and use the xmin from the newly inserted record as the
        // the slots xmin
        long xmin;
        String xminSlotName = "xmin_recovery";
        try (PostgresConnection conn = TestHelper.create()) {
            Connection jconn = conn.connection();
            Statement sxmin = jconn.createStatement();
            ResultSet rs = sxmin.executeQuery("SELECT xmin::text::int FROM t1 where pk = 1");
            assertThat(rs.next()).isTrue();
            xmin = rs.getInt(1);
            // just make sure our named slot is clear..
            conn.dropReplicationSlot(xminSlotName);
        }
        // insert more records, theses ones we would have "missed" after the slot being dropped
        String insertStmt = "INSERT INTO t1 VALUES (default, 12); INSERT INTO t1 VALUES (default, 13)";
        TestHelper.execute(insertStmt);
        // create a new slot after writes to simulate a failover of a slot
        TestHelper.createForReplication("xmin_recovery", true);

        PostgresConnectorConfig config = new PostgresConnectorConfig(
                TestHelper.defaultConfig()
                        .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.WRITE_RECOVERY)
                        .with(PostgresConnectorConfig.SLOT_NAME, "xmin_recovery")
                        .build()
        );
        TopicSelector<TableId> selector = PostgresTopicSelector.create(config);
        context = new PostgresTaskContext(
                config,
                TestHelper.getSchema(config),
                selector
        );

        SourceInfo source = new SourceInfo(TestHelper.TEST_SERVER, TestHelper.TEST_DATABASE);
        source.update(0L, null, 0L, null, xmin);
        snapshotProducer = new RecordsSnapshotProducer(context, source, true, true);
        // we only should expect records after our xmin
        TestConsumer consumer = testConsumer(2);
        snapshotProducer.start(consumer, e -> {});

        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        // assert we don't get our first record
        final SourceRecord first = consumer.remove();
        VerifyRecord.isValidRead(first, PK_FIELD, 2);
        assertRecordOffsetAndSnapshotSource(first, true, false);
        final SourceRecord second = consumer.remove();
        assertRecordOffsetAndSnapshotSource(second, true, true);
        VerifyRecord.isValidRead(second, PK_FIELD, 3);

        consumer.clear();

        // now insert two more records and check that we only get those back from the stream
        TestHelper.execute(insertStmt);
        consumer.expects(2);

        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        final SourceRecord third = consumer.remove();
        VerifyRecord.isValidInsert(third, PK_FIELD, 4);
        assertRecordOffsetAndSnapshotSource(third, false, false);
        final SourceRecord fourth = consumer.remove();
        VerifyRecord.isValidInsert(fourth, PK_FIELD, 5);
        assertRecordOffsetAndSnapshotSource(fourth, false, false);
        snapshotProducer.stop();
    }

    @Test
    public void shouldSkipRecoveryWithXmin() throws Exception {
        // This test simply shows that even with a SnapshotProducer we skip the need to
        // do a recovery if the slot state persists, this should be doable simply by creating
        // two consumers
        String xminSlotName = "xmin_restart";
        try (PostgresConnection conn = TestHelper.create()) {
            // just make sure our named slot is clear..
            conn.dropReplicationSlot(xminSlotName);
        }
        TestHelper.createForReplication(xminSlotName, false);
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE TABLE t1 (pk SERIAL, aa integer, PRIMARY KEY(pk));");
        String insertStmt = "INSERT INTO t1 VALUES (default, 12); INSERT INTO t1 VALUES (default, 13)";
        TestHelper.execute(insertStmt);
        PostgresConnectorConfig config = new PostgresConnectorConfig(
                TestHelper.defaultConfig()
                        .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.WRITE_RECOVERY)
                        .with(PostgresConnectorConfig.SLOT_NAME, xminSlotName)
                        .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false)
                        .build()
        );
        TopicSelector<TableId> selector = PostgresTopicSelector.create(config);
        context = new PostgresTaskContext(
                config,
                TestHelper.getSchema(config),
                selector
        );
        SourceInfo source = new SourceInfo(TestHelper.TEST_SERVER, TestHelper.TEST_DATABASE);
        snapshotProducer = new RecordsSnapshotProducer(context, source, true, true);
        TestConsumer consumer = testConsumer(4);
        snapshotProducer.start(consumer, e -> {});
        // TODO: for some reason, this needs a sleep here when all together
        // It seems like it is blocked on some underyling
        // resource, perhaps a socket back to a thread pool?
        Thread.sleep(100);

        // insert more to ensure we have some non snaps too
        TestHelper.execute(insertStmt);

        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        final SourceRecord first = consumer.remove();
        VerifyRecord.isValidRead(first, PK_FIELD, 1);
        assertRecordOffsetAndSnapshotSource(first, true, false);
        final SourceRecord second = consumer.remove();
        assertRecordOffsetAndSnapshotSource(second, true, true);
        VerifyRecord.isValidRead(second, PK_FIELD, 2);

        // first streamed records
        final SourceRecord third = consumer.remove();
        VerifyRecord.isValidInsert(third, PK_FIELD, 3);
        assertRecordOffsetAndSnapshotSource(third, false, false);
        final SourceRecord fourth = consumer.remove();
        assertRecordOffsetAndSnapshotSource(fourth, false, false);
        VerifyRecord.isValidInsert(fourth, PK_FIELD, 4);
        // commit the last LSN to the slot, this allows us to restart in a proper spot
        Long lsn = (Long) fourth.sourceOffset().get(SourceInfo.LSN_KEY);
        snapshotProducer.commit(lsn);
        Thread.sleep(1000);

        // stop our producer and make a new one
        snapshotProducer.stop();
        consumer.clear();

        // now insert two more records and check that we only get those back from the stream
        TestHelper.execute(insertStmt);
        consumer.expects(2);

        // generate a new sourceinfo from our last record
        // when we commit, the xmin may get advanced, so we need to advance our saved xmin to account for it
        SourceInfo restartSource = new SourceInfo(TestHelper.TEST_SERVER, TestHelper.TEST_DATABASE);
        restartSource.update(lsn,
                (Long) fourth.sourceOffset().get(SourceInfo.TIMESTAMP_KEY),
                (Long) fourth.sourceOffset().get(SourceInfo.TXID_KEY),
                (TableId) fourth.sourceOffset().get(SourceInfo.TABLE_NAME_KEY),
                context.getCurrentSlotInfo().catalogXmin());

        snapshotProducer = new RecordsSnapshotProducer(context, restartSource, true, true);
        snapshotProducer.start(consumer, e -> {});
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        final SourceRecord fifth = consumer.remove();
        VerifyRecord.isValidInsert(fifth, PK_FIELD, 5);
        assertRecordOffsetAndSnapshotSource(fifth, false, false);
        final SourceRecord sixth = consumer.remove();
        assertRecordOffsetAndSnapshotSource(sixth, false, false);
        VerifyRecord.isValidInsert(sixth, PK_FIELD, 6);

        snapshotProducer.stop();
        try (PostgresConnection conn = TestHelper.create()) {
            // just make sure our named slot is clear..
            conn.dropReplicationSlot(xminSlotName);
        }
    }

    private void assertReadRecord(SourceRecord record, Map<String, List<SchemaAndValueField>> expectedValuesByTopicName) {
        VerifyRecord.isValidRead(record, PK_FIELD, 1);
        String topicName = record.topic().replace(TestHelper.TEST_SERVER + ".", "");
        List<SchemaAndValueField> expectedValuesAndSchemasForTopic = expectedValuesByTopicName.get(topicName);
        assertNotNull("No expected values for " + topicName + " found", expectedValuesAndSchemasForTopic);
        assertRecordSchemaAndValues(expectedValuesAndSchemasForTopic, record, Envelope.FieldName.AFTER);
    }

    @Test
    @FixFor("DBZ-342")
    public void shouldGenerateSnapshotsForDefaultDatatypesAdaptiveMicroseconds() throws Exception {
        PostgresConnectorConfig config = new PostgresConnectorConfig(
                TestHelper.defaultConfig()
                        .with(PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS)
                        .build());

        TopicSelector<TableId> selector = PostgresTopicSelector.create(config);
        context = new PostgresTaskContext(
                config,
                TestHelper.getSchema(config),
                selector
        );

        snapshotProducer = new RecordsSnapshotProducer(context, new SourceInfo(TestHelper.TEST_SERVER, TestHelper.TEST_DATABASE), false, false);

        TestConsumer consumer = testConsumer(ALL_STMTS.size(), "public", "Quoted__");

        //insert data for each of different supported types
        String statementsBuilder = ALL_STMTS.stream().collect(Collectors.joining(";" + System.lineSeparator())) + ";";
        TestHelper.execute(statementsBuilder);

        //then start the producer and validate all records are there
        snapshotProducer.start(consumer, e -> {});
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        Map<String, List<SchemaAndValueField>> expectedValuesByTopicName = super.schemaAndValuesByTopicNameAdaptiveTimeMicroseconds();
        consumer.process(record -> assertReadRecord(record, expectedValuesByTopicName));

        // check the offset information for each record
        while (!consumer.isEmpty()) {
            SourceRecord record = consumer.remove();
            assertRecordOffsetAndSnapshotSource(record, true, consumer.isEmpty());
            assertSourceInfo(record);
        }
    }

    @Test
    @FixFor("DBZ-606")
    public void shouldGenerateSnapshotsForDecimalDatatypesUsingStringEncoding() throws Exception {
        // PostGIS must not be used
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");

        PostgresConnectorConfig config = new PostgresConnectorConfig(
                TestHelper.defaultConfig()
                        .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.STRING)
                        .build());

        TopicSelector<TableId> selector = PostgresTopicSelector.create(config);
        context = new PostgresTaskContext(
                config,
                TestHelper.getSchema(config),
                selector
        );

        snapshotProducer = new RecordsSnapshotProducer(context, new SourceInfo(TestHelper.TEST_SERVER, TestHelper.TEST_DATABASE), false, false);

        TestConsumer consumer = testConsumer(1, "public", "Quoted_\"");

        //insert data for each of different supported types
        TestHelper.execute(INSERT_NUMERIC_DECIMAL_TYPES_STMT);

        //then start the producer and validate all records are there
        snapshotProducer.start(consumer, e -> {});
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        Map<String, List<SchemaAndValueField>> expectedValuesByTopicName = super.schemaAndValuesByTopicNameStringEncodedDecimals();
        consumer.process(record -> assertReadRecord(record, expectedValuesByTopicName));

        // check the offset information for each record
        while (!consumer.isEmpty()) {
            SourceRecord record = consumer.remove();
            assertRecordOffsetAndSnapshotSource(record, true, consumer.isEmpty());
            assertSourceInfo(record);
        }
    }
}

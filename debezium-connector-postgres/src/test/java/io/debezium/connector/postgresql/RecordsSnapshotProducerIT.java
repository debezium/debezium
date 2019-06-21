/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static io.debezium.connector.postgresql.junit.SkipWhenDatabaseVersionLessThan.PostgresVersion.POSTGRES_10;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.postgresql.junit.SkipTestDependingOnDatabaseVersionRule;
import io.debezium.connector.postgresql.junit.SkipWhenDatabaseVersionLessThan;
import io.debezium.connector.postgresql.snapshot.AlwaysSnapshotter;
import io.debezium.connector.postgresql.snapshot.InitialOnlySnapshotter;
import io.debezium.connector.postgresql.spi.Snapshotter;
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

    @Rule
    public final TestRule skip = new SkipTestDependingOnDatabaseVersionRule();

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
        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .build());
        snapshotProducer = buildNoStreamProducer(context, config);

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
        snapshotProducer = buildNoStreamProducer(context, config);

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

        snapshotProducer = buildWithStreamProducer(context, config);
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
        snapshotProducer = buildWithStreamProducer(context, config);
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

        snapshotProducer = buildWithStreamProducer(context, config);
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

    private RecordsSnapshotProducer buildNoStreamProducer(PostgresTaskContext ctx, PostgresConnectorConfig config) {
        Snapshotter sn = new InitialOnlySnapshotter();
        sn.init(config, null, null);
        return new RecordsSnapshotProducer(ctx, TestHelper.sourceInfo(), sn);
    }

    private RecordsSnapshotProducer buildWithStreamProducer(PostgresTaskContext ctx, PostgresConnectorConfig config) {
        Snapshotter sn = new AlwaysSnapshotter();
        sn.init(config, null, null);
        return new RecordsSnapshotProducer(ctx, TestHelper.sourceInfo(), sn);
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

        snapshotProducer = buildNoStreamProducer(context, config);

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

        snapshotProducer = buildNoStreamProducer(context, config);

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

    @Test
    @FixFor("DBZ-1118")
    @SkipWhenDatabaseVersionLessThan(POSTGRES_10)
    public void shouldGenerateSnapshotsForPartitionedTables() throws Exception {
        TestHelper.dropAllSchemas();

        String ddl = "CREATE TABLE first_table (pk integer, user_id integer, PRIMARY KEY(pk));" +

                "CREATE TABLE partitioned (pk serial, user_id integer, aa integer) PARTITION BY RANGE (user_id);" +

                "CREATE TABLE partitioned_1_100 PARTITION OF partitioned " +
                "(CONSTRAINT p_1_100_pk PRIMARY KEY (pk)) " +
                "FOR VALUES FROM (1) TO (101);" +

                "CREATE TABLE partitioned_101_200 PARTITION OF partitioned " +
                "(CONSTRAINT p_101_200_pk PRIMARY KEY (pk)) " +
                "FOR VALUES FROM (101) TO (201);";

        TestHelper.execute(ddl);

        PostgresConnectorConfig config = new PostgresConnectorConfig(
                TestHelper.defaultConfig()
                        .build());

        TopicSelector<TableId> selector = PostgresTopicSelector.create(config);
        context = new PostgresTaskContext(
                config,
                TestHelper.getSchema(config),
                selector
        );

        snapshotProducer = buildNoStreamProducer(context, config);

        TestConsumer consumer = testConsumer(1 + 2 * 30); // Every record comes once from partitioned table and from partition

        // add 1 record to `first_table`. To reproduce the bug we must process at
        // least one row before processing the partitioned table.
        TestHelper.execute("INSERT INTO first_table (pk, user_id) VALUES (1000, 1);");

        // add 10 random records to the first partition, 20 to the second
        TestHelper.execute("INSERT INTO partitioned (user_id, aa) " +
                "SELECT RANDOM() * 99 + 1, RANDOM() * 100000 " +
                "FROM generate_series(1, 10);");
        TestHelper.execute("INSERT INTO partitioned (user_id, aa) " +
                "SELECT RANDOM() * 99 + 101, RANDOM() * 100000 " +
                "FROM generate_series(1, 20);");

        // then start the producer and validate all records are there
        snapshotProducer.start(consumer, e -> {});
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        Set<Integer> ids = new HashSet<>();

        Map<String, Integer> topicCounts = Collect.hashMapOf(
                "test_server.public.first_table", 0,
                "test_server.public.partitioned", 0,
                "test_server.public.partitioned_1_100", 0,
                "test_server.public.partitioned_101_200", 0);

        consumer.process(record -> {
            Struct key = (Struct) record.key();
            if (key != null) {
                final Integer id = key.getInt32("pk");
                Assertions.assertThat(ids).excludes(id);
                ids.add(id);
            }
            topicCounts.put(record.topic(), topicCounts.get(record.topic()) + 1);
        });

        // verify distinct records
        assertEquals(31, ids.size());

        // verify each topic contains exactly the number of input records
        assertEquals(1, topicCounts.get("test_server.public.first_table").intValue());
        assertEquals(30, topicCounts.get("test_server.public.partitioned").intValue());
        assertEquals(10, topicCounts.get("test_server.public.partitioned_1_100").intValue());
        assertEquals(20, topicCounts.get("test_server.public.partitioned_101_200").intValue());

        // check the offset information for each record
        while (!consumer.isEmpty()) {
            SourceRecord record = consumer.remove();
            assertRecordOffsetAndSnapshotSource(record, true, consumer.isEmpty());
            assertSourceInfo(record);
        }
    }

    @Test
    @FixFor("DBZ-1162")
    public void shouldGenerateSnapshotsForHstores() throws Exception {
        // PostGIS must not be used
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");

        PostgresConnectorConfig config = new PostgresConnectorConfig(
                TestHelper.defaultConfig()
                        .with(PostgresConnectorConfig.HSTORE_HANDLING_MODE, PostgresConnectorConfig.HStoreHandlingMode.JSON)
                        .build());

        TopicSelector<TableId> selector = PostgresTopicSelector.create(config);
        context = new PostgresTaskContext(
                config,
                TestHelper.getSchema(config),
                selector
        );

        snapshotProducer = buildNoStreamProducer(context, config);

        TestConsumer consumer = testConsumer(1, "public", "Quoted_\"");

        //insert data for each of different supported types
        TestHelper.execute(INSERT_HSTORE_TYPE_STMT);

        //then start the producer and validate all records are there
        snapshotProducer.start(consumer, e -> {});
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);


        final Map<String, List<SchemaAndValueField>> expectedValuesByTopicName = Collect.hashMapOf("public.hstore_table", schemaAndValueFieldForJsonEncodedHStoreType());

        consumer.process(record -> assertReadRecord(record, expectedValuesByTopicName));
    }

    @Test
    @FixFor("DBZ-1163")
    public void shouldGenerateSnapshotForATableWithoutPrimaryKey() throws Exception {
        final PostgresConnectorConfig config = new PostgresConnectorConfig(
                TestHelper.defaultConfig()
                        .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
                        .build()
        );
        context = new PostgresTaskContext(
                config,
                TestHelper.getSchema(config),
                PostgresTopicSelector.create(config)
        );
        snapshotProducer = buildNoStreamProducer(context, config);

        final TestConsumer consumer = testConsumer(1, "public");

        TestHelper.execute("insert into table_without_pk values(1, 1000)");

        //then start the producer and validate all records are there
        snapshotProducer.start(consumer, e -> {});
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        List<SchemaAndValueField> schemaAndValueFields = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("val", Schema.OPTIONAL_INT32_SCHEMA, 1000));

        consumer.process(record -> {
            assertThat(record.key()).isNull();
            String actualTopicName = record.topic().replace(TestHelper.TEST_SERVER + ".", "");
            assertEquals("public.table_without_pk", actualTopicName);
            assertRecordSchemaAndValues(schemaAndValueFields, record, Envelope.FieldName.AFTER);
        });
    }

    @Test
    // MACADDR8 Postgres type is only supported since Postgres version 10
    @SkipWhenDatabaseVersionLessThan(POSTGRES_10)
    @FixFor("DBZ-1193")
    public void shouldGenerateSnapshotForMacaddr8Datatype() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE TABLE macaddr8_table(pk SERIAL, m MACADDR8, PRIMARY KEY(pk));");

        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .build());
        snapshotProducer = buildNoStreamProducer(context, config);

        final TestConsumer consumer = testConsumer(1, "public");

        // insert macaddr8 data
        TestHelper.execute(INSERT_MACADDR8_TYPE_STMT);

        // then start the producer and validate the record are there
        snapshotProducer.start(consumer, e -> {});
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValueByTopicName = Collect.hashMapOf("public.macaddr8_table", schemaAndValueForMacaddr8Type());

        consumer.process(record -> assertReadRecord(record, expectedValueByTopicName));
    }

    @Test
    @FixFor("DBZ-1164")
    public void shouldGenerateSnapshotForTwentyFourHourTime() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");

        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .build());
        snapshotProducer = buildNoStreamProducer(context, config);

        final TestConsumer consumer = testConsumer(1, "public");

        // insert data and time data
        TestHelper.execute(INSERT_DATE_TIME_TYPES_STMT);

        snapshotProducer.start(consumer, e -> {});
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValueByTopicName = Collect.hashMapOf("public.time_table", schemaAndValuesForDateTimeTypes());

        consumer.process(record -> assertReadRecord(record, expectedValueByTopicName));
    }
}

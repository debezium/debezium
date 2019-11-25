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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.ComparisonFailure;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.junit.SkipTestDependingOnDatabaseVersionRule;
import io.debezium.connector.postgresql.junit.SkipWhenDatabaseVersionLessThan;
import io.debezium.data.Bits;
import io.debezium.data.Enum;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

/**
 * Integration test for {@link RecordsSnapshotProducerIT}
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class RecordsSnapshotProducerIT extends AbstractRecordsProducerTest {

    @Rule
    public final TestRule skip = new SkipTestDependingOnDatabaseVersionRule();

    @Before
    public void before() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("init_postgis.ddl");
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.executeDDL("postgis_create_tables.ddl");
    }

    @Test
    public void shouldGenerateSnapshotsForDefaultDatatypes() throws Exception {
        // insert data for each of different supported types
        String statementsBuilder = ALL_STMTS.stream().collect(Collectors.joining(";" + System.lineSeparator())) + ";";
        TestHelper.execute(statementsBuilder);

        // then start the producer and validate all records are there
        buildNoStreamProducer(TestHelper.defaultConfig());

        TestConsumer consumer = testConsumer(ALL_STMTS.size(), "public", "Quoted__");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        Map<String, List<SchemaAndValueField>> expectedValuesByTopicName = super.schemaAndValuesByTopicName();
        consumer.process(record -> assertReadRecord(record, expectedValuesByTopicName));
        Testing.Print.enable();
        // check the offset information for each record
        while (!consumer.isEmpty()) {
            SourceRecord record = consumer.remove();
            assertRecordOffsetAndSnapshotSource(record, true, consumer.isEmpty());
            assertSourceInfo(record);
        }
    }

    @Test
    public void shouldGenerateSnapshotsForCustomDatatypes() throws Exception {
        TestHelper.execute(INSERT_CUSTOM_TYPES_STMT);

        // then start the producer and validate all records are there
        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true));

        final TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValuesByTopicName = Collect.hashMapOf("public.custom_table", schemasAndValuesForCustomTypes());
        consumer.process(record -> assertReadRecord(record, expectedValuesByTopicName));
    }

    @Test
    public void shouldGenerateSnapshotAndContinueStreaming() throws Exception {
        // PostGIS must not be used
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");

        String insertStmt = "INSERT INTO s1.a (aa) VALUES (1);" +
                "INSERT INTO s2.a (aa) VALUES (1);";

        String statements = "CREATE SCHEMA s1; " +
                "CREATE SCHEMA s2; " +
                "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                "CREATE TABLE s2.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                insertStmt;
        TestHelper.execute(statements);

        buildWithStreamProducer(TestHelper.defaultConfig());

        TestConsumer consumer = testConsumer(2, "s1", "s2");
        waitForSnapshotToBeCompleted();

        // first make sure we get the initial records from both schemas...
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        consumer.clear();

        // then insert some more data and check that we get it back
        waitForStreamingToStart();
        TestHelper.execute(insertStmt);
        consumer.expects(2);
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        SourceRecord first = consumer.remove();
        VerifyRecord.isValidInsert(first, PK_FIELD, 2);
        assertEquals(topicName("s1.a"), first.topic());
        assertRecordOffsetAndSnapshotSource(first, false, false);
        assertSourceInfo(first, TestHelper.TEST_DATABASE, "s1", "a");

        SourceRecord second = consumer.remove();
        VerifyRecord.isValidInsert(second, PK_FIELD, 2);
        assertEquals(topicName("s2.a"), second.topic());
        assertRecordOffsetAndSnapshotSource(second, false, false);
        assertSourceInfo(second, TestHelper.TEST_DATABASE, "s2", "a");

        // now shut down the producers and insert some more records
        stopConnector();
        assertConnectorNotRunning();
        TestHelper.execute(insertStmt);

        // start a new producer back up, take a new snapshot (we expect all the records to be read back)
        int expectedRecordsCount = 6;
        buildWithStreamProducer(TestHelper.defaultConfig());
        waitForSnapshotToBeCompleted();

        consumer = testConsumer(expectedRecordsCount, "s1", "s2");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        AtomicInteger counter = new AtomicInteger(0);
        consumer.process(record -> {
            int counterVal = counter.getAndIncrement();
            int expectedPk = (counterVal % 3) + 1; // each table has 3 entries keyed 1-3
            VerifyRecord.isValidRead(record, PK_FIELD, expectedPk);
            assertRecordOffsetAndSnapshotSource(record, true, counterVal == (expectedRecordsCount - 1));
            assertSourceInfo(record);
        });
        consumer.clear();

        // now insert two more records and check that we only get those back from the stream
        waitForStreamingToStart();
        TestHelper.execute(insertStmt);
        consumer.expects(2);

        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);
        first = consumer.remove();
        VerifyRecord.isValidInsert(first, PK_FIELD, 4);
        assertRecordOffsetAndSnapshotSource(first, false, false);
        assertSourceInfo(first, TestHelper.TEST_DATABASE, "s1", "a");

        second = consumer.remove();
        VerifyRecord.isValidInsert(second, PK_FIELD, 4);
        assertRecordOffsetAndSnapshotSource(second, false, false);
        assertSourceInfo(second, TestHelper.TEST_DATABASE, "s2", "a");
    }

    @Test
    @FixFor("DBZ-1564")
    public void shouldCloseTransactionsAfterSnapshot() throws Exception {
        // PostGIS must not be used
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");

        String insertStmt = "INSERT INTO s1.a (aa) VALUES (1);" +
                "INSERT INTO s2.a (aa) VALUES (1);";

        String statements = "CREATE SCHEMA s1; " +
                "CREATE SCHEMA s2; " +
                "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                "CREATE TABLE s2.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                insertStmt;
        TestHelper.execute(statements);

        buildWithStreamProducer(TestHelper.defaultConfig());

        TestConsumer consumer = testConsumer(2, "s1", "s2");
        waitForSnapshotToBeCompleted();

        // first make sure we get the initial records from both schemas...
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        consumer.clear();

        waitForStreamingToStart();

        TestHelper.noTransactionActive();

        stopConnector();
    }

    @Test
    @FixFor("DBZ-859")
    public void shouldGenerateSnapshotAndSendHeartBeat() throws Exception {
        // PostGIS must not be used
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE TABLE t1 (pk SERIAL, aa integer, PRIMARY KEY(pk)); INSERT INTO t1 VALUES (default, 11)");

        buildWithStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(Heartbeat.HEARTBEAT_INTERVAL, 300_000));

        TestConsumer consumer = testConsumer(2);

        // Make sure we get the table schema record and heartbeat record
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        final SourceRecord first = consumer.remove();
        VerifyRecord.isValidRead(first, PK_FIELD, 1);
        assertRecordOffsetAndSnapshotSource(first, true, true);
        final SourceRecord second = consumer.remove();
        assertThat(second.topic()).startsWith("__debezium-heartbeat");
        assertRecordOffsetAndSnapshotSource(second, false, false);
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
        // insert data for each of different supported types
        String statementsBuilder = ALL_STMTS.stream().collect(Collectors.joining(";" + System.lineSeparator())) + ";";
        TestHelper.execute(statementsBuilder);

        // then start the producer and validate all records are there
        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS));

        TestConsumer consumer = testConsumer(ALL_STMTS.size(), "public", "Quoted__");
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

        // insert data for each of different supported types
        TestHelper.execute(INSERT_NUMERIC_DECIMAL_TYPES_STMT);

        // then start the producer and validate all records are there
        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.STRING));

        TestConsumer consumer = testConsumer(1, "public", "Quoted__");
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
        buildNoStreamProducer(TestHelper.defaultConfig());

        TestConsumer consumer = testConsumer(1 + 2 * 30); // Every record comes once from partitioned table and from partition
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

        // insert data for each of different supported types
        TestHelper.execute(INSERT_HSTORE_TYPE_STMT);

        // then start the producer and validate all records are there
        buildNoStreamProducer(TestHelper.defaultConfig());

        TestConsumer consumer = testConsumer(1, "public", "Quoted__");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValuesByTopicName = Collect.hashMapOf("public.hstore_table", schemaAndValueFieldForJsonEncodedHStoreType());

        consumer.process(record -> assertReadRecord(record, expectedValuesByTopicName));
    }

    @Test
    @FixFor("DBZ-1163")
    public void shouldGenerateSnapshotForATableWithoutPrimaryKey() throws Exception {
        TestHelper.execute("insert into table_without_pk values(1, 1000)");

        // then start the producer and validate all records are there
        buildNoStreamProducer(TestHelper.defaultConfig());

        TestConsumer consumer = testConsumer(1, "public", "Quoted__");
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

        // insert macaddr8 data
        TestHelper.execute(INSERT_MACADDR8_TYPE_STMT);

        // then start the producer and validate the record are there
        buildNoStreamProducer(TestHelper.defaultConfig());

        TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValueByTopicName = Collect.hashMapOf("public.macaddr8_table", schemaAndValueForMacaddr8Type());

        consumer.process(record -> assertReadRecord(record, expectedValueByTopicName));
    }

    @Test
    @FixFor("DBZ-1164")
    public void shouldGenerateSnapshotForTwentyFourHourTime() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");

        // insert data and time data
        TestHelper.execute(INSERT_DATE_TIME_TYPES_STMT);

        buildNoStreamProducer(TestHelper.defaultConfig());

        TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValueByTopicName = Collect.hashMapOf("public.time_table", schemaAndValuesForDateTimeTypes());

        consumer.process(record -> assertReadRecord(record, expectedValueByTopicName));
    }

    @Test
    @FixFor("DBZ-1345")
    public void shouldNotSnapshotMaterializedViews() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE TABLE mv_real_table (pk SERIAL, i integer, s VARCHAR(50), PRIMARY KEY(pk));");
        TestHelper.execute("CREATE MATERIALIZED VIEW mv (pk, s) AS SELECT mrv.pk, mrv.s FROM mv_real_table mrv WITH DATA;");

        // insert data
        TestHelper.execute("INSERT INTO mv_real_table (i,s) VALUES (1,'1');");
        TestHelper.execute("REFRESH MATERIALIZED VIEW mv WITH DATA;");

        buildNoStreamProducer(TestHelper.defaultConfig());

        TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValueByTopicName = Collect.hashMapOf("public.mv_real_table", schemaAndValueForMaterializedViewBaseType());
        consumer.process(record -> assertReadRecord(record, expectedValueByTopicName));
    }

    @Test
    @FixFor("DBZ-1413")
    public void shouldSnapshotDomainTypeWithPropagatedSourceTypeAttributes() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE DOMAIN float83 AS numeric(8,3) DEFAULT 0.0;");
        TestHelper.execute("CREATE DOMAIN money2 AS MONEY DEFAULT 0.0;");
        TestHelper.execute("CREATE TABLE alias_table (pk SERIAL, salary money, salary2 money2, a numeric(8,3), area float83, PRIMARY KEY(pk));");
        TestHelper.execute("INSERT INTO alias_table (salary, salary2, a, area) values (7.25, 8.25, 12345.123, 12345.123);");

        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE)
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with("column.propagate.source.type", "public.alias_table.area,public.alias_table.a"));

        final TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        // Specifying alias money2 results in JDBC type '2001' for 'salary2'
        // Specifying money results in JDBC type '8' for 'salary'

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField("salary", Decimal.builder(2).optional().build(), BigDecimal.valueOf(7.25)),
                new SchemaAndValueField("salary2", Decimal.builder(2).optional().build(), BigDecimal.valueOf(8.25)),
                new SchemaAndValueField("a", SchemaBuilder.float64().optional()
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "NUMERIC")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, "8")
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "3")
                        .build(), 12345.123),
                new SchemaAndValueField("area", SchemaBuilder.float64().optional()
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "FLOAT83")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, "8")
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "3")
                        .build(), 12345.123));

        consumer.process(record -> assertReadRecord(record, Collect.hashMapOf("public.alias_table", expected)));
    }

    @Test
    @FixFor("DBZ-1413")
    public void shouldSnapshotDomainAliasWithProperModifiers() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE DOMAIN varbit2 AS varbit(3);");
        TestHelper.execute("CREATE TABLE alias_table (pk SERIAL, value varbit2 NOT NULL, PRIMARY KEY(pk));");
        TestHelper.execute("INSERT INTO alias_table (value) values (B'101');");

        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE)
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true));

        final TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        List<SchemaAndValueField> expected = Collections.singletonList(
                new SchemaAndValueField("value", Bits.builder(3).build(), new byte[]{ 5, 0 }));

        consumer.process(record -> assertReadRecord(record, Collect.hashMapOf("public.alias_table", expected)));
    }

    @Test(expected = ComparisonFailure.class)
    @FixFor("DBZ-1413")
    public void shouldNotSnapshotNestedDomainAliasTypeModifiersNotPropagated() throws Exception {
        // The pgjdbc driver does not currently provide support for type modifier resolution
        // when a domain type extends an existing domain type that extends a base type using
        // explicit type modifiers.
        TestHelper.execute("CREATE DOMAIN varbit2 AS varbit(3);");
        TestHelper.execute("CREATE DOMAIN varbit2b AS varbit2;");
        TestHelper.execute("CREATE TABLE alias_table (pk SERIAL, value varbit2b NOT NULL, PRIMARY KEY (pk));");
        TestHelper.execute("INSERT INTO alias_table (value) values (B'101');");

        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE)
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true));

        final TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        // We would normally expect that the value column was created with a length of 3.
        // However due to how the resolution works in the driver, it returns 2147483647.
        // We probably want to avoid supporting this behavior for now?
        List<SchemaAndValueField> expected = Collections.singletonList(
                new SchemaAndValueField("value", Bits.builder(3).build(), new byte[]{ 5, 0 }));

        consumer.process(record -> assertReadRecord(record, Collect.hashMapOf("public.alias_table", expected)));
    }

    @Test
    @FixFor("DBZ-920")
    public void shouldSnapshotEnumAsKnownType() throws Exception {
        TestHelper.execute("CREATE TYPE test_type AS ENUM ('V1', 'V2');");
        TestHelper.execute("CREATE TABLE enum_table (pk SERIAL, value test_type NOT NULL, primary key(pk));");
        TestHelper.execute("INSERT INTO enum_table (value) values ('V1');");

        // Specifically enable `column.propagate.source.type` here to validate later that the actual
        // type, length, and scale values are resolved correctly when paired with Enum types.
        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.TABLE_WHITELIST, "public.enum_table")
                .with("column.propagate.source.type", "public.enum_table.value"));

        final TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        List<SchemaAndValueField> expected = Collections.singletonList(
                new SchemaAndValueField("value", Enum.builder("V1,V2")
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "TEST_TYPE")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, String.valueOf(Integer.MAX_VALUE))
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                        .build(), "V1"));

        consumer.process(record -> assertReadRecord(record, Collect.hashMapOf("public.enum_table", expected)));
    }

    private void buildNoStreamProducer(Configuration.Builder config) {
        start(PostgresConnector.class, config
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build());
        assertConnectorIsRunning();
    }

    private void buildWithStreamProducer(Configuration.Builder config) {
        start(PostgresConnector.class, config
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build());
        assertConnectorIsRunning();
    }
}

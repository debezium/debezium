/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.data.Bits;
import io.debezium.data.Enum;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.util.Collect;

/**
 * Integration test for {@link RecordsSnapshotProducerIT}
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class RecordsSnapshotProducerIT extends AbstractRecordsProducerTest {

    @Rule
    public final SkipTestRule skip = new SkipTestRule();

    @Before
    public void before() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
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
        AtomicInteger totalCount = new AtomicInteger(0);
        consumer.process(record -> {
            assertReadRecord(record, expectedValuesByTopicName);
            assertSourceInfo(record);

            SnapshotRecord expected = expectedSnapshotRecordFromPosition(
                    totalCount.incrementAndGet(), expectedValuesByTopicName.size(),
                    1, 1);
            assertRecordOffsetAndSnapshotSource(record, expected);
        });
    }

    public static class CustomDatatypeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

        private SchemaBuilder isbnSchema;

        @Override
        public void configure(Properties props) {
            isbnSchema = SchemaBuilder.string().name(props.getProperty("schema.name"));
        }

        @Override
        public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
            if ("isbn".equals(column.typeName())) {
                registration.register(isbnSchema, x -> x.toString());
            }
        }
    }

    @Override
    protected List<SchemaAndValueField> schemasAndValuesForCustomConverterTypes() {
        return Arrays.asList(new SchemaAndValueField("i",
                SchemaBuilder.string().name("io.debezium.postgresql.type.Isbn").build(), "0-393-04002-X"));
    }

    @Test
    @FixFor("DBZ-1134")
    public void shouldUseCustomConverter() throws Exception {
        TestHelper.execute(INSERT_CUSTOM_TYPES_STMT);

        // then start the producer and validate all records are there
        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with("converters", "first")
                .with("first.type", CustomDatatypeConverter.class.getName())
                .with("first.schema.name", "io.debezium.postgresql.type.Isbn"));

        final TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValuesByTopicName = Collect.hashMapOf("public.custom_table", schemasAndValuesForCustomConverterTypes());
        consumer.process(record -> assertReadRecord(record, expectedValuesByTopicName));

        waitForSnapshotToBeCompleted();

        TestHelper.execute("CREATE TABLE conv_table (pk serial, i isbn NOT NULL, PRIMARY KEY(pk))");
        TestHelper.execute("INSERT INTO conv_table VALUES (default, '978-0-393-04002-9')");
        final Map<String, List<SchemaAndValueField>> expectedValuesByTopicName2 = Collect.hashMapOf("public.conv_table",
                Arrays.asList(new SchemaAndValueField("i",
                        SchemaBuilder.string().name("io.debezium.postgresql.type.Isbn").build(), "0-393-04002-X")));
        consumer.clear();
        consumer.expects(1);
        consumer.process(record -> assertReadRecord(record, expectedValuesByTopicName2));

        TestHelper.execute("ALTER TABLE conv_table ALTER COLUMN i TYPE varchar(32)");
        TestHelper.execute("INSERT INTO conv_table VALUES (default, '978-0-393-04002-9')");
        final Map<String, List<SchemaAndValueField>> expectedValuesByTopicName3 = Collect.hashMapOf("public.conv_table",
                Arrays.asList(new SchemaAndValueField("i",
                        Schema.STRING_SCHEMA, "0-393-04002-X")));
        consumer.clear();
        consumer.expects(1);
        consumer.process(record -> assertReadRecord(record, expectedValuesByTopicName3));
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
        assertRecordOffsetAndSnapshotSource(first, SnapshotRecord.FALSE);
        assertSourceInfo(first, TestHelper.TEST_DATABASE, "s1", "a");

        SourceRecord second = consumer.remove();
        VerifyRecord.isValidInsert(second, PK_FIELD, 2);
        assertEquals(topicName("s2.a"), second.topic());
        assertRecordOffsetAndSnapshotSource(second, SnapshotRecord.FALSE);
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
            SnapshotRecord expectedType = (counterVal % 3) == 0 ? SnapshotRecord.FIRST_IN_DATA_COLLECTION
                    : (counterVal % 3) == 1 ? SnapshotRecord.TRUE
                            : (counterVal == expectedRecordsCount - 1) ? SnapshotRecord.LAST : SnapshotRecord.LAST_IN_DATA_COLLECTION;
            assertRecordOffsetAndSnapshotSource(record, expectedType);
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
        assertRecordOffsetAndSnapshotSource(first, SnapshotRecord.FALSE);
        assertSourceInfo(first, TestHelper.TEST_DATABASE, "s1", "a");

        second = consumer.remove();
        VerifyRecord.isValidInsert(second, PK_FIELD, 4);
        assertRecordOffsetAndSnapshotSource(second, SnapshotRecord.FALSE);
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

        TestHelper.assertNoOpenTransactions();

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
        assertRecordOffsetAndSnapshotSource(first, SnapshotRecord.LAST);
        final SourceRecord second = consumer.remove();
        assertThat(second.topic()).startsWith("__debezium-heartbeat");
        assertRecordOffsetAndSnapshotSource(second, SnapshotRecord.FALSE);
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
        AtomicInteger totalCount = new AtomicInteger(0);
        consumer.process(record -> {
            assertReadRecord(record, expectedValuesByTopicName);
            assertSourceInfo(record);

            SnapshotRecord expected = expectedSnapshotRecordFromPosition(
                    totalCount.incrementAndGet(), expectedValuesByTopicName.size(),
                    1, 1);
            assertRecordOffsetAndSnapshotSource(record, expected);
        });
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
        consumer.process(record -> {
            assertReadRecord(record, expectedValuesByTopicName);
            assertSourceInfo(record);

            assertRecordOffsetAndSnapshotSource(record, SnapshotRecord.LAST);
        });
    }

    @Test
    @FixFor("DBZ-1118")
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 10, reason = "Database version is less than 10.0")
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
        Configuration.Builder config = TestHelper.defaultConfig();
        config.with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.first_table, public.partitioned_1_100, public.partitioned_101_200");
        buildNoStreamProducer(config);

        Set<Integer> ids = new HashSet<>();

        Map<String, Integer> expectedTopicCounts = Collect.hashMapOf(
                "test_server.public.first_table", 1,
                "test_server.public.partitioned_1_100", 10,
                "test_server.public.partitioned_101_200", 20);
        int expectedTotalCount = expectedTopicCounts.values().stream().mapToInt(Integer::intValue).sum();

        TestConsumer consumer = testConsumer(expectedTotalCount);
        consumer.await(TestHelper.waitTimeForRecords() * 30L, TimeUnit.SECONDS);

        Map<String, Integer> actualTopicCounts = new HashMap<>();
        AtomicInteger actualTotalCount = new AtomicInteger(0);

        consumer.process(record -> {
            assertSourceInfo(record);
            Struct key = (Struct) record.key();
            if (key != null) {
                final Integer id = key.getInt32("pk");
                assertThat(ids).doesNotContain(id);
                ids.add(id);
            }

            actualTopicCounts.put(record.topic(), actualTopicCounts.getOrDefault(record.topic(), 0) + 1);

            SnapshotRecord expected = expectedSnapshotRecordFromPosition(
                    actualTotalCount.incrementAndGet(), expectedTotalCount,
                    actualTopicCounts.get(record.topic()), expectedTopicCounts.get(record.topic()));
            assertRecordOffsetAndSnapshotSource(record, expected);
        });

        // verify distinct records
        assertEquals(expectedTotalCount, actualTotalCount.get());
        assertEquals(expectedTotalCount, ids.size());

        // verify each topic contains exactly the number of input records
        assertTrue("Expected counts per topic don't match", expectedTopicCounts.entrySet().containsAll(actualTopicCounts.entrySet()));
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
                new SchemaAndValueField("id", SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("val", Schema.OPTIONAL_INT32_SCHEMA, 1000));

        consumer.process(record -> {
            assertThat(record.key()).isNull();
            String actualTopicName = record.topic().replace(TestHelper.TEST_SERVER + ".", "");
            assertEquals("public.table_without_pk", actualTopicName);
            assertRecordSchemaAndValues(schemaAndValueFields, record, Envelope.FieldName.AFTER);
        });
    }

    @Test
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 10, reason = "MACADDR8 data type is only supported since Postgres 10+")
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
    @FixFor("DBZ-1755")
    public void shouldGenerateSnapshotForPositiveMoney() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");

        // insert money
        TestHelper.execute(INSERT_CASH_TYPES_STMT);

        buildNoStreamProducer(TestHelper.defaultConfig());

        TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValueByTopicName = Collect.hashMapOf("public.cash_table", schemaAndValuesForMoneyTypes());
        consumer.process(record -> assertReadRecord(record, expectedValueByTopicName));
    }

    @Test
    @FixFor("DBZ-1755")
    public void shouldGenerateSnapshotForNegativeMoney() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");

        // insert money
        TestHelper.execute(INSERT_NEGATIVE_CASH_TYPES_STMT);

        buildNoStreamProducer(TestHelper.defaultConfig().with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.cash_table"));

        TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValueByTopicName = Collect.hashMapOf("public.cash_table", schemaAndValuesForNegativeMoneyTypes());
        consumer.process(record -> assertReadRecord(record, expectedValueByTopicName));
    }

    @Test
    @FixFor("DBZ-1755")
    public void shouldGenerateSnapshotForNullMoney() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");

        // insert money
        TestHelper.execute(INSERT_NULL_CASH_TYPES_STMT);

        buildNoStreamProducer(TestHelper.defaultConfig().with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.cash_table"));

        TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValueByTopicName = Collect.hashMapOf("public.cash_table", schemaAndValuesForNullMoneyTypes());
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
                .with("column.propagate.source.type", "public.alias_table.*"));

        final TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField("salary", SchemaBuilder.float64().optional()
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "MONEY")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, String.valueOf(Integer.MAX_VALUE))
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                        .build(), 7.25),
                new SchemaAndValueField("salary2", SchemaBuilder.float64().optional()
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "MONEY2")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, String.valueOf(Integer.MAX_VALUE))
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                        .build(), 8.25),
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
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with("column.propagate.source.type", "public.alias_table.value"));

        final TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        List<SchemaAndValueField> expected = Collections.singletonList(
                new SchemaAndValueField("value", Bits.builder(3)
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "VARBIT2")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, "3")
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                        .build(), new byte[]{ 5 }));

        consumer.process(record -> assertReadRecord(record, Collect.hashMapOf("public.alias_table", expected)));
    }

    @Test
    @FixFor("DBZ-1413")
    public void shouldSnapshotDomainTypesLikeBaseTypes() throws Exception {
        TestHelper.dropAllSchemas();

        // Construct domain types
        // note: skipped macaddr8 as that is only supported on PG10+ but was manually tested
        TestHelper.execute("CREATE DOMAIN bit2 AS BIT(3);");
        TestHelper.execute("CREATE DOMAIN smallint2 AS smallint;");
        TestHelper.execute("CREATE DOMAIN integer2 as integer;");
        TestHelper.execute("CREATE DOMAIN bigint2 as bigint;");
        TestHelper.execute("CREATE DOMAIN real2 as real;");
        TestHelper.execute("CREATE DOMAIN bool2 AS BOOL DEFAULT false;");
        TestHelper.execute("CREATE DOMAIN float82 as float8;");
        TestHelper.execute("CREATE DOMAIN numeric2 as numeric(6,2);");
        TestHelper.execute("CREATE DOMAIN string2 AS varchar(25) DEFAULT NULL;");
        TestHelper.execute("CREATE DOMAIN date2 AS date;");
        TestHelper.execute("CREATE DOMAIN time2 as time;");
        TestHelper.execute("CREATE DOMAIN timetz2 as timetz;");
        TestHelper.execute("CREATE DOMAIN timestamp2 as timestamp;");
        TestHelper.execute("CREATE DOMAIN timestamptz2 AS timestamptz;");
        TestHelper.execute("CREATE DOMAIN timewotz2 as time without time zone;");
        TestHelper.execute("CREATE DOMAIN box2 as box;");
        TestHelper.execute("CREATE DOMAIN circle2 as circle;");
        TestHelper.execute("CREATE DOMAIN interval2 as interval;");
        TestHelper.execute("CREATE DOMAIN line2 as line;");
        TestHelper.execute("CREATE DOMAIN lseg2 as lseg;");
        TestHelper.execute("CREATE DOMAIN path2 as path;");
        TestHelper.execute("CREATE DOMAIN point2 as point;");
        TestHelper.execute("CREATE DOMAIN polygon2 as polygon;");
        TestHelper.execute("CREATE DOMAIN char2 as char;");
        TestHelper.execute("CREATE DOMAIN text2 as text;");
        TestHelper.execute("CREATE DOMAIN json2 as json;");
        TestHelper.execute("CREATE DOMAIN xml2 as xml;");
        TestHelper.execute("CREATE DOMAIN uuid2 as uuid;");
        TestHelper.execute("CREATE DOMAIN varbit2 as varbit(3);");
        TestHelper.execute("CREATE DOMAIN inet2 as inet;");
        TestHelper.execute("CREATE DOMAIN cidr2 as cidr;");
        TestHelper.execute("CREATE DOMAIN macaddr2 as macaddr;");

        // Create table
        TestHelper.execute("CREATE TABLE alias_table (pk SERIAL" +
                ", bit_base bit(3) NOT NULL, bit_alias bit2 NOT NULL" +
                ", smallint_base smallint NOT NULL, smallint_alias smallint2 NOT NULL" +
                ", integer_base integer NOT NULL, integer_alias integer2 NOT NULL" +
                ", bigint_base bigint NOT NULL, bigint_alias bigint2 NOT NULL" +
                ", real_base real NOT NULL, real_alias real2 NOT NULL" +
                ", float8_base float8 NOT NULL, float8_alias float82 NOT NULL" +
                ", numeric_base numeric(6,2) NOT NULL, numeric_alias numeric2 NOT NULL" +
                ", bool_base bool NOT NULL, bool_alias bool2 NOT NULL" +
                ", string_base varchar(25) NOT NULL, string_alias string2 NOT NULL" +
                ", date_base date NOT NULL, date_alias date2 NOT NULL" +
                ", time_base time NOT NULL, time_alias time2 NOT NULL" +
                ", timetz_base timetz NOT NULL, timetz_alias timetz2 NOT NULL" +
                ", timestamp_base timestamp NOT NULL, timestamp_alias timestamp2 NOT NULL" +
                ", timestamptz_base timestamptz NOT NULL, timestamptz_alias timestamptz2 NOT NULL" +
                ", timewottz_base time without time zone NOT NULL, timewottz_alias timewotz2 NOT NULL" +
                ", box_base box NOT NULL, box_alias box2 NOT NULL" +
                ", circle_base circle NOT NULL, circle_alias circle2 NOT NULL" +
                ", interval_base interval NOT NULL, interval_alias interval2 NOT NULL" +
                ", line_base line NOT NULL, line_alias line2 NOT NULL" +
                ", lseg_base lseg NOT NULL, lseg_alias lseg2 NOT NULL" +
                ", path_base path NOT NULL, path_alias path2 NOT NULL" +
                ", point_base point NOT NULL, point_alias point2 NOT NULL" +
                ", polygon_base polygon NOT NULL, polygon_alias polygon2 NOT NULL" +
                ", char_base char NOT NULL, char_alias char2 NOT NULL" +
                ", text_base text NOT NULL, text_alias text2 NOT NULL" +
                ", json_base json NOT NULL, json_alias json2 NOT NULL" +
                ", xml_base xml NOT NULL, xml_alias xml2 NOT NULL" +
                ", uuid_base UUID NOT NULL, uuid_alias uuid2 NOT NULL" +
                ", varbit_base varbit(3) NOT NULL, varbit_alias varbit2 NOT NULL" +
                ", inet_base inet NOT NULL, inet_alias inet2 NOT NULL" +
                ", cidr_base cidr NOT NULL, cidr_alias cidr2 NOT NULL" +
                ", macaddr_base macaddr NOT NULL, macaddr_alias macaddr2 NOT NULL" +
                ", PRIMARY KEY(pk));");

        // Insert the one row we want to snapshot
        TestHelper.execute("INSERT INTO alias_table (" +
                "bit_base, bit_alias, " +
                "smallint_base, smallint_alias, " +
                "integer_base, integer_alias, " +
                "bigint_base, bigint_alias, " +
                "real_base, real_alias, " +
                "float8_base, float8_alias, " +
                "numeric_base, numeric_alias, " +
                "bool_base, bool_alias, " +
                "string_base, string_alias, " +
                "date_base, date_alias, " +
                "time_base, time_alias, " +
                "timetz_base, timetz_alias, " +
                "timestamp_base, timestamp_alias, " +
                "timestamptz_base, timestamptz_alias, " +
                "timewottz_base, timewottz_alias, " +
                "box_base, box_alias, " +
                "circle_base, circle_alias, " +
                "interval_base, interval_alias, " +
                "line_base, line_alias, " +
                "lseg_base, lseg_alias, " +
                "path_base, path_alias, " +
                "point_base, point_alias, " +
                "polygon_base, polygon_alias, " +
                "char_base, char_alias, " +
                "text_base, text_alias, " +
                "json_base, json_alias, " +
                "xml_base, xml_alias, " +
                "uuid_base, uuid_alias, " +
                "varbit_base, varbit_alias, " +
                "inet_base, inet_alias, " +
                "cidr_base, cidr_alias, " +
                "macaddr_base, macaddr_alias " +
                ") VALUES (" +
                "B'101', B'101', " +
                "1, 1, " +
                "1, 1, " +
                "1000, 1000, " +
                "3.14, 3.14, " +
                "3.14, 3.14, " +
                "1234.12, 1234.12, " +
                "true, true, " +
                "'hello', 'hello', " +
                "'2019-10-02', '2019-10-02', " +
                "'01:02:03', '01:02:03', " +
                "'01:02:03.123789Z', '01:02:03.123789Z', " +
                "'2019-10-02T01:02:03.123456', '2019-10-02T01:02:03.123456', " +
                "'2019-10-02T13:51:30.123456+02:00'::TIMESTAMPTZ, '2019-10-02T13:51:30.123456+02:00'::TIMESTAMPTZ, " +
                "'01:02:03', '01:02:03', " +
                "'(0,0),(1,1)', '(0,0),(1,1)', " +
                "'10,4,10', '10,4,10', " +
                "'1 year 2 months 3 days 4 hours 5 minutes 6 seconds', '1 year 2 months 3 days 4 hours 5 minutes 6 seconds', " +
                "'(0,0),(0,1)', '(0,0),(0,1)', " +
                "'((0,0),(0,1))', '((0,0),(0,1))', " +
                "'((0,0),(0,1),(0,2))', '((0,0),(0,1),(0,2))', " +
                "'(1,1)', '(1,1)', " +
                "'((0,0),(0,1),(1,0),(0,0))', '((0,0),(0,1),(1,0),(0,0))', " +
                "'a', 'a', " +
                "'Hello World', 'Hello World', " +
                "'{\"key\": \"value\"}', '{\"key\": \"value\"}', " +
                "XML('<foo>Hello</foo>'), XML('<foo>Hello</foo>'), " +
                "'40e6215d-b5c6-4896-987c-f30f3678f608', '40e6215d-b5c6-4896-987c-f30f3678f608', " +
                "B'101', B'101', " +
                "'192.168.0.1', '192.168.0.1', " +
                "'192.168/24', '192.168/24', " +
                "'08:00:2b:01:02:03', '08:00:2b:01:02:03' " +
                ");");

        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE)
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.alias_table"));

        final TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final List<SchemaAndValueField> expected = schemasAndValuesForDomainAliasTypes(false);
        consumer.process(record -> assertReadRecord(record, Collect.hashMapOf("public.alias_table", expected)));
    }

    @FixFor("DBZ-1413")
    public void shouldSnapshotNestedDomainAliasTypeModifiersNotPropagated() throws Exception {
        TestHelper.execute("CREATE DOMAIN varbit2 AS varbit(3);");
        TestHelper.execute("CREATE DOMAIN varbit2b AS varbit2;");
        TestHelper.execute("CREATE TABLE alias_table (pk SERIAL, value varbit2b NOT NULL, PRIMARY KEY (pk));");
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
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.enum_table")
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

    @Test
    @FixFor("DBZ-1969")
    public void shouldSnapshotEnumArrayAsKnownType() throws Exception {
        TestHelper.execute("CREATE TYPE test_type AS ENUM ('V1', 'V2');");
        TestHelper.execute("CREATE TABLE enum_array_table (pk SERIAL, value test_type[] NOT NULL, primary key(pk));");
        TestHelper.execute("INSERT INTO enum_array_table (value) values ('{V1, V2}');");

        // Specifically enable `column.propagate.source.type` here to validate later that the actual
        // type, length, and scale values are resolved correctly when paired with Enum types.
        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, false)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.enum_array_table")
                .with("column.propagate.source.type", "public.enum_array_table.value"));

        final TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        List<SchemaAndValueField> expected = Collections.singletonList(
                new SchemaAndValueField("value", SchemaBuilder.array(Enum.builder("V1,V2"))
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "_TEST_TYPE")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, String.valueOf(Integer.MAX_VALUE))
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                        .build(), Arrays.asList("V1", "V2")));

        consumer.process(record -> assertReadRecord(record, Collect.hashMapOf("public.enum_array_table", expected)));
    }

    @Test
    @FixFor("DBZ-1969")
    public void shouldSnapshotTimeArrayTypesAsKnownTypes() throws Exception {
        TestHelper.execute("CREATE TABLE time_array_table (pk SERIAL, "
                + "timea time[] NOT NULL, "
                + "timetza timetz[] NOT NULL, "
                + "timestampa timestamp[] NOT NULL, "
                + "timestamptza timestamptz[] NOT NULL, primary key(pk));");
        TestHelper.execute("INSERT INTO time_array_table (timea, timetza, timestampa, timestamptza) "
                + "values ("
                + "'{00:01:02,01:02:03}', "
                + "'{13:51:02+0200,14:51:03+0200}', "
                + "'{2020-04-01 00:01:02,2020-04-01 01:02:03}', "
                + "'{2020-04-01 13:51:02+0200,2020-04-01 14:51:03+0200}')");

        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, false)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.time_array_table"));

        final TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        consumer.process(record -> assertReadRecord(record, Collect.hashMapOf("public.time_array_table", schemaAndValuesForTimeArrayTypes())));
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldGenerateSnapshotForByteaAsBytes() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute(INSERT_BYTEA_BINMODE_STMT);

        buildNoStreamProducer(TestHelper.defaultConfig());

        TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValueByTopicName = Collect.hashMapOf("public.bytea_binmode_table", schemaAndValueForByteaBytes());

        consumer.process(record -> assertReadRecord(record, expectedValueByTopicName));
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldGenerateSnapshotForByteaAsBase64String() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute(INSERT_BYTEA_BINMODE_STMT);

        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.BINARY_HANDLING_MODE, PostgresConnectorConfig.BinaryHandlingMode.BASE64));

        TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValueByTopicName = Collect.hashMapOf("public.bytea_binmode_table", schemaAndValueForByteaBase64());

        consumer.process(record -> assertReadRecord(record, expectedValueByTopicName));
    }

    @Test
    @FixFor("DBZ-5544")
    public void shouldGenerateSnapshotForByteaAsBase64UrlSafeString() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute(INSERT_BYTEA_BINMODE_STMT);

        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.BINARY_HANDLING_MODE, PostgresConnectorConfig.BinaryHandlingMode.BASE64_URL_SAFE));

        TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValueByTopicName = Collect.hashMapOf("public.bytea_binmode_table", schemaAndValueForByteaBase64UrlSafe());

        consumer.process(record -> assertReadRecord(record, expectedValueByTopicName));
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldGenerateSnapshotForByteaAsHexString() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute(INSERT_BYTEA_BINMODE_STMT);

        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.BINARY_HANDLING_MODE, PostgresConnectorConfig.BinaryHandlingMode.HEX));

        TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValueByTopicName = Collect.hashMapOf("public.bytea_binmode_table", schemaAndValueForByteaHex());

        consumer.process(record -> assertReadRecord(record, expectedValueByTopicName));
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldGenerateSnapshotForUnknownColumnAsBytes() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute(INSERT_CIRCLE_STMT);

        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true));

        TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValueByTopicName = Collect.hashMapOf("public.circle_table", schemaAndValueForUnknownColumnBytes());

        consumer.process(record -> assertReadRecord(record, expectedValueByTopicName));
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldGenerateSnapshotForUnknownColumnAsBase64() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute(INSERT_CIRCLE_STMT);

        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.BINARY_HANDLING_MODE, BinaryHandlingMode.BASE64));

        TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValueByTopicName = Collect.hashMapOf("public.circle_table", schemaAndValueForUnknownColumnBase64());

        consumer.process(record -> assertReadRecord(record, expectedValueByTopicName));
    }

    @Test
    @FixFor("DBZ-5544")
    public void shouldGenerateSnapshotForUnknownColumnAsBase64UrlSafe() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute(INSERT_CIRCLE_STMT);

        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.BINARY_HANDLING_MODE, BinaryHandlingMode.BASE64_URL_SAFE));

        TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValueByTopicName = Collect.hashMapOf("public.circle_table", schemaAndValueForUnknownColumnBase64UrlSafe());

        consumer.process(record -> assertReadRecord(record, expectedValueByTopicName));
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldGenerateSnapshotForUnknownColumnAsHex() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute(INSERT_CIRCLE_STMT);

        buildNoStreamProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.BINARY_HANDLING_MODE, BinaryHandlingMode.HEX));

        TestConsumer consumer = testConsumer(1, "public");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        final Map<String, List<SchemaAndValueField>> expectedValueByTopicName = Collect.hashMapOf("public.circle_table", schemaAndValueForUnknownColumnHex());

        consumer.process(record -> assertReadRecord(record, expectedValueByTopicName));
    }

    @Test
    @FixFor("DBZ-5240")
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 11, reason = "Primary keys on partitioned tables are supported only on Postgres 11+")
    public void shouldIncludePartitionedTableIntoSnapshot() throws Exception {

        // create partitioned table
        TestHelper.dropAllSchemas();
        TestHelper.execute(
                "CREATE SCHEMA s1;"
                        + "CREATE TABLE s1.part (pk SERIAL, aa integer, PRIMARY KEY(pk, aa)) PARTITION BY RANGE (aa);"
                        + "CREATE TABLE s1.part1 PARTITION OF s1.part FOR VALUES FROM (0) TO (500);"
                        + "CREATE TABLE s1.part2 PARTITION OF s1.part FOR VALUES FROM (500) TO (1000);");

        // insert records
        TestHelper.execute("INSERT into s1.part VALUES(1, 1)");
        TestHelper.execute("INSERT into s1.part VALUES(2, 2)");
        TestHelper.execute("INSERT into s1.part VALUES(3, 700)");
        TestHelper.execute("INSERT into s1.part VALUES(4, 800)");

        // start connector
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY.getValue())
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.part");
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        // check the records from the snapshot
        final int expectedCount = 4;
        final int[] expectedPks = { 1, 2, 3, 4 };
        SourceRecords actualRecords = consumeRecordsByTopic(expectedCount);
        assertThat(actualRecords.allRecordsInOrder().size()).isEqualTo(expectedCount);
        List<SourceRecord> recordsForTopicPart = actualRecords.recordsForTopic(topicName("s1.part"));
        assertThat(recordsForTopicPart.size()).isEqualTo(expectedCount);
        IntStream.range(0, expectedCount)
                .forEach(i -> VerifyRecord.isValidRead(recordsForTopicPart.remove(0), PK_FIELD, expectedPks[i]));
    }

    private void buildNoStreamProducer(Configuration.Builder config) {
        alterConfig(config);
        start(PostgresConnector.class, config
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build());
        assertConnectorIsRunning();
    }

    private void buildWithStreamProducer(Configuration.Builder config) {
        alterConfig(config);
        start(PostgresConnector.class, config
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, CustomTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build());
        assertConnectorIsRunning();
    }

    protected void alterConfig(Builder config) {

    }
}

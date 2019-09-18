/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIs.DecoderPluginName.DECODERBUFS;
import static io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIs.DecoderPluginName.PGOUTPUT;
import static io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIsNot.DecoderPluginName.WAL2JSON;
import static junit.framework.TestCase.assertEquals;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertFalse;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SchemaRefreshMode;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.junit.SkipTestDependingOnDecoderPluginNameRule;
import io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIs;
import io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIsNot;
import io.debezium.data.Envelope;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.junit.ConditionalFail;
import io.debezium.junit.ShouldFailWhen;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Stopwatch;
import io.debezium.util.Testing;

/**
 * Integration test for the {@link RecordsStreamProducer} class. This also tests indirectly the PG plugin functionality for
 * different use cases.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class RecordsStreamProducerIT extends AbstractRecordsProducerTest {

    private TestConsumer consumer;

    @Rule
    public final TestRule skip = new SkipTestDependingOnDecoderPluginNameRule();

    @Rule
    public TestRule conditionalFail = new ConditionalFail();

    @Before
    public void before() throws Exception {
        // ensure the slot is deleted for each test
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("init_postgis.ddl");
        String statements =
                "CREATE SCHEMA IF NOT EXISTS public;" +
                "DROP TABLE IF EXISTS test_table;" +
                "CREATE TABLE test_table (pk SERIAL, text TEXT, PRIMARY KEY(pk));" +
                "CREATE TABLE table_with_interval (id SERIAL PRIMARY KEY, title VARCHAR(512) NOT NULL, time_limit INTERVAL DEFAULT '60 days'::INTERVAL NOT NULL);" +
                "INSERT INTO test_table(text) VALUES ('insert');";
        TestHelper.execute(statements);

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, false)
                .with(PostgresConnectorConfig.SCHEMA_BLACKLIST, "postgis");

        // todo DBZ-766 are these really needed?
        if (TestHelper.decoderPlugin() == PostgresConnectorConfig.LogicalDecoder.PGOUTPUT) {
            configBuilder = configBuilder.with("database.replication", "database")
                    .with("database.preferQueryMode", "simple")
                    .with("assumeMinServerVersion.set", "9.4");
        }

        Testing.Print.enable();
    }

    private void startConnector(Function<Configuration.Builder, Configuration.Builder> customConfig, boolean waitForSnapshot) throws InterruptedException {
        start(PostgresConnector.class, new PostgresConnectorConfig(customConfig.apply(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, false)
                .with(PostgresConnectorConfig.SCHEMA_BLACKLIST, "postgis")
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, waitForSnapshot ? SnapshotMode.INITIAL : SnapshotMode.NEVER))
                .build()).getConfig()
        );
        assertConnectorIsRunning();
        waitForStreamingToStart();

        if (waitForSnapshot) {
            // Wait for snapshot to be in progress
            consumer = testConsumer(1);
            consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
            consumer.remove();
        }
    }

    private void startConnector(Function<Configuration.Builder, Configuration.Builder> customConfig) throws InterruptedException {
        startConnector(customConfig, true);
    }

    private void startConnector() throws InterruptedException {
        startConnector(Function.identity(), true);
    }

    @Test
    public void shouldReceiveChangesForInsertsWithDifferentDataTypes() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");
        startConnector();

        consumer = testConsumer(1);

        //numerical types
        consumer.expects(1);
        assertInsert(INSERT_NUMERIC_TYPES_STMT, 1, schemasAndValuesForNumericType());

        //numerical decimal types
        consumer.expects(1);
        assertInsert(INSERT_NUMERIC_DECIMAL_TYPES_STMT_NO_NAN, 1, schemasAndValuesForBigDecimalEncodedNumericTypes());

        // string types
        consumer.expects(1);
        assertInsert(INSERT_STRING_TYPES_STMT, 1, schemasAndValuesForStringTypes());

        // monetary types
        consumer.expects(1);
        assertInsert(INSERT_CASH_TYPES_STMT, 1, schemaAndValuesForMoneyTypes());

        // bits and bytes
        consumer.expects(1);
        assertInsert(INSERT_BIN_TYPES_STMT, 1, schemaAndValuesForBinTypes());

        //date and time
        consumer.expects(1);
        assertInsert(INSERT_DATE_TIME_TYPES_STMT, 1, schemaAndValuesForDateTimeTypes());

        // text
        consumer.expects(1);
        assertInsert(INSERT_TEXT_TYPES_STMT, 1, schemasAndValuesForTextTypes());

        // geom types
        consumer.expects(1);
        assertInsert(INSERT_GEOM_TYPES_STMT, 1, schemaAndValuesForGeomTypes());

        // range types
        consumer.expects(1);
        assertInsert(INSERT_RANGE_TYPES_STMT, 1, schemaAndValuesForRangeTypes());
    }

    @Test
    @FixFor("DBZ-766")
    public void shouldReceiveChangesAfterConnectionRestart() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();

        startConnector(config -> config
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SCHEMA_BLACKLIST, "postgis")
        );

        TestHelper.execute("CREATE TABLE t0 (pk SERIAL, d INTEGER, PRIMARY KEY(pk));");

        consumer = testConsumer(1);
        waitForStreamingToStart();

        // Insert new row and verify inserted
        executeAndWait("INSERT INTO t0 (pk,d) VALUES(1,1);");
        assertRecordInserted("public.t0", PK_FIELD, 1);

        // simulate the connector is stopped
        stopConnector();

        // Alter schema offline
        TestHelper.execute("ALTER TABLE t0 ADD COLUMN d2 INTEGER;");
        TestHelper.execute("ALTER TABLE t0 ALTER COLUMN d SET NOT NULL;");


        // Start the producer and wait; the wait is to guarantee the stream thread is polling
        // This appears to be a potential race condition problem
        startConnector(config -> config
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SCHEMA_BLACKLIST, "postgis"),
                false
        );
        consumer = testConsumer(1);
        waitForStreamingToStart();

        // Insert new row and verify inserted
        executeAndWait("INSERT INTO t0 (pk,d,d2) VALUES (2,1,3);");
        assertRecordInserted("public.t0", PK_FIELD, 2);
    }

    @Test
    public void shouldReceiveChangesForInsertsCustomTypes() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true));
        // custom types + null value
        assertInsert(INSERT_CUSTOM_TYPES_STMT, 1, schemasAndValuesForCustomTypes());
    }

    @Test
    @FixFor("DBZ-1141")
    public void shouldProcessNotNullColumnsConnectDateTypes() throws Exception {
        final Struct before = testProcessNotNullColumns(TemporalPrecisionMode.CONNECT);
        if (before != null) {
            Assertions.assertThat(before.get("created_at")).isEqualTo(new java.util.Date(0));
            Assertions.assertThat(before.get("created_at_tz")).isEqualTo("1970-01-01T00:00:00Z");
            Assertions.assertThat(before.get("ctime")).isEqualTo(new java.util.Date(0));
            Assertions.assertThat(before.get("ctime_tz")).isEqualTo("00:00:00Z");
            Assertions.assertThat(before.get("cdate")).isEqualTo(new java.util.Date(0));
            Assertions.assertThat(before.get("cmoney")).isEqualTo(new BigDecimal("0.00"));
            Assertions.assertThat(before.get("cbits")).isEqualTo(new byte[0]);
        }
    }

    @Test
    @FixFor("DBZ-1141")
    public void shouldProcessNotNullColumnsAdaptiveDateTypes() throws Exception {
        final Struct before = testProcessNotNullColumns(TemporalPrecisionMode.ADAPTIVE);
        if (before != null) {
            Assertions.assertThat(before.get("created_at")).isEqualTo(0L);
            Assertions.assertThat(before.get("created_at_tz")).isEqualTo("1970-01-01T00:00:00Z");
            Assertions.assertThat(before.get("ctime")).isEqualTo(0L);
            Assertions.assertThat(before.get("ctime_tz")).isEqualTo("00:00:00Z");
            Assertions.assertThat(before.get("cdate")).isEqualTo(0);
            Assertions.assertThat(before.get("cmoney")).isEqualTo(new BigDecimal("0.00"));
            Assertions.assertThat(before.get("cbits")).isEqualTo(new byte[0]);
        }
    }

    @Test
    @FixFor("DBZ-1141")
    public void shouldProcessNotNullColumnsAdaptiveMsDateTypes() throws Exception {
        final Struct before = testProcessNotNullColumns(TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        if (before != null) {
            Assertions.assertThat(before.get("created_at")).isEqualTo(0L);
            Assertions.assertThat(before.get("created_at_tz")).isEqualTo("1970-01-01T00:00:00Z");
            Assertions.assertThat(before.get("ctime")).isEqualTo(0L);
            Assertions.assertThat(before.get("ctime_tz")).isEqualTo("00:00:00Z");
            Assertions.assertThat(before.get("cdate")).isEqualTo(0);
            Assertions.assertThat(before.get("cmoney")).isEqualTo(new BigDecimal("0.00"));
            Assertions.assertThat(before.get("cbits")).isEqualTo(new byte[0]);
        }
    }

    private Struct testProcessNotNullColumns(TemporalPrecisionMode temporalMode) throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SCHEMA_BLACKLIST, "postgis")
                .with(PostgresConnectorConfig.TIME_PRECISION_MODE, temporalMode)
        );

        consumer.expects(1);
        executeAndWait("INSERT INTO not_null_table VALUES (default, 30, '2019-02-10 11:34:58', '2019-02-10 11:35:00', '10:20:11', '10:20:12', '2019-02-01', '$20', B'101')");
        consumer.remove();

        consumer.expects(1);
        executeAndWait("UPDATE not_null_table SET val=40");
        final SourceRecord record = consumer.remove();
        VerifyRecord.isValidUpdate(record, "pk", 1);
        VerifyRecord.isValid(record);
        return ((Struct) record.value()).getStruct("before");
    }

    @Test(timeout = 30000)
    public void shouldReceiveChangesForInsertsWithPostgisTypes() throws Exception {
        TestHelper.executeDDL("postgis_create_tables.ddl");

        startConnector();
        consumer = testConsumer(1, "public"); // spatial_ref_sys produces a ton of records in the postgis schema
        consumer.setIgnoreExtraRecords(true);

        // need to wait for all the spatial_ref_sys to flow through and be ignored.
        // this exceeds the normal 2s timeout.
        TestHelper.execute("INSERT INTO public.dummy_table DEFAULT VALUES;");
        consumer.await(TestHelper.waitTimeForRecords() * 10, TimeUnit.SECONDS);
        while (true) {
            if (!consumer.isEmpty()) {
                SourceRecord record = consumer.remove();
                if (record.topic().endsWith(".public.dummy_table")) {
                    break;
                }
            }
        }

        // now do it for actual testing
        // postgis types
        consumer.expects(1);
        assertInsert(INSERT_POSTGIS_TYPES_STMT, 1, schemaAndValuesForPostgisTypes());
    }

    @Test(timeout = 30000)
    public void shouldReceiveChangesForInsertsWithPostgisArrayTypes() throws Exception {
        TestHelper.executeDDL("postgis_create_tables.ddl");

        startConnector();
        consumer = testConsumer(1, "public"); // spatial_ref_sys produces a ton of records in the postgis schema
        consumer.setIgnoreExtraRecords(true);

        // need to wait for all the spatial_ref_sys to flow through and be ignored.
        // this exceeds the normal 2s timeout.
        TestHelper.execute("INSERT INTO public.dummy_table DEFAULT VALUES;");
        consumer.await(TestHelper.waitTimeForRecords() * 10, TimeUnit.SECONDS);
        while (true) {
            if (!consumer.isEmpty()) {
                SourceRecord record = consumer.remove();
                if (record.topic().endsWith(".public.dummy_table")) {
                    break;
                }
            }
        }

        // now do it for actual testing
        // postgis types
        consumer.expects(1);
        assertInsert(INSERT_POSTGIS_ARRAY_TYPES_STMT, 1, schemaAndValuesForPostgisArrayTypes());
    }

    @Test
    @ShouldFailWhen(DecoderDifferences.AreQuotedIdentifiersUnsupported.class)
    // TODO DBZ-493
    public void shouldReceiveChangesForInsertsWithQuotedNames() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector();

        // Quoted column name
        assertInsert(INSERT_QUOTED_TYPES_STMT, 1, schemasAndValuesForQuotedTypes());
    }

    @Test
    public void shouldReceiveChangesForInsertsWithArrayTypes() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector();

        assertInsert(INSERT_ARRAY_TYPES_STMT, 1, schemasAndValuesForArrayTypes());
    }

    @Test
    @FixFor("DBZ-1029")
    public void shouldReceiveChangesForInsertsIndependentOfReplicaIdentity() throws Exception {
        // insert statement should not be affected by replica identity settings in any way

        startConnector();

        TestHelper.execute("ALTER TABLE test_table REPLICA IDENTITY DEFAULT;");
        String statement = "INSERT INTO test_table (text) VALUES ('pk_and_default');";
        assertInsert(statement, 2, Collections.singletonList(new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "pk_and_default")));

        consumer.expects(1);
        TestHelper.execute("ALTER TABLE test_table REPLICA IDENTITY FULL;");
        statement = "INSERT INTO test_table (text) VALUES ('pk_and_full');";
        assertInsert(statement, 3, Collections.singletonList(new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "pk_and_full")));

        consumer.expects(1);
        TestHelper.execute("ALTER TABLE test_table DROP CONSTRAINT test_table_pkey CASCADE;");
        statement = "INSERT INTO test_table (pk, text) VALUES (4, 'no_pk_and_full');";
        assertInsert(statement, 4, Collections.singletonList(new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "no_pk_and_full")));

        consumer.expects(1);
        TestHelper.execute("ALTER TABLE test_table REPLICA IDENTITY DEFAULT;");
        statement = "INSERT INTO test_table (pk, text) VALUES (5, 'no_pk_and_default');";
        assertInsert(statement, 5, Collections.singletonList(new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "no_pk_and_default")));
    }

    @Test
    @FixFor("DBZ-478")
    public void shouldReceiveChangesForNullInsertsWithArrayTypes() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector();

        assertInsert(INSERT_ARRAY_TYPES_WITH_NULL_VALUES_STMT, 1, schemasAndValuesForArrayTypesWithNullValues());
    }

    @Test
    public void shouldReceiveChangesForNewTable() throws Exception {
        String statement = "CREATE SCHEMA s1;" +
                           "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                           "INSERT INTO s1.a (aa) VALUES (11);";

        startConnector();

        executeAndWait(statement);
        assertRecordInserted("s1.a", PK_FIELD, 1);
    }

    @Test
    public void shouldReceiveChangesForRenamedTable() throws Exception {
        String statement = "DROP TABLE IF EXISTS renamed_test_table;" +
                           "ALTER TABLE test_table RENAME TO renamed_test_table;" +
                           "INSERT INTO renamed_test_table (text) VALUES ('new');";
        startConnector();

        executeAndWait(statement);
        assertRecordInserted("public.renamed_test_table", PK_FIELD, 2);
    }

    @Test
    @SkipWhenDecoderPluginNameIs(value = PGOUTPUT, reason = "An update on a table with no primary key and default replica throws PSQLException as tables must have a PK")
    public void shouldReceiveChangesForUpdates() throws Exception {
        startConnector();
        executeAndWait("UPDATE test_table set text='update' WHERE pk=1");

        // the update record should be the last record
        SourceRecord updatedRecord = consumer.remove();
        String topicName = topicName("public.test_table");
        assertEquals(topicName, updatedRecord.topic());
        VerifyRecord.isValidUpdate(updatedRecord, PK_FIELD, 1);

        // default replica identity only fires previous values for PK changes
        List<SchemaAndValueField> expectedAfter = Collections.singletonList(
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "update"));
        assertRecordSchemaAndValues(expectedAfter, updatedRecord, Envelope.FieldName.AFTER);

        // alter the table and set its replica identity to full the issue another update
        consumer.expects(1);
        TestHelper.execute("ALTER TABLE test_table REPLICA IDENTITY FULL");
        executeAndWait("UPDATE test_table set text='update2' WHERE pk=1");

        updatedRecord = consumer.remove();
        assertEquals(topicName, updatedRecord.topic());
        VerifyRecord.isValidUpdate(updatedRecord, PK_FIELD, 1);

        // now we should get both old and new values
        List<SchemaAndValueField> expectedBefore = Collections.singletonList(new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "update"));
        assertRecordSchemaAndValues(expectedBefore, updatedRecord, Envelope.FieldName.BEFORE);

        expectedAfter = Collections.singletonList(new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "update2"));
        assertRecordSchemaAndValues(expectedAfter, updatedRecord, Envelope.FieldName.AFTER);

        // without PK and with REPLICA IDENTITY FULL we still getting all fields 'before' and all fields 'after'
        TestHelper.execute("ALTER TABLE test_table DROP CONSTRAINT test_table_pkey CASCADE;");
        consumer.expects(1);
        executeAndWait("UPDATE test_table SET text = 'update3' WHERE pk = 1;");
        updatedRecord = consumer.remove();
        assertEquals(topicName, updatedRecord.topic());

        expectedBefore = Collections.singletonList(new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA,  "update2"));
        assertRecordSchemaAndValues(expectedBefore, updatedRecord, Envelope.FieldName.BEFORE);

        expectedAfter = Collections.singletonList(new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA,  "update3"));
        assertRecordSchemaAndValues(expectedAfter, updatedRecord, Envelope.FieldName.AFTER);

        // without PK and with REPLICA IDENTITY DEFAULT we will get nothing
        TestHelper.execute("ALTER TABLE test_table REPLICA IDENTITY DEFAULT;");
        consumer.expects(0);
        executeAndWaitForNoRecords("UPDATE test_table SET text = 'no_pk_and_default' WHERE pk = 1;");
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    public void shouldReceiveChangesForUpdatesWithColumnChanges() throws Exception {
        // add a new column
        String statements = "ALTER TABLE test_table ADD COLUMN uvc VARCHAR(2);" +
                            "ALTER TABLE test_table REPLICA IDENTITY FULL;" +
                            "UPDATE test_table SET uvc ='aa' WHERE pk = 1;";

        startConnector();
        consumer = testConsumer(1);
        executeAndWait(statements);

        // the update should be the last record
        SourceRecord updatedRecord = consumer.remove();
        String topicName = topicName("public.test_table");
        assertEquals(topicName, updatedRecord.topic());
        VerifyRecord.isValidUpdate(updatedRecord, PK_FIELD, 1);

        // now check we got the updated value (the old value should be null, the new one whatever we set)
        List<SchemaAndValueField> expectedBefore = Collections.singletonList(new SchemaAndValueField("uvc", null, null));
        assertRecordSchemaAndValues(expectedBefore, updatedRecord, Envelope.FieldName.BEFORE);

        List<SchemaAndValueField> expectedAfter = Collections.singletonList(new SchemaAndValueField("uvc", SchemaBuilder.OPTIONAL_STRING_SCHEMA,
                                                                                           "aa"));
        assertRecordSchemaAndValues(expectedAfter, updatedRecord, Envelope.FieldName.AFTER);

        // rename a column
        statements = "ALTER TABLE test_table RENAME COLUMN uvc to xvc;" +
                     "UPDATE test_table SET xvc ='bb' WHERE pk = 1;";

        consumer.expects(1);
        executeAndWait(statements);

        updatedRecord = consumer.remove();
        VerifyRecord.isValidUpdate(updatedRecord, PK_FIELD, 1);

        // now check we got the updated value (the old value should be null, the new one whatever we set)
        expectedBefore = Collections.singletonList(new SchemaAndValueField("xvc", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "aa"));
        assertRecordSchemaAndValues(expectedBefore, updatedRecord, Envelope.FieldName.BEFORE);

        expectedAfter = Collections.singletonList(new SchemaAndValueField("xvc", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "bb"));
        assertRecordSchemaAndValues(expectedAfter, updatedRecord, Envelope.FieldName.AFTER);

        // drop a column
        statements = "ALTER TABLE test_table DROP COLUMN xvc;" +
                     "UPDATE test_table SET text ='update' WHERE pk = 1;";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();
        VerifyRecord.isValidUpdate(updatedRecord, PK_FIELD, 1);

        // change a column type
        statements = "ALTER TABLE test_table ADD COLUMN modtype INTEGER;" +
                "INSERT INTO test_table (pk,modtype) VALUES (2,1);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        VerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 2);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("modtype", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 1)), updatedRecord, Envelope.FieldName.AFTER);

        statements = "ALTER TABLE test_table ALTER COLUMN modtype TYPE SMALLINT;"
                + "UPDATE test_table SET modtype = 2 WHERE pk = 2;";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();
        VerifyRecord.isValidUpdate(updatedRecord, PK_FIELD, 2);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("modtype", SchemaBuilder.OPTIONAL_INT16_SCHEMA, (short) 1)), updatedRecord, Envelope.FieldName.BEFORE);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("modtype", SchemaBuilder.OPTIONAL_INT16_SCHEMA, (short) 2)), updatedRecord, Envelope.FieldName.AFTER);
    }

    @Test
    public void shouldReceiveChangesForUpdatesWithPKChanges() throws Exception {
        startConnector();
        consumer = testConsumer(3);
        executeAndWait("UPDATE test_table SET text = 'update', pk = 2");

        String topicName = topicName("public.test_table");

        // first should be a delete of the old pk
        SourceRecord deleteRecord = consumer.remove();
        assertEquals(topicName, deleteRecord.topic());
        VerifyRecord.isValidDelete(deleteRecord, PK_FIELD, 1);

        // followed by a tombstone of the old pk
        SourceRecord tombstoneRecord = consumer.remove();
        assertEquals(topicName, tombstoneRecord.topic());
        VerifyRecord.isValidTombstone(tombstoneRecord, PK_FIELD, 1);

        // and finally insert of the new value
        SourceRecord insertRecord = consumer.remove();
        assertEquals(topicName, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, PK_FIELD, 2);
    }

    @Test
    @FixFor("DBZ-582")
    public void shouldReceiveChangesForUpdatesWithPKChangesWithoutTombstone() throws Exception {
        startConnector(config -> config
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
        );
        consumer = testConsumer(2);

        executeAndWait("UPDATE test_table SET text = 'update', pk = 2");

        String topicName = topicName("public.test_table");

        // first should be a delete of the old pk
        SourceRecord deleteRecord = consumer.remove();
        assertEquals(topicName, deleteRecord.topic());
        VerifyRecord.isValidDelete(deleteRecord, PK_FIELD, 1);

        // followed by insert of the new value
        SourceRecord insertRecord = consumer.remove();
        assertEquals(topicName, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, PK_FIELD, 2);
    }

    @Test
    public void shouldReceiveChangesForDefaultValues() throws Exception {
        String statements = "ALTER TABLE test_table REPLICA IDENTITY FULL;" +
                            "ALTER TABLE test_table ADD COLUMN default_column TEXT DEFAULT 'default';" +
                            "INSERT INTO test_table (text) VALUES ('update');";
        startConnector();
        consumer = testConsumer(1);
        executeAndWait(statements);

        SourceRecord insertRecord = consumer.remove();
        assertEquals(topicName("public.test_table"), insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, PK_FIELD, 2);
        List<SchemaAndValueField> expectedSchemaAndValues = Arrays.asList(
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "update"),
                new SchemaAndValueField("default_column", SchemaBuilder.OPTIONAL_STRING_SCHEMA , "default"));
        assertRecordSchemaAndValues(expectedSchemaAndValues, insertRecord, Envelope.FieldName.AFTER);
    }

    @Test
    public void shouldReceiveChangesForTypeConstraints() throws Exception {
        // add a new column
        String statements = "ALTER TABLE test_table ADD COLUMN num_val NUMERIC(5,2);" +
                            "ALTER TABLE test_table REPLICA IDENTITY FULL;" +
                            "UPDATE test_table SET num_val = 123.45 WHERE pk = 1;";

        startConnector();
        consumer = testConsumer(1);
        executeAndWait(statements);

        // the update should be the last record
        SourceRecord updatedRecord = consumer.remove();
        String topicName = topicName("public.test_table");
        assertEquals(topicName, updatedRecord.topic());
        VerifyRecord.isValidUpdate(updatedRecord, PK_FIELD, 1);

        // now check we got the updated value (the old value should be null, the new one whatever we set)
        List<SchemaAndValueField> expectedBefore = Collections.singletonList(new SchemaAndValueField("num_val", null, null));
        assertRecordSchemaAndValues(expectedBefore, updatedRecord, Envelope.FieldName.BEFORE);

        List<SchemaAndValueField> expectedAfter = Collections.singletonList(new SchemaAndValueField("num_val", Decimal.builder(2).parameter(TestHelper.PRECISION_PARAMETER_KEY, "5").optional().build(), new BigDecimal("123.45")));
        assertRecordSchemaAndValues(expectedAfter, updatedRecord, Envelope.FieldName.AFTER);

        // change a constraint
        statements = "ALTER TABLE test_table ALTER COLUMN num_val TYPE NUMERIC(6,1);" +
                "INSERT INTO test_table (pk,num_val) VALUES (2,123.41);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        VerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 2);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("num_val", Decimal.builder(1).parameter(TestHelper.PRECISION_PARAMETER_KEY, "6").optional().build(), new BigDecimal("123.4"))), updatedRecord, Envelope.FieldName.AFTER);

        statements = "ALTER TABLE test_table ALTER COLUMN num_val TYPE NUMERIC;" +
                "INSERT INTO test_table (pk,num_val) VALUES (3,123.4567);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        final Struct dvs = new Struct(VariableScaleDecimal.schema());
        dvs.put("scale", 4).put("value", new BigDecimal("123.4567").unscaledValue().toByteArray());
        VerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 3);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("num_val", VariableScaleDecimal.builder().optional().build(), dvs)), updatedRecord, Envelope.FieldName.AFTER);

        statements = "ALTER TABLE test_table ALTER COLUMN num_val TYPE DECIMAL(12,4);" +
                "INSERT INTO test_table (pk,num_val) VALUES (4,2.48);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        VerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 4);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("num_val", Decimal.builder(4).parameter(TestHelper.PRECISION_PARAMETER_KEY, "12").optional().build(), new BigDecimal("2.4800"))), updatedRecord, Envelope.FieldName.AFTER);

        statements = "ALTER TABLE test_table ALTER COLUMN num_val TYPE DECIMAL(12);" +
                "INSERT INTO test_table (pk,num_val) VALUES (5,1238);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        VerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 5);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("num_val", Decimal.builder(0).parameter(TestHelper.PRECISION_PARAMETER_KEY, "12").optional().build(), new BigDecimal("1238"))), updatedRecord, Envelope.FieldName.AFTER);

        statements = "ALTER TABLE test_table ALTER COLUMN num_val TYPE DECIMAL;" +
                "INSERT INTO test_table (pk,num_val) VALUES (6,1225.1);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        final Struct dvs2 = new Struct(VariableScaleDecimal.schema());
        dvs2.put("scale", 1).put("value", new BigDecimal("1225.1").unscaledValue().toByteArray());
        VerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 6);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("num_val", VariableScaleDecimal.builder().optional().build(), dvs2)), updatedRecord, Envelope.FieldName.AFTER);

        statements = "ALTER TABLE test_table ALTER COLUMN num_val SET NOT NULL;" +
                "INSERT INTO test_table (pk,num_val) VALUES (7,1976);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        dvs2.put("scale", 0).put("value", new BigDecimal("1976").unscaledValue().toByteArray());
        VerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 7);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("num_val", VariableScaleDecimal.builder().build(), dvs2)), updatedRecord, Envelope.FieldName.AFTER);
    }

    @Test
    public void shouldReceiveChangesForDeletes() throws Exception {
        // add a new entry and remove both
        String statements = "INSERT INTO test_table (text) VALUES ('insert2');" +
                            "DELETE FROM test_table WHERE pk > 0;";

        startConnector();
        consumer = testConsumer(5);
        executeAndWait(statements);


        String topicPrefix = "public.test_table";
        String topicName = topicName(topicPrefix);
        assertRecordInserted(topicPrefix, PK_FIELD, 2);

        // first entry removed
        SourceRecord record = consumer.remove();
        assertEquals(topicName, record.topic());
        VerifyRecord.isValidDelete(record, PK_FIELD, 1);

        // followed by a tombstone
        record = consumer.remove();
        assertEquals(topicName, record.topic());
        VerifyRecord.isValidTombstone(record, PK_FIELD, 1);

        // second entry removed
        record = consumer.remove();
        assertEquals(topicName, record.topic());
        VerifyRecord.isValidDelete(record, PK_FIELD, 2);

        // followed by a tombstone
        record = consumer.remove();
        assertEquals(topicName, record.topic());
        VerifyRecord.isValidTombstone(record, PK_FIELD, 2);
    }

    @Test
    @FixFor("DBZ-582")
    public void shouldReceiveChangesForDeletesWithoutTombstone() throws Exception {
        // add a new entry and remove both
        String statements = "INSERT INTO test_table (text) VALUES ('insert2');" +
                            "DELETE FROM test_table WHERE pk > 0;";
        startConnector(config -> config
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
        );
        consumer = testConsumer(3);
        executeAndWait(statements);


        String topicPrefix = "public.test_table";
        String topicName = topicName(topicPrefix);
        assertRecordInserted(topicPrefix, PK_FIELD, 2);

        // first entry removed
        SourceRecord record = consumer.remove();
        assertEquals(topicName, record.topic());
        VerifyRecord.isValidDelete(record, PK_FIELD, 1);

        // second entry removed
        record = consumer.remove();
        assertEquals(topicName, record.topic());
        VerifyRecord.isValidDelete(record, PK_FIELD, 2);
    }

    @Test
    @SkipWhenDecoderPluginNameIs(value = PGOUTPUT, reason = "A delete on a table with no primary key and default replica throws PSQLException as tables must have a PK")
    public void shouldReceiveChangesForDeletesDependingOnReplicaIdentity() throws Exception {
        String topicName = topicName("public.test_table");

        // With PK we should get delete event with default level of replica identity
        String statement = "ALTER TABLE test_table REPLICA IDENTITY DEFAULT;" +
                            "DELETE FROM test_table WHERE pk = 1;";
        startConnector(config -> config
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
        );
        consumer = testConsumer(1);
        executeAndWait(statement);
        SourceRecord record = consumer.remove();
        assertEquals(topicName, record.topic());
        VerifyRecord.isValidDelete(record, PK_FIELD, 1);

        // Without PK we should get delete event with REPLICA IDENTITY FULL
        statement = "ALTER TABLE test_table REPLICA IDENTITY FULL;" +
                    "ALTER TABLE test_table DROP CONSTRAINT test_table_pkey CASCADE;" +
                    "INSERT INTO test_table (pk, text) VALUES (2, 'insert2');" +
                    "DELETE FROM test_table WHERE pk = 2;";
        consumer.expects(2);
        executeAndWait(statement);
        assertRecordInserted("public.test_table", PK_FIELD, 2);
        record = consumer.remove();
        assertEquals(topicName, record.topic());
        VerifyRecord.isValidDelete(record, PK_FIELD, 2);

        // Without PK and without REPLICA IDENTITY FULL we will not get delete event
        statement = "ALTER TABLE test_table REPLICA IDENTITY DEFAULT;" +
                    "INSERT INTO test_table (pk, text) VALUES (3, 'insert3');" +
                    "DELETE FROM test_table WHERE pk = 3;";
        consumer.expects(1);
        executeAndWait(statement);
        assertRecordInserted("public.test_table", PK_FIELD, 3);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    public void shouldReceiveNumericTypeAsDouble() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, PostgresConnectorConfig.DecimalHandlingMode.DOUBLE));

        assertInsert(INSERT_NUMERIC_DECIMAL_TYPES_STMT, 1, schemasAndValuesForDoubleEncodedNumericTypes());
    }

    @Test
    @FixFor("DBZ-611")
    public void shouldReceiveNumericTypeAsString() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, PostgresConnectorConfig.DecimalHandlingMode.STRING));

        assertInsert(INSERT_NUMERIC_DECIMAL_TYPES_STMT, 1, schemasAndValuesForStringEncodedNumericTypes());
    }

    @Test
    @FixFor("DBZ-898")
    public void shouldReceiveHStoreTypeWithSingleValueAsMap() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.HSTORE_HANDLING_MODE, PostgresConnectorConfig.HStoreHandlingMode.MAP));

        assertInsert(INSERT_HSTORE_TYPE_STMT, 1, schemaAndValueFieldForMapEncodedHStoreType());
    }

    @Test
    @FixFor("DBZ-898")
    public void shouldReceiveHStoreTypeWithMultipleValuesAsMap() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.HSTORE_HANDLING_MODE, PostgresConnectorConfig.HStoreHandlingMode.MAP));

        assertInsert(INSERT_HSTORE_TYPE_WITH_MULTIPLE_VALUES_STMT, 1, schemaAndValueFieldForMapEncodedHStoreTypeWithMultipleValues());
    }

    @Test
    @FixFor("DBZ-898")
    public void shouldReceiveHStoreTypeWithNullValuesAsMap() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.HSTORE_HANDLING_MODE, PostgresConnectorConfig.HStoreHandlingMode.MAP));

        assertInsert(INSERT_HSTORE_TYPE_WITH_NULL_VALUES_STMT, 1, schemaAndValueFieldForMapEncodedHStoreTypeWithNullValues());
    }

    @Test
    @FixFor("DBZ-898")
    public void shouldReceiveHStoreTypeWithSpecialCharactersInValuesAsMap() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.HSTORE_HANDLING_MODE, PostgresConnectorConfig.HStoreHandlingMode.MAP));

        assertInsert(INSERT_HSTORE_TYPE_WITH_SPECIAL_CHAR_STMT, 1, schemaAndValueFieldForMapEncodedHStoreTypeWithSpecialCharacters());
    }

    @Test
    @FixFor("DBZ-898")
    public void shouldReceiveHStoreTypeAsJsonString() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");
        consumer = testConsumer(1);

        startConnector(config -> config.with(PostgresConnectorConfig.HSTORE_HANDLING_MODE, PostgresConnectorConfig.HStoreHandlingMode.JSON));

        assertInsert(INSERT_HSTORE_TYPE_STMT, 1, schemaAndValueFieldForJsonEncodedHStoreType());
    }

    @Test
    @FixFor("DBZ-898")
    public void shouldReceiveHStoreTypeWithMultipleValuesAsJsonString() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.HSTORE_HANDLING_MODE, PostgresConnectorConfig.HStoreHandlingMode.JSON));

        assertInsert(INSERT_HSTORE_TYPE_WITH_MULTIPLE_VALUES_STMT, 1, schemaAndValueFieldForJsonEncodedHStoreTypeWithMultipleValues());
    }

    @Test
    @FixFor("DBZ-898")
    public void shouldReceiveHStoreTypeWithSpecialValuesInJsonString() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.HSTORE_HANDLING_MODE, PostgresConnectorConfig.HStoreHandlingMode.JSON));

        assertInsert(INSERT_HSTORE_TYPE_WITH_SPECIAL_CHAR_STMT, 1, schemaAndValueFieldForJsonEncodedHStoreTypeWithSpcialCharacters());
    }

    @Test
    @FixFor("DBZ-898")
    public void shouldReceiveHStoreTypeWithNullValuesAsJsonString() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.HSTORE_HANDLING_MODE, PostgresConnectorConfig.HStoreHandlingMode.JSON));

        assertInsert(INSERT_HSTORE_TYPE_WITH_NULL_VALUES_STMT, 1, schemaAndValueFieldForJsonEncodedHStoreTypeWithNullValues());
    }

    @Test
    @FixFor("DBZ-259")
    public void shouldProcessIntervalDelete() throws Exception {
        final String statements =
                "INSERT INTO table_with_interval VALUES (default, 'Foo', default);" +
                "INSERT INTO table_with_interval VALUES (default, 'Bar', default);" +
                "DELETE FROM table_with_interval WHERE id = 1;";

        startConnector();
        consumer.expects(4);
        executeAndWait(statements);

        final String topicPrefix = "public.table_with_interval";
        final String topicName = topicName(topicPrefix);
        final String pk = "id";
        assertRecordInserted(topicPrefix, pk, 1);
        assertRecordInserted(topicPrefix, pk, 2);

        // first entry removed
        SourceRecord record = consumer.remove();
        assertEquals(topicName, record.topic());
        VerifyRecord.isValidDelete(record, pk, 1);

        // followed by a tombstone
        record = consumer.remove();
        assertEquals(topicName, record.topic());
        VerifyRecord.isValidTombstone(record, pk, 1);
    }

    @Test
    @FixFor("DBZ-644")
    public void shouldPropagateSourceColumnTypeToSchemaParameter() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with("column.propagate.source.type", ".*vc.*"));

        assertInsert(INSERT_STRING_TYPES_STMT, 1, schemasAndValuesForStringTypesWithSourceColumnTypeInfo());
    }

    @Test
    @FixFor("DBZ-1073")
    public void shouldPropagateSourceColumnTypeScaleToSchemaParameter() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config
            .with("column.propagate.source.type", ".*(d|dzs)")
            .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, PostgresConnectorConfig.DecimalHandlingMode.DOUBLE)
        );

        assertInsert(INSERT_NUMERIC_DECIMAL_TYPES_STMT, 1, schemasAndValuesForNumericTypesWithSourceColumnTypeInfo());
    }

    @Test
    @FixFor("DBZ-800")
    public void shouldReceiveHeartbeatAlsoWhenChangingNonWhitelistedTable() throws Exception {
        // the low heartbeat interval should make sure that a heartbeat message is emitted after each change record
        // received from Postgres
        startConnector(config -> config
                .with(Heartbeat.HEARTBEAT_INTERVAL, "1")
                .with(PostgresConnectorConfig.POLL_INTERVAL_MS, "50")
                .with(PostgresConnectorConfig.TABLE_WHITELIST, "s1\\.b")
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER),
                false
        );
        waitForStreamingToStart();

        String statement = "CREATE SCHEMA s1;" +
                           "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                           "CREATE TABLE s1.b (pk SERIAL, bb integer, PRIMARY KEY(pk));" +
                           "INSERT INTO s1.a (aa) VALUES (11);" +
                           "INSERT INTO s1.b (bb) VALUES (22);";

        // streaming from database is non-blocking so we should receive many heartbeats
        final int expectedAtMostStartHeartbeats = 10;
        final int expectedHeartbeats = 5;
        // heartbeat for unfiltered table, data change, heartbeats
        consumer = testConsumer(expectedAtMostStartHeartbeats + 1 + expectedHeartbeats);
        consumer.setIgnoreExtraRecords(true);
        executeAndWait(statement);

        // change record for s1.b and heartbeats
        Optional<SourceRecord> record;
        int startHeartbeats = 0;
        while (true) {
            record = isHeartBeatRecordInserted();
            if (record.isPresent()) {
                assertThat(startHeartbeats).describedAs("Too many start heartbeats").isLessThanOrEqualTo(expectedAtMostStartHeartbeats);
                break;
            }
            startHeartbeats++;
        }

        assertRecordInserted(record.get(), "s1.b", PK_FIELD, 1);
        for (int i = 0; i < expectedHeartbeats; i++) {
            assertHeartBeatRecordInserted();
        }
    }

    @Test
    @FixFor("DBZ-911")
    @SkipWhenDecoderPluginNameIs(value = PGOUTPUT, reason = "Decoder synchronizes all schema columns when processing relation messages")
    public void shouldNotRefreshSchemaOnUnchangedToastedData() throws Exception {
        startConnector(config -> config
                .with(PostgresConnectorConfig.SCHEMA_REFRESH_MODE, PostgresConnectorConfig.SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST)
        );

        String toastedValue = RandomStringUtils.randomAlphanumeric(10000);

        // inserting a toasted value should /always/ produce a correct record
        String statement = "ALTER TABLE test_table ADD COLUMN not_toast integer; INSERT INTO test_table (not_toast, text) values (10, '" + toastedValue + "')";
        consumer = testConsumer(1);
        executeAndWait(statement);

        SourceRecord record = consumer.remove();

        // after record should contain the toasted value
        List<SchemaAndValueField> expectedAfter = Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, toastedValue)
        );
        assertRecordSchemaAndValues(expectedAfter, record, Envelope.FieldName.AFTER);

        // now we remove the toast column and update the not_toast column to see that our unchanged toast data
        // does not trigger a table schema refresh. the after schema should look the same as before.
        statement = "ALTER TABLE test_table DROP COLUMN text; update test_table set not_toast = 5 where not_toast = 10";

        consumer.expects(1);
        executeAndWait(statement);
        assertWithTask(task -> {
            Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_table"));
            assertEquals(Arrays.asList("pk", "text", "not_toast"), tbl.retrieveColumnNames());
        });
    }

    @Test
    @FixFor("DBZ-911")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Decoder synchronizes all schema columns when processing relation messages")
    public void shouldRefreshSchemaOnUnchangedToastedDataWhenSchemaChanged() throws Exception {
        startConnector(config -> config
                .with(PostgresConnectorConfig.SCHEMA_REFRESH_MODE, PostgresConnectorConfig.SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST)
        );

        String toastedValue = RandomStringUtils.randomAlphanumeric(10000);

        // inserting a toasted value should /always/ produce a correct record
        String statement = "ALTER TABLE test_table ADD COLUMN not_toast integer; INSERT INTO test_table (not_toast, text) values (10, '" + toastedValue + "')";
        consumer = testConsumer(1);
        executeAndWait(statement);

        SourceRecord record = consumer.remove();

        // after record should contain the toasted value
        List<SchemaAndValueField> expectedAfter = Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, toastedValue)
        );
        assertRecordSchemaAndValues(expectedAfter, record, Envelope.FieldName.AFTER);

        // now we remove the toast column and update the not_toast column to see that our unchanged toast data
        // does trigger a table schema refresh.  the after schema should be reflect the changes
        statement = "ALTER TABLE test_table DROP COLUMN text; update test_table set not_toast = 5 where not_toast = 10";

        consumer.expects(1);
        executeAndWait(statement);
        assertWithTask(task -> {
            Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_table"));
            assertEquals(Arrays.asList("pk", "not_toast"), tbl.retrieveColumnNames());
        });
    }

    @Test
    @FixFor("DBZ-842")
    public void shouldNotPropagateUnchangedToastedData() throws Exception {
        startConnector(config -> config
                .with(PostgresConnectorConfig.SCHEMA_REFRESH_MODE, PostgresConnectorConfig.SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST)
        );

        final String toastedValue1 = RandomStringUtils.randomAlphanumeric(10000);
        final String toastedValue2 = RandomStringUtils.randomAlphanumeric(10000);
        final String toastedValue3 = RandomStringUtils.randomAlphanumeric(10000);

        // inserting a toasted value should /always/ produce a correct record
        String statement =
                "ALTER TABLE test_table ADD COLUMN not_toast integer;"
              + "ALTER TABLE test_table ADD COLUMN mandatory_text TEXT NOT NULL DEFAULT '';"
              + "ALTER TABLE test_table ALTER COLUMN mandatory_text SET STORAGE EXTENDED;"
              + "ALTER TABLE test_table ALTER COLUMN mandatory_text SET DEFAULT '" + toastedValue3 + "';"
              + "INSERT INTO test_table (not_toast, text, mandatory_text) values (10, '" + toastedValue1 + "', '" + toastedValue1 + "');"
              + "INSERT INTO test_table (not_toast, text, mandatory_text) values (10, '" + toastedValue2 + "', '" + toastedValue2 + "');";
        consumer = testConsumer(2);
        executeAndWait(statement);

        // after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, toastedValue1),
                new SchemaAndValueField("mandatory_text", SchemaBuilder.STRING_SCHEMA, toastedValue1)
        ), consumer.remove(), Envelope.FieldName.AFTER);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, toastedValue2),
                new SchemaAndValueField("mandatory_text", SchemaBuilder.STRING_SCHEMA, toastedValue2)
        ), consumer.remove(), Envelope.FieldName.AFTER);

        statement =
                "UPDATE test_table SET not_toast = 2;"
              + "UPDATE test_table SET not_toast = 3;";

        consumer.expects(6);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_table"));
                assertEquals(Arrays.asList("pk", "text", "not_toast", "mandatory_text"), tbl.retrieveColumnNames());
            });
        });
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 2),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "insert"),
                new SchemaAndValueField("mandatory_text", SchemaBuilder.STRING_SCHEMA, "")
        ), consumer.remove(), Envelope.FieldName.AFTER);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 2),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, DecoderDifferences.optionalToastedValuePlaceholder()),
                new SchemaAndValueField("mandatory_text", SchemaBuilder.STRING_SCHEMA, DecoderDifferences.mandatoryToastedValuePlaceholder())
        ), consumer.remove(), Envelope.FieldName.AFTER);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 2),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, DecoderDifferences.optionalToastedValuePlaceholder()),
                new SchemaAndValueField("mandatory_text", SchemaBuilder.STRING_SCHEMA, DecoderDifferences.mandatoryToastedValuePlaceholder())
        ), consumer.remove(), Envelope.FieldName.AFTER);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 3),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "insert"),
                new SchemaAndValueField("mandatory_text", SchemaBuilder.STRING_SCHEMA, "")
        ), consumer.remove(), Envelope.FieldName.AFTER);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 3),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, DecoderDifferences.optionalToastedValuePlaceholder()),
                new SchemaAndValueField("mandatory_text", SchemaBuilder.STRING_SCHEMA, DecoderDifferences.mandatoryToastedValuePlaceholder())
        ), consumer.remove(), Envelope.FieldName.AFTER);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 3),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, DecoderDifferences.optionalToastedValuePlaceholder()),
                new SchemaAndValueField("mandatory_text", SchemaBuilder.STRING_SCHEMA, DecoderDifferences.mandatoryToastedValuePlaceholder())
        ), consumer.remove(), Envelope.FieldName.AFTER);
    }

    @Test
    @FixFor("DBZ-1029")
    public void shouldReceiveChangesForTableWithoutPrimaryKey() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_table;",
                "CREATE TABLE test_table (id SERIAL, text TEXT);",
                "ALTER TABLE test_table REPLICA IDENTITY FULL"
        );

        startConnector(Function.identity(), false);
        consumer = testConsumer(1);

        // INSERT
        String statement = "INSERT INTO test_table (text) VALUES ('a');";
        assertInsert(
                statement,
                Arrays.asList(
                        new SchemaAndValueField("id", SchemaBuilder.INT32_SCHEMA, 1), // SERIAL is NOT NULL implicitly
                        new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "a")
                )
        );

        // UPDATE
        consumer.expects(1);
        executeAndWait("UPDATE test_table set text='b' WHERE id=1");
        SourceRecord updatedRecord = consumer.remove();
        VerifyRecord.isValidUpdate(updatedRecord);

        List<SchemaAndValueField> expectedBefore = Arrays.asList(
                new SchemaAndValueField("id", SchemaBuilder.INT32_SCHEMA, 1),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "a")
        );
        assertRecordSchemaAndValues(expectedBefore, updatedRecord, Envelope.FieldName.BEFORE);

        List<SchemaAndValueField> expectedAfter = Arrays.asList(
                new SchemaAndValueField("id", SchemaBuilder.INT32_SCHEMA, 1),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "b")
        );
        assertRecordSchemaAndValues(expectedAfter, updatedRecord, Envelope.FieldName.AFTER);

        // DELETE
        consumer.expects(2);
        executeAndWait("DELETE FROM test_table WHERE id=1");
        SourceRecord deletedRecord = consumer.remove();
        VerifyRecord.isValidDelete(deletedRecord);

        expectedBefore = Arrays.asList(
                new SchemaAndValueField("id", SchemaBuilder.INT32_SCHEMA, 1),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "b")
        );
        assertRecordSchemaAndValues(expectedBefore, deletedRecord, Envelope.FieldName.BEFORE);

        expectedAfter = null;
        assertRecordSchemaAndValues(expectedAfter, deletedRecord, Envelope.FieldName.AFTER);
    }

    @Test()
    @FixFor("DBZ-1130")
    @SkipWhenDecoderPluginNameIsNot(value = WAL2JSON, reason = "WAL2JSON specific: Pass 'add-tables' stream parameter and verify it acts as a whitelist")
    public void testPassingStreamParams() throws Exception {
        // Verify that passing stream parameters works by using the WAL2JSON add-tables parameter which acts as a
        // whitelist.
        startConnector(config -> config
                .with(PostgresConnectorConfig.STREAM_PARAMS, "add-tables=s1.should_stream")
        );
        String statement = "CREATE SCHEMA s1;" +
                "CREATE TABLE s1.should_stream (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                "CREATE TABLE s1.should_not_stream (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                "INSERT INTO s1.should_not_stream (aa) VALUES (456);" +
                "INSERT INTO s1.should_stream (aa) VALUES (123);";


        // Verify only one record made it
        consumer = testConsumer(1);
        executeAndWait(statement);

        // Verify the record that made it was from the whitelisted table
        assertRecordInserted("s1.should_stream", PK_FIELD, 1);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test()
    @FixFor("DBZ-1130")
    @SkipWhenDecoderPluginNameIsNot(value = WAL2JSON, reason = "WAL2JSON specific: Pass multiple stream parameters and values verifying they work")
    public void testPassingStreamMultipleParams() throws Exception {
        // Verify that passing multiple stream parameters and multiple parameter values works.
        startConnector(config -> config
                .with(PostgresConnectorConfig.STREAM_PARAMS, "add-tables=s1.should_stream,s2.*;filter-tables=s2.should_not_stream")
        );
        String statement = "CREATE SCHEMA s1;" + "CREATE SCHEMA s2;" +
                "CREATE TABLE s1.should_stream (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                "CREATE TABLE s2.should_stream (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                "CREATE TABLE s1.should_not_stream (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                "CREATE TABLE s2.should_not_stream (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                "INSERT INTO s1.should_not_stream (aa) VALUES (456);" +
                "INSERT INTO s2.should_not_stream (aa) VALUES (111);" +
                "INSERT INTO s1.should_stream (aa) VALUES (123);" +
                "INSERT INTO s2.should_stream (aa) VALUES (999);";


        // Verify only the whitelisted record from s1 and s2 made it.
        consumer = testConsumer(2);
        executeAndWait(statement);

        // Verify the record that made it was from the whitelisted table
        assertRecordInserted("s1.should_stream", PK_FIELD, 1);
        assertRecordInserted("s2.should_stream", PK_FIELD, 1);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    @FixFor("DBZ-1146")
    public void shouldReceiveChangesForReplicaIdentityFullTableWithToastedValueTableFromSnapshot() throws Exception {
        testReceiveChangesForReplicaIdentityFullTableWithToastedValue(SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST, true);
    }

    @Test
    @FixFor("DBZ-1146")
    public void shouldReceiveChangesForReplicaIdentityFullTableWithToastedValueTableFromStreaming() throws Exception {
        testReceiveChangesForReplicaIdentityFullTableWithToastedValue(SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST, false);
    }

    @Test
    @FixFor("DBZ-1146")
    public void shouldReceiveChangesForReplicaIdentityFullTableWithToastedValueTableFromSnapshotFullDiff() throws Exception {
        testReceiveChangesForReplicaIdentityFullTableWithToastedValue(SchemaRefreshMode.COLUMNS_DIFF, true);
    }

    @Test
    @FixFor("DBZ-1146")
    public void shouldReceiveChangesForReplicaIdentityFullTableWithToastedValueTableFromStreamingFullDiff() throws Exception {
        testReceiveChangesForReplicaIdentityFullTableWithToastedValue(SchemaRefreshMode.COLUMNS_DIFF, false);
    }

    @Test()
    @FixFor("DBZ-1181")
    @SkipWhenDecoderPluginNameIs(value = DECODERBUFS, reason = "")
    public void testEmptyChangesProducesHeartbeat() throws Exception {
        // the low heartbeat interval should make sure that a heartbeat message is emitted after each change record
        // received from Postgres
        startConnector(config -> config.with(Heartbeat.HEARTBEAT_INTERVAL, "1"));

        // Expecting 1 heartbeat + 1 data change
        consumer.expects(1 + 1);

        executeAndWait(
                "DROP TABLE IF EXISTS test_table;" +
                "CREATE TABLE test_table (id SERIAL, text TEXT);" +
                "INSERT INTO test_table (text) VALUES ('mydata');"
        );
        consumer.clear();

        consumer.expects(1);
        // Expecting one empty DDL change
        String statement = "CREATE SCHEMA s1;";

        executeAndWait(statement);

        // Expecting one heartbeat for the empty DDL change
        assertHeartBeatRecordInserted();
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    @FixFor("DBZ-1082")
    public void shouldHaveNoXminWhenNotEnabled() throws Exception {
        startConnector(config -> config.with(PostgresConnectorConfig.XMIN_FETCH_INTERVAL, "0"));

        TestHelper.execute("ALTER TABLE test_table REPLICA IDENTITY DEFAULT;");
        String statement = "INSERT INTO test_table (text) VALUES ('no_xmin');";
        executeAndWait(statement);

        // Verify the record that made it does not have an xmin
        SourceRecord rec = assertRecordInserted("public.test_table", PK_FIELD, 2);
        assertSourceInfo(rec, "postgres", "public", "test_table");

        Struct source = ((Struct) rec.value()).getStruct("source");
        assertThat(source.getInt64("xmin")).isNull();

        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    @FixFor("DBZ-1082")
    public void shouldHaveXminWhenEnabled() throws Exception {
        startConnector(config -> config.with(PostgresConnectorConfig.XMIN_FETCH_INTERVAL, "10"));

        TestHelper.execute("ALTER TABLE test_table REPLICA IDENTITY DEFAULT;");
        String statement = "INSERT INTO test_table (text) VALUES ('with_xmin');";
        executeAndWait(statement);

        // Verify the record that made it does not have an xmin
        SourceRecord rec = assertRecordInserted("public.test_table", PK_FIELD, 2);
        assertSourceInfo(rec, "postgres", "public", "test_table");

        Struct source = ((Struct) rec.value()).getStruct("source");
        assertThat(source.getInt64("xmin")).isGreaterThan(0L);

        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    public void shouldProcessLargerTx() throws Exception {
        Testing.Print.disable();
        final int numberOfEvents = 1000;

        startConnector();
        waitForStreamingToStart();

        final String topicPrefix = "public.test_table";
        final String topicName = topicName(topicPrefix);

        final Stopwatch stopwatch = Stopwatch.reusable();
        consumer = testConsumer(numberOfEvents);
        // This is not accurate as we measure also including the data but
        // it is sufficient to confirm there is no large difference
        // in runtime between the cases
        stopwatch.start();
        executeAndWait(IntStream.rangeClosed(2, numberOfEvents + 1)
                .boxed()
                .map(x -> "INSERT INTO test_table (text) VALUES ('insert" + x + "')")
                .collect(Collectors.joining(";")));
        stopwatch.stop();
        final long firstRun = stopwatch.durations().statistics().getTotal().toMillis();
        logger.info("Single tx duration = {} ms", firstRun);
        for (int i = 0; i < numberOfEvents; i++) {
            SourceRecord record = consumer.remove();
            assertEquals(topicName, record.topic());
            VerifyRecord.isValidInsert(record, PK_FIELD, i + 2);
        }

        consumer.expects(numberOfEvents);
        IntStream.rangeClosed(2, numberOfEvents + 1).forEach(x -> TestHelper.execute("INSERT INTO test_table (text) VALUES ('insert" + x + "')"));
        stopwatch.start();
        // There should be no significant difference between many TX runtime and single large TX
        // We still add generous limits as the runtime is in seconds and we cannot provide
        // a stable scheduling environment
        consumer.await(3 * firstRun, TimeUnit.MILLISECONDS);
        stopwatch.stop();
        for (int i = 0; i < numberOfEvents; i++) {
            SourceRecord record = consumer.remove();
            assertEquals(topicName, record.topic());
            VerifyRecord.isValidInsert(record, PK_FIELD, i + 1002);
        }
        logger.info("Many tx duration = {} ms", stopwatch.durations().statistics().getTotal().toMillis());
    }

    private void testReceiveChangesForReplicaIdentityFullTableWithToastedValue(PostgresConnectorConfig.SchemaRefreshMode mode, boolean tablesBeforeStart) throws Exception {
        if (tablesBeforeStart) {
            TestHelper.execute(
                    "DROP TABLE IF EXISTS test_table;",
                    "CREATE TABLE test_table (id SERIAL, not_toast int, text TEXT);",
                    "ALTER TABLE test_table REPLICA IDENTITY FULL"
            );
        }

        startConnector(config -> config.with(PostgresConnectorConfig.SCHEMA_REFRESH_MODE, mode), false);
        consumer = testConsumer(1);

        final String toastedValue = RandomStringUtils.randomAlphanumeric(10000);

        if (!tablesBeforeStart) {
            TestHelper.execute(
                    "DROP TABLE IF EXISTS test_table;",
                    "CREATE TABLE test_table (id SERIAL, not_toast int, text TEXT);",
                    "ALTER TABLE test_table REPLICA IDENTITY FULL"
            );
        }

        // INSERT
        String statement = "INSERT INTO test_table (not_toast, text) VALUES (10,'" + toastedValue + "');";
        assertInsert(
                statement,
                Arrays.asList(
                        new SchemaAndValueField("id", SchemaBuilder.INT32_SCHEMA, 1), // SERIAL is NOT NULL implicitly
                        new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                        new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, toastedValue)
                )
        );

        // UPDATE
        consumer.expects(1);
        executeAndWait("UPDATE test_table set not_toast = 20");
        SourceRecord updatedRecord = consumer.remove();

        if (DecoderDifferences.areToastedValuesPresentInSchema() || mode == SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST) {
            assertRecordSchemaAndValues(Arrays.asList(
                    new SchemaAndValueField("id", SchemaBuilder.INT32_SCHEMA, 1),
                    new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                    new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, toastedValue)
            ), updatedRecord, Envelope.FieldName.BEFORE);
            assertRecordSchemaAndValues(Arrays.asList(
                    new SchemaAndValueField("id", SchemaBuilder.INT32_SCHEMA, 1),
                    new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 20),
                    new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, toastedValue)
            ), updatedRecord, Envelope.FieldName.AFTER);
        }
        else {
            assertRecordSchemaAndValues(Arrays.asList(
                    new SchemaAndValueField("id", SchemaBuilder.INT32_SCHEMA, 1),
                    new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10)
            ), updatedRecord, Envelope.FieldName.BEFORE);
            assertRecordSchemaAndValues(Arrays.asList(
                    new SchemaAndValueField("id", SchemaBuilder.INT32_SCHEMA, 1),
                    new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 20)
            ), updatedRecord, Envelope.FieldName.AFTER);
        }

        // DELETE
        consumer.expects(2);
        executeAndWait("DELETE FROM test_table");
        final SourceRecord deletedRecord = consumer.remove();
        final SourceRecord tmobstoneRecord = consumer.remove();
        assertThat(tmobstoneRecord.value()).isNull();
        assertThat(tmobstoneRecord.valueSchema()).isNull();
        if (DecoderDifferences.areToastedValuesPresentInSchema() || mode == SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST) {
            assertRecordSchemaAndValues(Arrays.asList(
                    new SchemaAndValueField("id", SchemaBuilder.INT32_SCHEMA, 1),
                    new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 20),
                    new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, toastedValue)
            ), deletedRecord, Envelope.FieldName.BEFORE);
        }
        else {
            assertRecordSchemaAndValues(Arrays.asList(
                    new SchemaAndValueField("id", SchemaBuilder.INT32_SCHEMA, 1),
                    new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 20)
            ), deletedRecord, Envelope.FieldName.BEFORE);
        }
    }

    private void assertHeartBeatRecordInserted() {
        assertFalse("records not generated", consumer.isEmpty());

        SourceRecord heartbeat = consumer.remove();
        assertEquals("__debezium-heartbeat." + TestHelper.TEST_SERVER, heartbeat.topic());

        Struct key = (Struct) heartbeat.key();
        assertThat(key.get("serverName")).isEqualTo(TestHelper.TEST_SERVER);

        Struct value = (Struct) heartbeat.value();
        assertThat(value.getInt64("ts_ms")).isLessThanOrEqualTo(Instant.now().toEpochMilli());
    }

    private Optional<SourceRecord> isHeartBeatRecordInserted() {
        assertFalse("records not generated", consumer.isEmpty());
        final String heartbeatTopicName = "__debezium-heartbeat." + TestHelper.TEST_SERVER;

        SourceRecord record = consumer.remove();
        if (!heartbeatTopicName.equals(record.topic())) {
            return Optional.of(record);
        }

        assertEquals(heartbeatTopicName, record.topic());

        Struct key = (Struct) record.key();
        assertThat(key.get("serverName")).isEqualTo(TestHelper.TEST_SERVER);

        Struct value = (Struct) record.value();
        assertThat(value.getInt64("ts_ms")).isLessThanOrEqualTo(Instant.now().toEpochMilli());
        return Optional.empty();
    }

    private void assertInsert(String statement, List<SchemaAndValueField> expectedSchemaAndValuesByColumn) {
        assertInsert(statement, null, expectedSchemaAndValuesByColumn);
    }

    private void assertInsert(String statement, Integer pk, List<SchemaAndValueField> expectedSchemaAndValuesByColumn) {
        TableId table = tableIdFromInsertStmt(statement);
        String expectedTopicName = table.schema() + "." + table.table();
        expectedTopicName = expectedTopicName.replaceAll("[ \"]", "_");

        try {
            executeAndWait(statement);
            SourceRecord record = assertRecordInserted(expectedTopicName, pk != null ? PK_FIELD : null, pk);
            assertRecordOffsetAndSnapshotSource(record, false, false);
            assertSourceInfo(record, "postgres", table.schema(), table.table());
            assertRecordSchemaAndValues(expectedSchemaAndValuesByColumn, record, Envelope.FieldName.AFTER);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private SourceRecord assertRecordInserted(SourceRecord insertedRecord, String expectedTopicName, String pkColumn, Integer pk) throws InterruptedException {
        assertEquals(topicName(expectedTopicName), insertedRecord.topic());

        if (pk != null) {
            VerifyRecord.isValidInsert(insertedRecord, pkColumn, pk);
        }
        else {
            VerifyRecord.isValidInsert(insertedRecord);
        }

        return insertedRecord;
    }

    private SourceRecord assertRecordInserted(String expectedTopicName, String pkColumn, Integer pk) throws InterruptedException {
        assertFalse("records not generated", consumer.isEmpty());
        SourceRecord insertedRecord = consumer.remove();

        return assertRecordInserted(insertedRecord, expectedTopicName, pkColumn, pk);
    }

    private void executeAndWait(String statements) throws Exception {
        TestHelper.execute(statements);
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);
    }

    private void executeAndWaitForNoRecords(String statements) throws Exception {
        TestHelper.execute(statements);
        consumer.await(5, TimeUnit.SECONDS);
    }
}

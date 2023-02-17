/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.TYPE_LENGTH_PARAMETER_KEY;
import static io.debezium.connector.postgresql.TestHelper.TYPE_NAME_PARAMETER_KEY;
import static io.debezium.connector.postgresql.TestHelper.TYPE_SCALE_PARAMETER_KEY;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIs.DecoderPluginName.PGOUTPUT;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.postgresql.util.PSQLException;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.postgresql.PostgresConnectorConfig.IntervalHandlingMode;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SchemaRefreshMode;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.junit.SkipTestDependingOnDecoderPluginNameRule;
import io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIs;
import io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIsNot;
import io.debezium.data.Bits;
import io.debezium.data.Enum;
import io.debezium.data.Envelope;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.VerifyRecord;
import io.debezium.data.geometry.Point;
import io.debezium.doc.FixFor;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.junit.ConditionalFail;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
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
        String statements = "CREATE SCHEMA IF NOT EXISTS public;" +
                "DROP TABLE IF EXISTS test_table;" +
                "CREATE TABLE test_table (pk SERIAL, text TEXT, PRIMARY KEY(pk));" +
                "CREATE TABLE table_with_interval (id SERIAL PRIMARY KEY, title VARCHAR(512) NOT NULL, time_limit INTERVAL DEFAULT '60 days'::INTERVAL NOT NULL);" +
                "INSERT INTO test_table(text) VALUES ('insert');";
        TestHelper.execute(statements);

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, false)
                .with(PostgresConnectorConfig.SCHEMA_EXCLUDE_LIST, "postgis");

        // todo DBZ-766 are these really needed?
        if (TestHelper.decoderPlugin() == PostgresConnectorConfig.LogicalDecoder.PGOUTPUT) {
            configBuilder = configBuilder.with("database.replication", "database")
                    .with("database.preferQueryMode", "simple")
                    .with("assumeMinServerVersion.set", "9.4");
        }

        Testing.Print.enable();
    }

    private void startConnector(Function<Configuration.Builder, Configuration.Builder> customConfig, boolean waitForSnapshot, Predicate<SourceRecord> isStopRecord)
            throws InterruptedException {
        start(PostgresConnector.class, new PostgresConnectorConfig(customConfig.apply(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, false)
                .with(PostgresConnectorConfig.SCHEMA_EXCLUDE_LIST, "postgis")
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, waitForSnapshot ? SnapshotMode.INITIAL : SnapshotMode.NEVER))
                .build()).getConfig(), isStopRecord);
        assertConnectorIsRunning();
        waitForStreamingToStart();

        if (waitForSnapshot) {
            // Wait for snapshot to be in progress
            consumer = testConsumer(1);
            consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
            consumer.remove();
        }
    }

    private void startConnector(Function<Configuration.Builder, Configuration.Builder> customConfig, boolean waitForSnapshot) throws InterruptedException {
        startConnector(customConfig, waitForSnapshot, (x) -> false);
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

        // numerical types
        consumer.expects(1);
        assertInsert(INSERT_NUMERIC_TYPES_STMT, 1, schemasAndValuesForNumericType());

        // numerical decimal types
        consumer.expects(1);
        assertInsert(INSERT_NUMERIC_DECIMAL_TYPES_STMT_NO_NAN, 1, schemasAndValuesForBigDecimalEncodedNumericTypes());

        // string types
        consumer.expects(1);
        assertInsert(INSERT_STRING_TYPES_STMT, 1, schemasAndValuesForStringTypes());

        // monetary types
        consumer.expects(1);
        assertInsert(INSERT_CASH_TYPES_STMT, 1, schemaAndValuesForMoneyTypes());

        // negative monetary types
        consumer.expects(1);
        assertInsert(INSERT_NEGATIVE_CASH_TYPES_STMT, 2, schemaAndValuesForNegativeMoneyTypes());

        // bits and bytes
        consumer.expects(1);
        assertInsert(INSERT_BIN_TYPES_STMT, 1, schemaAndValuesForBinTypes());

        // date and time
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
    @FixFor("DBZ-5014")
    public void shouldReceiveDeletesWithInfinityDate() throws Exception {
        Testing.Print.enable();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute("ALTER TABLE time_table REPLICA IDENTITY FULL");
        startConnector();

        executeAndWait(INSERT_DATE_TIME_TYPES_STMT);

        consumer = testConsumer(1);

        assertDelete(DELETE_DATE_TIME_TYPES_STMT, 1, schemaAndValuesForDateTimeTypes());
    }

    @Test
    @FixFor("DBZ-1498")
    public void shouldReceiveChangesForIntervalAsString() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");
        startConnector(config -> config
                .with(PostgresConnectorConfig.INTERVAL_HANDLING_MODE, IntervalHandlingMode.STRING));

        consumer = testConsumer(1);

        // date and time
        consumer.expects(1);
        assertInsert(INSERT_DATE_TIME_TYPES_STMT, 1, schemaAndValuesForIntervalAsString());
    }

    @Test
    @FixFor("DBZ-766")
    public void shouldReceiveChangesAfterConnectionRestart() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();

        startConnector(config -> config
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.SCHEMA_EXCLUDE_LIST, "postgis"));

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
                .with(PostgresConnectorConfig.SCHEMA_EXCLUDE_LIST, "postgis"),
                false);
        consumer = testConsumer(1);
        waitForStreamingToStart();

        // Insert new row and verify inserted
        executeAndWait("INSERT INTO t0 (pk,d,d2) VALUES (2,1,3);");
        assertRecordInserted("public.t0", PK_FIELD, 2);
    }

    @Test
    @FixFor("DBZ-1698")
    public void shouldReceiveUpdateSchemaAfterConnectionRestart() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();

        startConnector(config -> config
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SCHEMA_EXCLUDE_LIST, "postgis")
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false)
                .with(PostgresConnectorConfig.SCHEMA_REFRESH_MODE, SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST));

        TestHelper.execute("CREATE TABLE t0 (pk SERIAL, d INTEGER, PRIMARY KEY(pk));");

        consumer = testConsumer(1);
        waitForStreamingToStart();

        // Insert new row and verify inserted
        executeAndWait("INSERT INTO t0 (pk,d) VALUES(1,1);");
        assertRecordInserted("public.t0", PK_FIELD, 1);

        // simulate the connector is stopped
        stopConnector();
        Thread.sleep(3000);

        // Add record offline
        TestHelper.execute("INSERT INTO t0 (pk,d) VALUES(2,2);");

        // Alter schema offline
        TestHelper.execute("ALTER TABLE t0 ADD COLUMN d2 NUMERIC(10,6) DEFAULT 0 NOT NULL;");
        TestHelper.execute("ALTER TABLE t0 ALTER COLUMN d SET NOT NULL;");

        // Start the producer and wait; the wait is to guarantee the stream thread is polling
        // This appears to be a potential race condition problem
        startConnector(config -> config
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SCHEMA_EXCLUDE_LIST, "postgis")
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false)
                .with(PostgresConnectorConfig.SCHEMA_REFRESH_MODE, SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST),
                false);
        consumer = testConsumer(2);
        waitForStreamingToStart();

        // Insert new row and verify inserted
        executeAndWait("INSERT INTO t0 (pk,d,d2) VALUES (3,1,3);");
        assertRecordInserted("public.t0", PK_FIELD, 2);
        assertRecordInserted("public.t0", PK_FIELD, 3);

        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
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
            assertThat(before.get("created_at")).isEqualTo(new java.util.Date(0));
            assertThat(before.get("created_at_tz")).isEqualTo("1970-01-01T00:00:00Z");
            assertThat(before.get("ctime")).isEqualTo(new java.util.Date(0));
            assertThat(before.get("ctime_tz")).isEqualTo("00:00:00Z");
            assertThat(before.get("cdate")).isEqualTo(new java.util.Date(0));
            assertThat(before.get("cmoney")).isEqualTo(new BigDecimal("0.00"));
            assertThat(before.get("cbits")).isEqualTo(new byte[0]);
        }
    }

    @Test
    @FixFor("DBZ-1141")
    public void shouldProcessNotNullColumnsAdaptiveDateTypes() throws Exception {
        final Struct before = testProcessNotNullColumns(TemporalPrecisionMode.ADAPTIVE);
        if (before != null) {
            assertThat(before.get("created_at")).isEqualTo(0L);
            assertThat(before.get("created_at_tz")).isEqualTo("1970-01-01T00:00:00Z");
            assertThat(before.get("ctime")).isEqualTo(0L);
            assertThat(before.get("ctime_tz")).isEqualTo("00:00:00Z");
            assertThat(before.get("cdate")).isEqualTo(0);
            assertThat(before.get("cmoney")).isEqualTo(new BigDecimal("0.00"));
            assertThat(before.get("cbits")).isEqualTo(new byte[0]);
        }
    }

    @Test
    @FixFor("DBZ-1141")
    public void shouldProcessNotNullColumnsAdaptiveMsDateTypes() throws Exception {
        final Struct before = testProcessNotNullColumns(TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        if (before != null) {
            assertThat(before.get("created_at")).isEqualTo(0L);
            assertThat(before.get("created_at_tz")).isEqualTo("1970-01-01T00:00:00Z");
            assertThat(before.get("ctime")).isEqualTo(0L);
            assertThat(before.get("ctime_tz")).isEqualTo("00:00:00Z");
            assertThat(before.get("cdate")).isEqualTo(0);
            assertThat(before.get("cmoney")).isEqualTo(new BigDecimal("0.00"));
            assertThat(before.get("cbits")).isEqualTo(new byte[0]);
        }
    }

    @Test
    @FixFor("DBZ-1158")
    public void shouldProcessNotNullColumnsFallbacksReplicaIdentity() throws Exception {
        // Use adaptive here as its the connector default
        final Struct before = testProcessNotNullColumns(TemporalPrecisionMode.ADAPTIVE);
        if (before != null) {
            assertThat(before.get("csmallint")).isEqualTo((short) 0);
            assertThat(before.get("cinteger")).isEqualTo(0);
            assertThat(before.get("cbigint")).isEqualTo(0L);
            assertThat(before.get("creal")).isEqualTo(0.f);
            assertThat(before.get("cbool")).isEqualTo(false);
            assertThat(before.get("cfloat8")).isEqualTo(0.0);
            assertThat(before.get("cnumeric")).isEqualTo(new BigDecimal("0.00"));
            assertThat(before.get("cvarchar")).isEqualTo("");
            assertThat(before.get("cbox")).isEqualTo(new byte[0]);
            assertThat(before.get("ccircle")).isEqualTo(new byte[0]);
            assertThat(before.get("cinterval")).isEqualTo(0L);
            assertThat(before.get("cline")).isEqualTo(new byte[0]);
            assertThat(before.get("clseg")).isEqualTo(new byte[0]);
            assertThat(before.get("cpath")).isEqualTo(new byte[0]);
            assertThat(before.get("cpoint")).isEqualTo(Point.createValue(Point.builder().build(), 0, 0));
            assertThat(before.get("cpolygon")).isEqualTo(new byte[0]);
            assertThat(before.get("cchar")).isEqualTo("");
            assertThat(before.get("ctext")).isEqualTo("");
            assertThat(before.get("cjson")).isEqualTo("");
            assertThat(before.get("cxml")).isEqualTo("");
            assertThat(before.get("cuuid")).isEqualTo("");
            assertThat(before.get("cvarbit")).isEqualTo(new byte[0]);
            assertThat(before.get("cinet")).isEqualTo("");
            assertThat(before.get("ccidr")).isEqualTo("");
            assertThat(before.get("cmacaddr")).isEqualTo("");
        }
    }

    private Struct testProcessNotNullColumns(TemporalPrecisionMode temporalMode) throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SCHEMA_EXCLUDE_LIST, "postgis")
                .with(PostgresConnectorConfig.TIME_PRECISION_MODE, temporalMode));

        consumer.expects(1);
        executeAndWait("INSERT INTO not_null_table VALUES (default, 30, '2019-02-10 11:34:58', '2019-02-10 11:35:00', "
                + "'10:20:11', '10:20:12', '2019-02-01', '$20', B'101', 32766, 2147483646, 9223372036854775806, 3.14, "
                + "true, 3.14768, 1234.56, 'Test', '(0,0),(1,1)', '<(0,0),1>', '01:02:03', '{0,1,2}', '((0,0),(1,1))', "
                + "'((0,0),(0,1),(0,2))', '(1,1)', '((0,0),(0,1),(1,1))', 'a', 'hello world', '{\"key\": 123}', "
                + "'<doc><item>abc</item></doc>', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', B'101', '192.168.1.100', "
                + "'192.168.1', '08:00:2b:01:02:03');");

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
    @SkipWhenDecoderPluginNameIs(value = PGOUTPUT, reason = "Decoder synchronizes all schema columns when processing relation messages")
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
    @FixFor("DBZ-1029")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Decoder synchronizes all schema columns when processing relation messages")
    public void shouldReceiveChangesForInsertsIndependentOfReplicaIdentityWhenSchemaChanged() throws Exception {
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
        assertInsert(statement, Arrays.asList(new SchemaAndValueField("pk", SchemaBuilder.int32().defaultValue(0).build(), 4),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "no_pk_and_full")));

        consumer.expects(1);
        TestHelper.execute("ALTER TABLE test_table REPLICA IDENTITY DEFAULT;");
        statement = "INSERT INTO test_table (pk, text) VALUES (5, 'no_pk_and_default');";
        assertInsert(statement, Arrays.asList(new SchemaAndValueField("pk", SchemaBuilder.int32().defaultValue(0).build(), 5),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "no_pk_and_default")));
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
        TestHelper.setReplicaIdentityForTable("test_table", "FULL");
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

        expectedBefore = Collections.singletonList(new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "update2"));
        assertRecordSchemaAndValues(expectedBefore, updatedRecord, Envelope.FieldName.BEFORE);

        expectedAfter = Collections.singletonList(new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "update3"));
        assertRecordSchemaAndValues(expectedAfter, updatedRecord, Envelope.FieldName.AFTER);

        // without PK and with REPLICA IDENTITY DEFAULT we will get nothing
        TestHelper.setReplicaIdentityForTable("test_table", "DEFAULT");
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

    private Header getPKUpdateNewKeyHeader(SourceRecord record) {
        return this.getHeaderField(record, RelationalChangeRecordEmitter.PK_UPDATE_NEWKEY_FIELD);
    }

    private Header getPKUpdateOldKeyHeader(SourceRecord record) {
        return this.getHeaderField(record, RelationalChangeRecordEmitter.PK_UPDATE_OLDKEY_FIELD);
    }

    private Header getHeaderField(SourceRecord record, String fieldName) {
        return StreamSupport.stream(record.headers().spliterator(), false)
                .filter(header -> fieldName.equals(header.key()))
                .collect(Collectors.toList()).get(0);
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

        Header keyPKUpdateHeader = getPKUpdateNewKeyHeader(deleteRecord);
        assertEquals(Integer.valueOf(2), ((Struct) keyPKUpdateHeader.value()).getInt32("pk"));

        // followed by a tombstone of the old pk
        SourceRecord tombstoneRecord = consumer.remove();
        assertEquals(topicName, tombstoneRecord.topic());
        VerifyRecord.isValidTombstone(tombstoneRecord, PK_FIELD, 1);

        // and finally insert of the new value
        SourceRecord insertRecord = consumer.remove();
        assertEquals(topicName, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, PK_FIELD, 2);

        keyPKUpdateHeader = getPKUpdateOldKeyHeader(insertRecord);
        assertEquals(Integer.valueOf(1), ((Struct) keyPKUpdateHeader.value()).getInt32("pk"));
    }

    @Test
    @FixFor("DBZ-582")
    public void shouldReceiveChangesForUpdatesWithPKChangesWithoutTombstone() throws Exception {
        startConnector(config -> config
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false));
        consumer = testConsumer(2);

        executeAndWait("UPDATE test_table SET text = 'update', pk = 2");

        String topicName = topicName("public.test_table");

        // first should be a delete of the old pk
        SourceRecord deleteRecord = consumer.remove();
        assertEquals(topicName, deleteRecord.topic());
        VerifyRecord.isValidDelete(deleteRecord, PK_FIELD, 1);

        Header keyPKUpdateHeader = getPKUpdateNewKeyHeader(deleteRecord);
        assertEquals(Integer.valueOf(2), ((Struct) keyPKUpdateHeader.value()).getInt32("pk"));

        // followed by insert of the new value
        SourceRecord insertRecord = consumer.remove();
        assertEquals(topicName, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, PK_FIELD, 2);

        keyPKUpdateHeader = getPKUpdateOldKeyHeader(insertRecord);
        assertEquals(Integer.valueOf(1), ((Struct) keyPKUpdateHeader.value()).getInt32("pk"));
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
                new SchemaAndValueField("default_column", SchemaBuilder.string().optional().defaultValue("default").build(), "default"));
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

        List<SchemaAndValueField> expectedAfter = Collections.singletonList(
                new SchemaAndValueField("num_val", Decimal.builder(2).parameter(TestHelper.PRECISION_PARAMETER_KEY, "5").optional().build(), new BigDecimal("123.45")));
        assertRecordSchemaAndValues(expectedAfter, updatedRecord, Envelope.FieldName.AFTER);

        // change a constraint
        statements = "ALTER TABLE test_table ALTER COLUMN num_val TYPE NUMERIC(6,1);" +
                "INSERT INTO test_table (pk,num_val) VALUES (2,123.41);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        VerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 2);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("num_val", Decimal.builder(1).parameter(TestHelper.PRECISION_PARAMETER_KEY, "6").optional().build(),
                        new BigDecimal("123.4"))),
                updatedRecord, Envelope.FieldName.AFTER);

        statements = "ALTER TABLE test_table ALTER COLUMN num_val TYPE NUMERIC;" +
                "INSERT INTO test_table (pk,num_val) VALUES (3,123.4567);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        final Struct dvs = new Struct(VariableScaleDecimal.schema());
        dvs.put("scale", 4).put("value", new BigDecimal("123.4567").unscaledValue().toByteArray());
        VerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 3);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("num_val", VariableScaleDecimal.builder().optional().build(), dvs)), updatedRecord,
                Envelope.FieldName.AFTER);

        statements = "ALTER TABLE test_table ALTER COLUMN num_val TYPE DECIMAL(12,4);" +
                "INSERT INTO test_table (pk,num_val) VALUES (4,2.48);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        VerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 4);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("num_val", Decimal.builder(4).parameter(TestHelper.PRECISION_PARAMETER_KEY, "12").optional().build(),
                        new BigDecimal("2.4800"))),
                updatedRecord, Envelope.FieldName.AFTER);

        statements = "ALTER TABLE test_table ALTER COLUMN num_val TYPE DECIMAL(12);" +
                "INSERT INTO test_table (pk,num_val) VALUES (5,1238);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        VerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 5);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("num_val", Decimal.builder(0).parameter(TestHelper.PRECISION_PARAMETER_KEY, "12").optional().build(),
                        new BigDecimal("1238"))),
                updatedRecord, Envelope.FieldName.AFTER);

        statements = "ALTER TABLE test_table ALTER COLUMN num_val TYPE DECIMAL;" +
                "INSERT INTO test_table (pk,num_val) VALUES (6,1225.1);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        final Struct dvs2 = new Struct(VariableScaleDecimal.schema());
        dvs2.put("scale", 1).put("value", new BigDecimal("1225.1").unscaledValue().toByteArray());
        VerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 6);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("num_val", VariableScaleDecimal.builder().optional().build(), dvs2)), updatedRecord,
                Envelope.FieldName.AFTER);

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
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false));
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
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false));
        consumer = testConsumer(1);
        executeAndWait(statement);
        SourceRecord record = consumer.remove();
        assertEquals(topicName, record.topic());
        VerifyRecord.isValidDelete(record, PK_FIELD, 1);

        // Without PK we should get delete event with REPLICA IDENTITY FULL
        statement = "ALTER TABLE test_table DROP CONSTRAINT test_table_pkey CASCADE;" +
                "INSERT INTO test_table (pk, text) VALUES (2, 'insert2');" +
                "DELETE FROM test_table WHERE pk = 2;";
        consumer.expects(2);
        TestHelper.setReplicaIdentityForTable("test_table", "FULL");
        executeAndWait(statement);
        assertRecordInserted("public.test_table", PK_FIELD, 2);
        record = consumer.remove();
        assertEquals(topicName, record.topic());
        VerifyRecord.isValidDelete(record, PK_FIELD, 2);

        // Without PK and without REPLICA IDENTITY FULL we will not get delete event
        statement = "INSERT INTO test_table (pk, text) VALUES (3, 'insert3');" +
                "DELETE FROM test_table WHERE pk = 3;";
        consumer.expects(1);
        TestHelper.setReplicaIdentityForTable("test_table", "DEFAULT");
        executeAndWait(statement);
        assertRecordInserted("public.test_table", PK_FIELD, 3);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    @FixFor("DBZ-4137")
    public void shouldReceiveNumericTypeAsDoubleWithNullDefaults() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS numeric_table_with_n_defaults;",
                "CREATE TABLE numeric_table_with_n_defaults (\n" +
                        "    r int4 NOT NULL,\n" +
                        "    r_numeric numeric(19, 4) NULL DEFAULT NULL,\n" +
                        "    r_int int4 NULL DEFAULT NULL);",
                "ALTER TABLE numeric_table_with_n_defaults REPLICA IDENTITY FULL");

        startConnector(config -> config.with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE),
                false);
        consumer = testConsumer(1);

        // INSERT
        String statement = "INSERT INTO numeric_table_with_n_defaults (r) VALUES (1);";
        assertInsert(
                statement,
                Arrays.asList(
                        new SchemaAndValueField("r", Schema.INT32_SCHEMA, 1),
                        new SchemaAndValueField("r_numeric",
                                new SchemaBuilder(Schema.Type.FLOAT64)
                                        .name(Schema.FLOAT64_SCHEMA.name())
                                        .version(Schema.FLOAT64_SCHEMA.version())
                                        .optional()
                                        .defaultValue(null)
                                        .build(),
                                null),
                        new SchemaAndValueField("r_int",
                                new SchemaBuilder(Schema.Type.INT32)
                                        .name(Schema.INT32_SCHEMA.name())
                                        .version(Schema.INT32_SCHEMA.version())
                                        .optional()
                                        .defaultValue(null)
                                        .build(),
                                null)));
    }

    @Test
    @FixFor("DBZ-4137")
    public void shouldReceiveNumericTypeAsDoubleWithDefaults() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS numeric_table_with_defaults;",
                "CREATE TABLE numeric_table_with_defaults (\n" +
                        "    r int4 NOT NULL,\n" +
                        "    r_numeric numeric(19, 4) NOT NULL DEFAULT 1,\n" +
                        "    r_int int4 NOT NULL DEFAULT 2);",
                "ALTER TABLE numeric_table_with_defaults REPLICA IDENTITY FULL");

        startConnector(config -> config.with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE),
                false);
        consumer = testConsumer(1);

        // INSERT
        String statement = "INSERT INTO numeric_table_with_defaults (r) VALUES (1);";
        assertInsert(
                statement,
                Arrays.asList(
                        new SchemaAndValueField("r", Schema.INT32_SCHEMA, 1),
                        new SchemaAndValueField("r_numeric",
                                new SchemaBuilder(Schema.Type.FLOAT64)
                                        .name(Schema.FLOAT64_SCHEMA.name())
                                        .version(Schema.FLOAT64_SCHEMA.version())
                                        .defaultValue(1.0d)
                                        .build(),
                                1.0d),
                        new SchemaAndValueField("r_int",
                                new SchemaBuilder(Schema.Type.INT32)
                                        .name(Schema.INT32_SCHEMA.name())
                                        .version(Schema.INT32_SCHEMA.version())
                                        .defaultValue(2)
                                        .build(),
                                2)));
    }

    @Test
    public void shouldReceiveNumericTypeAsDouble() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE));

        assertInsert(INSERT_NUMERIC_DECIMAL_TYPES_STMT, 1, schemasAndValuesForDoubleEncodedNumericTypes());
    }

    @Test
    @FixFor("DBZ-611")
    public void shouldReceiveNumericTypeAsString() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.STRING));

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
    @FixFor("DBZ-1814")
    public void shouldReceiveByteaBytes() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.BINARY_HANDLING_MODE, PostgresConnectorConfig.BinaryHandlingMode.BYTES));

        assertInsert(INSERT_BYTEA_BINMODE_STMT, 1, schemaAndValueForByteaBytes());
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldReceiveByteaBase64String() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.BINARY_HANDLING_MODE, PostgresConnectorConfig.BinaryHandlingMode.BASE64));

        assertInsert(INSERT_BYTEA_BINMODE_STMT, 1, schemaAndValueForByteaBase64());
    }

    @Test
    @FixFor("DBZ-5544")
    public void shouldReceiveByteaBase64UrlSafeString() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.BINARY_HANDLING_MODE, PostgresConnectorConfig.BinaryHandlingMode.BASE64_URL_SAFE));

        assertInsert(INSERT_BYTEA_BINMODE_STMT, 1, schemaAndValueForByteaBase64UrlSafe());
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldReceiveByteaHexString() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.BINARY_HANDLING_MODE, PostgresConnectorConfig.BinaryHandlingMode.HEX));

        assertInsert(INSERT_BYTEA_BINMODE_STMT, 1, schemaAndValueForByteaHex());
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldReceiveUnknownTypeAsBytes() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true));

        assertInsert(INSERT_CIRCLE_STMT, 1, schemaAndValueForUnknownColumnBytes());
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldReceiveUnknownTypeAsBase64() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.BINARY_HANDLING_MODE, BinaryHandlingMode.BASE64));

        assertInsert(INSERT_CIRCLE_STMT, 1, schemaAndValueForUnknownColumnBase64());
    }

    @Test
    @FixFor("DBZ-5544")
    public void shouldReceiveUnknownTypeAsBase64UrlSafe() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.BINARY_HANDLING_MODE, BinaryHandlingMode.BASE64_URL_SAFE));

        assertInsert(INSERT_CIRCLE_STMT, 1, schemaAndValueForUnknownColumnBase64UrlSafe());
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldReceiveUnknownTypeAsHex() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        startConnector(config -> config.with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.BINARY_HANDLING_MODE, BinaryHandlingMode.HEX));

        assertInsert(INSERT_CIRCLE_STMT, 1, schemaAndValueForUnknownColumnHex());
    }

    @Test
    @FixFor("DBZ-259")
    public void shouldProcessIntervalDelete() throws Exception {
        final String statements = "INSERT INTO table_with_interval VALUES (default, 'Foo', default);" +
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
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, PostgresConnectorConfig.DecimalHandlingMode.DOUBLE));

        assertInsert(INSERT_NUMERIC_DECIMAL_TYPES_STMT, 1, schemasAndValuesForNumericTypesWithSourceColumnTypeInfo());
    }

    @Test
    @FixFor("DBZ-800")
    public void shouldReceiveHeartbeatAlsoWhenChangingNonWhitelistedTable() throws Exception {
        Testing.Print.enable();
        startConnector(config -> config
                .with(Heartbeat.HEARTBEAT_INTERVAL, "100")
                .with(PostgresConnectorConfig.POLL_INTERVAL_MS, "50")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.b")
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER),
                false);
        waitForStreamingToStart();

        String statement = "CREATE SCHEMA s1;" +
                "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                "CREATE TABLE s1.b (pk SERIAL, bb integer, PRIMARY KEY(pk));" +
                "INSERT INTO s1.b (bb) VALUES (22);";

        Testing.print("Executing test statements");
        TestHelper.execute(statement);

        try {
            final AtomicInteger heartbeatCount = new AtomicInteger();
            final AtomicBoolean receivedInsert = new AtomicBoolean();
            Awaitility.await().atMost(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS).until(() -> {
                final SourceRecord record = consumeRecord();
                Testing.print("Arrived record " + record);
                if (record != null) {
                    if (record.topic().endsWith("s1.b")) {
                        assertRecordInserted(record, "s1.b", PK_FIELD, 1);
                        receivedInsert.set(true);
                    }
                    else {
                        assertHeartBeatRecord(record);
                        heartbeatCount.incrementAndGet();
                    }
                }
                return receivedInsert.get() && heartbeatCount.get() > 0;
            });
        }
        catch (ConditionTimeoutException e) {
            fail("Failed to receive insert and at least 1 heartbeat message", e);
        }

        final Set<Long> lsn = new HashSet<>();
        TestHelper.execute("INSERT INTO s1.a (aa) VALUES (11);");
        try {
            Awaitility.await().atMost(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS).until(() -> {
                final SourceRecord record = consumeRecord();
                if (record != null) {
                    lsn.add((Long) record.sourceOffset().get("lsn"));
                    return lsn.size() >= 2;
                }
                return false;
            });
        }
        catch (ConditionTimeoutException e) {
            fail("Failed to detect at least 2 LSN changes", e);
        }
        Testing.print("Done");
    }

    @Test
    @FixFor("DBZ-1565")
    public void shouldWarnOnMissingHeartbeatForFilteredEvents() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(PostgresStreamingChangeEventSource.class);
        startConnector(config -> config
                .with(PostgresConnectorConfig.POLL_INTERVAL_MS, "50")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.b")
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER),
                false);
        waitForStreamingToStart();

        String statement = "CREATE SCHEMA s1;" +
                "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                "CREATE TABLE s1.b (pk SERIAL, bb integer, PRIMARY KEY(pk));" +
                "INSERT INTO s1.a (aa) VALUES (11);" +
                "INSERT INTO s1.b (bb) VALUES (22);";

        consumer = testConsumer(1);
        executeAndWait(statement);

        final int filteredCount = 10_100;
        TestHelper.execute(
                IntStream.range(0, filteredCount)
                        .mapToObj(x -> "INSERT INTO s1.a (pk) VALUES (default);")
                        .collect(Collectors.joining()));
        Awaitility.await().alias("WAL growing log message").pollInterval(1, TimeUnit.SECONDS).atMost(5 * TestHelper.waitTimeForRecords(), TimeUnit.SECONDS)
                .until(() -> logInterceptor.containsWarnMessage(
                        "Received 10001 events which were all filtered out, so no offset could be committed. This prevents the replication slot from acknowledging the processed WAL offsets, causing a growing backlog of non-removeable WAL segments on the database server. Consider to either adjust your filter configuration or enable heartbeat events (via the heartbeat.interval.ms option) to avoid this situation."));
    }

    @Test
    @FixFor("DBZ-911")
    @SkipWhenDecoderPluginNameIs(value = PGOUTPUT, reason = "Decoder synchronizes all schema columns when processing relation messages")
    public void shouldNotRefreshSchemaOnUnchangedToastedData() throws Exception {
        startConnector(config -> config
                .with(PostgresConnectorConfig.SCHEMA_REFRESH_MODE, PostgresConnectorConfig.SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST));

        String toastedValue = RandomStringUtils.randomAlphanumeric(10000);

        // inserting a toasted value should /always/ produce a correct record
        String statement = "ALTER TABLE test_table ADD COLUMN not_toast integer; INSERT INTO test_table (not_toast, text) values (10, '" + toastedValue + "')";
        consumer = testConsumer(1);
        executeAndWait(statement);

        SourceRecord record = consumer.remove();

        // after record should contain the toasted value
        List<SchemaAndValueField> expectedAfter = Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, toastedValue));
        assertRecordSchemaAndValues(expectedAfter, record, Envelope.FieldName.AFTER);

        // now we remove the toast column and update the not_toast column to see that our unchanged toast data
        // does not trigger a table schema refresh. the after schema should look the same as before.
        statement = "ALTER TABLE test_table DROP COLUMN text; update test_table set not_toast = 5 where not_toast = 10";

        consumer.expects(1);
        executeAndWait(statement);
        assertWithTask(task -> {
            Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_table", false));
            assertEquals(Arrays.asList("pk", "text", "not_toast"), tbl.retrieveColumnNames());
        });

        TestHelper.assertNoOpenTransactions();
    }

    @Test
    @FixFor("DBZ-911")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Decoder synchronizes all schema columns when processing relation messages")
    public void shouldRefreshSchemaOnUnchangedToastedDataWhenSchemaChanged() throws Exception {
        startConnector(config -> config
                .with(PostgresConnectorConfig.SCHEMA_REFRESH_MODE, PostgresConnectorConfig.SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST));

        String toastedValue = RandomStringUtils.randomAlphanumeric(10000);

        // inserting a toasted value should /always/ produce a correct record
        String statement = "ALTER TABLE test_table ADD COLUMN not_toast integer; INSERT INTO test_table (not_toast, text) values (10, '" + toastedValue + "')";
        consumer = testConsumer(1);
        executeAndWait(statement);

        SourceRecord record = consumer.remove();

        // after record should contain the toasted value
        List<SchemaAndValueField> expectedAfter = Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, toastedValue));
        assertRecordSchemaAndValues(expectedAfter, record, Envelope.FieldName.AFTER);

        // now we remove the toast column and update the not_toast column to see that our unchanged toast data
        // does trigger a table schema refresh. the after schema should be reflect the changes
        statement = "ALTER TABLE test_table DROP COLUMN text; update test_table set not_toast = 5 where not_toast = 10";

        consumer.expects(1);
        executeAndWait(statement);
        assertWithTask(task -> {
            Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_table", false));
            assertEquals(Arrays.asList("pk", "not_toast"), tbl.retrieveColumnNames());
        });
    }

    @Test
    @FixFor("DBZ-842")
    public void shouldNotPropagateUnchangedToastedData() throws Exception {
        startConnector(config -> config
                .with(PostgresConnectorConfig.SCHEMA_REFRESH_MODE, PostgresConnectorConfig.SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST));

        final String toastedValue1 = RandomStringUtils.randomAlphanumeric(10000);
        final String toastedValue2 = RandomStringUtils.randomAlphanumeric(10000);
        final String toastedValue3 = RandomStringUtils.randomAlphanumeric(10000);

        // inserting a toasted value should /always/ produce a correct record
        String statement = "ALTER TABLE test_table ADD COLUMN not_toast integer;"
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
                new SchemaAndValueField("mandatory_text", SchemaBuilder.string().defaultValue(toastedValue3).build(), toastedValue1)), consumer.remove(),
                Envelope.FieldName.AFTER);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, toastedValue2),
                new SchemaAndValueField("mandatory_text", SchemaBuilder.string().defaultValue(toastedValue3).build(), toastedValue2)), consumer.remove(),
                Envelope.FieldName.AFTER);

        statement = "UPDATE test_table SET not_toast = 2;"
                + "UPDATE test_table SET not_toast = 3;";

        consumer.expects(6);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_table", false));
                assertEquals(Arrays.asList("pk", "text", "not_toast", "mandatory_text"), tbl.retrieveColumnNames());
            });
        });
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 2),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "insert"),
                new SchemaAndValueField("mandatory_text", SchemaBuilder.string().defaultValue(toastedValue3).build(), "")), consumer.remove(), Envelope.FieldName.AFTER);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 2),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, DecoderDifferences.optionalToastedValuePlaceholder()),
                new SchemaAndValueField("mandatory_text", SchemaBuilder.string().defaultValue(toastedValue3).build(),
                        DecoderDifferences.mandatoryToastedValuePlaceholder())),
                consumer.remove(),
                Envelope.FieldName.AFTER);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 2),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, DecoderDifferences.optionalToastedValuePlaceholder()),
                new SchemaAndValueField("mandatory_text", SchemaBuilder.string().defaultValue(toastedValue3).build(),
                        DecoderDifferences.mandatoryToastedValuePlaceholder())),
                consumer.remove(),
                Envelope.FieldName.AFTER);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 3),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "insert"),
                new SchemaAndValueField("mandatory_text", SchemaBuilder.string().defaultValue(toastedValue3).build(), "")), consumer.remove(), Envelope.FieldName.AFTER);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 3),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, DecoderDifferences.optionalToastedValuePlaceholder()),
                new SchemaAndValueField("mandatory_text", SchemaBuilder.string().defaultValue(toastedValue3).build(),
                        DecoderDifferences.mandatoryToastedValuePlaceholder())),
                consumer.remove(),
                Envelope.FieldName.AFTER);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 3),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, DecoderDifferences.optionalToastedValuePlaceholder()),
                new SchemaAndValueField("mandatory_text", SchemaBuilder.string().defaultValue(toastedValue3).build(),
                        DecoderDifferences.mandatoryToastedValuePlaceholder())),
                consumer.remove(),
                Envelope.FieldName.AFTER);
    }

    @Test
    @FixFor("DBZ-4941")
    public void shouldHandleToastedArrayColumn() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY, text TEXT);");
        startConnector(Function.identity(), false);
        final String toastedValue = RandomStringUtils.randomAlphanumeric(10000);

        String statement = "ALTER TABLE test_toast_table ADD COLUMN not_toast integer;"
                + "ALTER TABLE test_toast_table ADD COLUMN mandatory_text_array TEXT[] NOT NULL;"
                + "ALTER TABLE test_toast_table ALTER COLUMN mandatory_text_array SET STORAGE EXTENDED;"
                + "INSERT INTO test_toast_table (not_toast, text, mandatory_text_array) values (10, 'text', ARRAY ['" + toastedValue + "']);";
        consumer = testConsumer(1);
        executeAndWait(statement);

        // after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "text"),
                new SchemaAndValueField("mandatory_text_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build(), Arrays.asList(toastedValue))),
                consumer.remove(),
                Envelope.FieldName.AFTER);
        statement = "UPDATE test_toast_table SET not_toast = 2;";

        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "text", "not_toast", "mandatory_text_array"), tbl.retrieveColumnNames());
            });
        });
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 2),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "text"),
                new SchemaAndValueField("mandatory_text_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build(),
                        Arrays.asList(DecoderDifferences.mandatoryToastedValuePlaceholder()))),
                consumer.remove(),
                Envelope.FieldName.AFTER);
    }

    @Test
    @FixFor("DBZ-5935")
    public void shouldHandleToastedArrayColumnCharacterVarying() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY, text character varying(255));");
        startConnector(Function.identity(), false);
        final String toastedValue = RandomStringUtils.randomAlphanumeric(10000);

        String statement = "ALTER TABLE test_toast_table ADD COLUMN not_toast integer;"
                + "ALTER TABLE test_toast_table ADD COLUMN mandatory_text_array character varying(20000)[] NOT NULL;"
                + "ALTER TABLE test_toast_table ALTER COLUMN mandatory_text_array SET STORAGE EXTENDED;"
                + "INSERT INTO test_toast_table (not_toast, text, mandatory_text_array) values (10, 'text', ARRAY ['" + toastedValue + "']);";
        consumer = testConsumer(1);
        executeAndWait(statement);

        // after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "text"),
                new SchemaAndValueField("mandatory_text_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build(), Arrays.asList(toastedValue))),
                consumer.remove(),
                Envelope.FieldName.AFTER);
        statement = "UPDATE test_toast_table SET not_toast = 2;";

        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "text", "not_toast", "mandatory_text_array"), tbl.retrieveColumnNames());
            });
        });
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 2),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "text"),
                new SchemaAndValueField("mandatory_text_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build(),
                        Arrays.asList(DecoderDifferences.mandatoryToastedValuePlaceholder()))),
                consumer.remove(),
                Envelope.FieldName.AFTER);
    }

    @Test
    @FixFor("DBZ-6122")
    public void shouldHandleToastedDateArrayColumn() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY);");
        startConnector(Function.identity(), false);
        List<Integer> intList = IntStream.range(1, 100000).boxed().map((x) -> 19338).collect(Collectors.toList());
        final String toastedValue = intList.stream().map((x) -> "'2022-12-12'::date").collect(Collectors.joining(","));

        String statement = "ALTER TABLE test_toast_table ADD COLUMN not_toast integer;"
                + "ALTER TABLE test_toast_table ADD COLUMN date_array date[];"
                + "ALTER TABLE test_toast_table ALTER COLUMN date_array SET STORAGE EXTENDED;"
                + "INSERT INTO test_toast_table (not_toast, date_array) values (10, ARRAY [" + toastedValue + "]);";
        consumer = testConsumer(1);
        executeAndWait(statement);

        // after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("date_array",
                        SchemaBuilder.array(SchemaBuilder.int32().name("io.debezium.time.Date").optional().version(1).build()).optional().build(),
                        intList)),
                consumer.remove(),
                Envelope.FieldName.AFTER);
        statement = "UPDATE test_toast_table SET not_toast = 2;";

        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "not_toast", "date_array"), tbl.retrieveColumnNames());
            });
        });
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 2),
                new SchemaAndValueField("date_array",
                        SchemaBuilder.array(SchemaBuilder.int32().name("io.debezium.time.Date").optional().version(1).build()).optional().build(),
                        DecoderDifferences.toastedValueIntPlaceholder())),
                consumer.remove(),
                Envelope.FieldName.AFTER);
    }

    @Test
    @FixFor("DBZ-6122")
    public void shouldHandleToastedByteArrayColumn() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY);");
        startConnector(Function.identity(), false);
        List<Integer> intList = IntStream.range(1, 100000).boxed().map((x) -> 19338).collect(Collectors.toList());
        final String toastedValue = intList.stream().map((x) -> "'2022-12-12'::date").collect(Collectors.joining(","));

        String statement = "ALTER TABLE test_toast_table ADD COLUMN not_toast integer;"
                + "ALTER TABLE test_toast_table ADD COLUMN date_array date[];"
                + "ALTER TABLE test_toast_table ALTER COLUMN date_array SET STORAGE EXTENDED;"
                + "INSERT INTO test_toast_table (not_toast, date_array) values (10, ARRAY [" + toastedValue + "]);";
        consumer = testConsumer(1);
        executeAndWait(statement);

        // after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                        new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                        new SchemaAndValueField("date_array",
                                SchemaBuilder.array(SchemaBuilder.int32().name("io.debezium.time.Date").optional().version(1).build()).optional().build(),
                                intList)),
                consumer.remove(),
                Envelope.FieldName.AFTER);
        statement = "UPDATE test_toast_table SET not_toast = 2;";

        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "not_toast", "date_array"), tbl.retrieveColumnNames());
            });
        });
        assertRecordSchemaAndValues(Arrays.asList(
                        new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 2),
                        new SchemaAndValueField("date_array",
                                SchemaBuilder.array(SchemaBuilder.int32().name("io.debezium.time.Date").optional().version(1).build()).optional().build(),
                                DecoderDifferences.toastedValueIntPlaceholder())),
                consumer.remove(),
                Envelope.FieldName.AFTER);
    }
    @Test
    @FixFor("DBZ-5936")
    public void shouldHandleToastedIntegerArrayColumn() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY);");
        startConnector(Function.identity(), false);
        List<Integer> intList = IntStream.range(1, 10000).boxed().collect(Collectors.toList());
        final String toastedValue = intList.stream().map(String::valueOf)
                .collect(Collectors.joining(","));
        String statement = "ALTER TABLE test_toast_table ADD COLUMN not_toast integer;"
                + "ALTER TABLE test_toast_table ADD COLUMN int_array int[];"
                + "ALTER TABLE test_toast_table ALTER COLUMN int_array SET STORAGE EXTENDED;"
                + "INSERT INTO test_toast_table (not_toast, int_array) values (10, ARRAY [" + toastedValue + "]);";
        consumer = testConsumer(1);
        executeAndWait(statement);

        // after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("int_array", SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(), intList)),
                consumer.remove(),
                Envelope.FieldName.AFTER);
        statement = "UPDATE test_toast_table SET not_toast = 2;";

        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "not_toast", "int_array"), tbl.retrieveColumnNames());
            });
        });
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 2),
                new SchemaAndValueField("int_array", SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(),
                        DecoderDifferences.toastedValueIntPlaceholder())),
                consumer.remove(),
                Envelope.FieldName.AFTER);
    }

    @Test
    @FixFor("DBZ-5936")
    public void shouldHandleToastedBigIntArrayColumn() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY);");
        startConnector(Function.identity(), false);
        List<Long> bigintList = LongStream.range(1, 10000).boxed().collect(Collectors.toList());
        final String toastedValue = bigintList.stream().map(String::valueOf)
                .collect(Collectors.joining(","));
        String statement = "ALTER TABLE test_toast_table ADD COLUMN not_toast integer;"
                + "ALTER TABLE test_toast_table ADD COLUMN bigint_array bigint[];"
                + "ALTER TABLE test_toast_table ALTER COLUMN bigint_array SET STORAGE EXTENDED;"
                + "INSERT INTO test_toast_table (not_toast, bigint_array) values (10, ARRAY [" + toastedValue + "]);";
        consumer = testConsumer(1);
        executeAndWait(statement);

        // after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("bigint_array", SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build(), bigintList)),
                consumer.remove(),
                Envelope.FieldName.AFTER);
        statement = "UPDATE test_toast_table SET not_toast = 2;";

        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "not_toast", "bigint_array"), tbl.retrieveColumnNames());
            });
        });
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 2),
                new SchemaAndValueField("bigint_array", SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build(),
                        DecoderDifferences.toastedValueBigintPlaceholder())),
                consumer.remove(),
                Envelope.FieldName.AFTER);
    }

    @Test
    @FixFor("DBZ-5936")
    public void shouldHandleToastedJsonArrayColumn() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY, text TEXT);");
        startConnector(Function.identity(), false);
        final String toastedValue = RandomStringUtils.randomAlphanumeric(10000);

        String statement = "ALTER TABLE test_toast_table ADD COLUMN not_toast integer;"
                + "ALTER TABLE test_toast_table ADD COLUMN json_array json[];"
                + "ALTER TABLE test_toast_table ALTER COLUMN json_array SET STORAGE EXTENDED;"
                + "INSERT INTO test_toast_table (not_toast, text, json_array) "
                + "VALUES (10, 'text', ARRAY [ '{\"key\": \"" + toastedValue + "\" }'::json ]);";
        consumer = testConsumer(1);
        executeAndWait(statement);

        // after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "text"),
                new SchemaAndValueField("json_array", SchemaBuilder.array(
                        io.debezium.data.Json.builder().optional().build()).optional().build(),
                        Arrays.asList("{\"key\": \"" + toastedValue + "\" }"))),
                consumer.remove(),
                Envelope.FieldName.AFTER);
        statement = "UPDATE test_toast_table SET not_toast = 2;";

        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "text", "not_toast", "json_array"), tbl.retrieveColumnNames());
            });
        });
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 2),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "text"),
                new SchemaAndValueField("json_array", SchemaBuilder.array(
                        io.debezium.data.Json.builder().optional().build()).optional().build(),
                        Arrays.asList(DecoderDifferences.mandatoryToastedValuePlaceholder()))),
                consumer.remove(),
                Envelope.FieldName.AFTER);
    }

    @Test
    @FixFor("DBZ-1029")
    public void shouldReceiveChangesForTableWithoutPrimaryKey() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_table;",
                "CREATE TABLE test_table (id SERIAL, text TEXT);",
                "ALTER TABLE test_table REPLICA IDENTITY FULL");

        startConnector(Function.identity(), false);
        consumer = testConsumer(1);

        // INSERT
        String statement = "INSERT INTO test_table (text) VALUES ('a');";
        assertInsert(
                statement,
                Arrays.asList(
                        new SchemaAndValueField("id", SchemaBuilder.int32().defaultValue(0).build(), 1), // SERIAL is NOT NULL implicitly
                        new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "a")));

        // UPDATE
        consumer.expects(1);
        executeAndWait("UPDATE test_table set text='b' WHERE id=1");
        SourceRecord updatedRecord = consumer.remove();
        VerifyRecord.isValidUpdate(updatedRecord);

        List<SchemaAndValueField> expectedBefore = Arrays.asList(
                new SchemaAndValueField("id", SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "a"));
        assertRecordSchemaAndValues(expectedBefore, updatedRecord, Envelope.FieldName.BEFORE);

        List<SchemaAndValueField> expectedAfter = Arrays.asList(
                new SchemaAndValueField("id", SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "b"));
        assertRecordSchemaAndValues(expectedAfter, updatedRecord, Envelope.FieldName.AFTER);

        // DELETE
        consumer.expects(2);
        executeAndWait("DELETE FROM test_table WHERE id=1");
        SourceRecord deletedRecord = consumer.remove();
        VerifyRecord.isValidDelete(deletedRecord);

        expectedBefore = Arrays.asList(
                new SchemaAndValueField("id", SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "b"));
        assertRecordSchemaAndValues(expectedBefore, deletedRecord, Envelope.FieldName.BEFORE);

        expectedAfter = null;
        assertRecordSchemaAndValues(expectedAfter, deletedRecord, Envelope.FieldName.AFTER);
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
    public void testEmptyChangesProducesHeartbeat() throws Exception {
        // the low heartbeat interval should make sure that a heartbeat message is emitted after each change record
        // received from Postgres
        startConnector(config -> config.with(Heartbeat.HEARTBEAT_INTERVAL, "100"));
        waitForStreamingToStart();

        TestHelper.execute(
                "DROP TABLE IF EXISTS test_table;" +
                        "CREATE TABLE test_table (id SERIAL, text TEXT);" +
                        "INSERT INTO test_table (text) VALUES ('mydata');");

        // Expecting 1 data change
        Awaitility.await().atMost(TestHelper.waitTimeForRecords() * 10, TimeUnit.SECONDS).until(() -> {
            final SourceRecord record = consumeRecord();
            return record != null && Envelope.isEnvelopeSchema(record.valueSchema());
        });

        // Wait for heartbeat that is emitted after the data change
        // This is necessary to make sure that timing does not influence the lsn count check
        final Set<Long> lsns = new HashSet<>();
        Awaitility.await().atMost(TestHelper.waitTimeForRecords() * 10, TimeUnit.SECONDS).until(() -> {
            final SourceRecord record = consumeRecord();
            if (record == null) {
                return false;
            }
            assertThat(record.valueSchema().name()).endsWith(".Heartbeat");
            lsns.add((Long) record.sourceOffset().get("lsn"));
            return true;
        });

        // Expecting one empty DDL change
        String statement = "CREATE SCHEMA s1;";

        TestHelper.execute(statement);

        // Expecting changes for the empty DDL change
        Awaitility.await().atMost(TestHelper.waitTimeForRecords() * 10, TimeUnit.SECONDS).until(() -> {
            final SourceRecord record = consumeRecord();
            assertThat(record.valueSchema().name()).endsWith(".Heartbeat");
            lsns.add((Long) record.sourceOffset().get("lsn"));
            // CREATE SCHEMA should change LSN
            return lsns.size() == 2;
        });
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

    @Test
    @FixFor("DBZ-1824")
    public void stopInTheMiddleOfTxAndResume() throws Exception {
        Testing.Print.enable();
        final int numberOfEvents = 50;
        final int STOP_ID = 20;

        startConnector(config -> config.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false), true, record -> {
            if (!"test_server.public.test_table.Envelope".equals(record.valueSchema().name())) {
                return false;
            }
            final Struct envelope = (Struct) record.value();
            final Struct after = envelope.getStruct("after");
            final Integer pk = after.getInt32("pk");
            return pk == STOP_ID;
        });
        waitForStreamingToStart();

        final String topicPrefix = "public.test_table";
        final String topicName = topicName(topicPrefix);

        final int expectFirstRun = STOP_ID - 2;
        final int expectSecondRun = numberOfEvents - STOP_ID;
        consumer = testConsumer(expectFirstRun);
        executeAndWait(IntStream.rangeClosed(2, numberOfEvents + 1)
                .boxed()
                .map(x -> "INSERT INTO test_table (text) VALUES ('insert" + x + "')")
                .collect(Collectors.joining(";")));

        // 2..19, 1 is from snapshot
        for (int i = 0; i < expectFirstRun; i++) {
            SourceRecord record = consumer.remove();
            assertEquals(topicName, record.topic());
            VerifyRecord.isValidInsert(record, PK_FIELD, i + 2);
        }

        stopConnector();

        startConnector(Function.identity(), false);
        consumer.expects(expectSecondRun);
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        // 20..51
        for (int i = 0; i < expectSecondRun; i++) {
            SourceRecord record = consumer.remove();
            assertEquals(topicName, record.topic());
            VerifyRecord.isValidInsert(record, PK_FIELD, STOP_ID + i);
        }
    }

    @Test
    @FixFor("DBZ-2397")
    public void restartConnectorInTheMiddleOfUncommittedTx() throws Exception {
        Testing.Print.enable();

        final PostgresConnection tx1Connection = TestHelper.create();
        tx1Connection.setAutoCommit(false);

        final PostgresConnection tx2Connection = TestHelper.create();
        tx2Connection.setAutoCommit(true);

        startConnector(config -> config.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false), true);
        waitForStreamingToStart();

        tx1Connection.executeWithoutCommitting("INSERT INTO test_table (text) VALUES ('tx-1-1')");
        tx2Connection.execute("INSERT INTO test_table (text) VALUES ('tx-2-1')");
        consumer = testConsumer(1);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        assertThat(((Struct) consumer.remove().value()).getStruct("after").getString("text")).isEqualTo("tx-2-1");

        stopConnector();
        startConnector(Function.identity(), false);
        waitForStreamingToStart();

        tx1Connection.executeWithoutCommitting("INSERT INTO test_table (text) VALUES ('tx-1-2')");
        tx2Connection.execute("INSERT INTO test_table (text) VALUES ('tx-2-2')");

        tx1Connection.executeWithoutCommitting("INSERT INTO test_table (text) VALUES ('tx-1-3')");
        tx2Connection.execute("INSERT INTO test_table (text) VALUES ('tx-2-3')");

        tx1Connection.commit();

        consumer = testConsumer(5);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        assertThat(((Struct) consumer.remove().value()).getStruct("after").getString("text")).isEqualTo("tx-2-2");
        assertThat(((Struct) consumer.remove().value()).getStruct("after").getString("text")).isEqualTo("tx-2-3");

        assertThat(((Struct) consumer.remove().value()).getStruct("after").getString("text")).isEqualTo("tx-1-1");
        assertThat(((Struct) consumer.remove().value()).getStruct("after").getString("text")).isEqualTo("tx-1-2");
        assertThat(((Struct) consumer.remove().value()).getStruct("after").getString("text")).isEqualTo("tx-1-3");
    }

    @Test
    @FixFor("DBZ-1730")
    public void shouldStartConsumingFromSlotLocation() throws Exception {
        Testing.Print.enable();

        startConnector(config -> config
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false)
                .with(EmbeddedEngine.OFFSET_STORAGE, MemoryOffsetBackingStore.class), true);
        waitForStreamingToStart();

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO test_table (text) VALUES ('insert2')");
        consumer.remove();

        stopConnector();
        TestHelper.execute(
                "INSERT INTO test_table (text) VALUES ('insert3');",
                "INSERT INTO test_table (text) VALUES ('insert4')");
        startConnector(config -> config
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, true)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER)
                .with(EmbeddedEngine.OFFSET_STORAGE, MemoryOffsetBackingStore.class), false);

        consumer.expects(3);
        consumer.await(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS);

        // After loss of offset and not doing snapshot we always stream the first record available in replication slot
        // even if we have seen it as it is not possible to make a difference from plain snapshot never mode
        assertThat(((Struct) consumer.remove().value()).getStruct("after").getString("text")).isEqualTo("insert2");

        assertThat(((Struct) consumer.remove().value()).getStruct("after").getString("text")).isEqualTo("insert3");
        assertThat(((Struct) consumer.remove().value()).getStruct("after").getString("text")).isEqualTo("insert4");

        stopConnector();
    }

    @Test
    @SkipWhenDatabaseVersion(check = EqualityCheck.LESS_THAN, major = 11, reason = "TRUNCATE events only supported in PG11+ PGOUTPUT Plugin")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Tests specifically that pgoutput handles TRUNCATE messages")
    public void shouldProcessTruncateMessages() throws Exception {
        startConnector(builder -> builder
                .with(PostgresConnectorConfig.SKIPPED_OPERATIONS, "none"));
        waitForStreamingToStart();

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO test_table (text) values ('TRUNCATE TEST');");

        SourceRecord record = consumer.remove();
        assertEquals(TestHelper.topicName("public.test_table"), record.topic());
        VerifyRecord.isValidInsert(record, PK_FIELD, 2);

        consumer.expects(1);
        TestHelper.execute("TRUNCATE TABLE public.test_table RESTART IDENTITY CASCADE;");
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        assertFalse(consumer.isEmpty());
        SourceRecord truncateRecord = consumer.remove();
        assertNotNull(truncateRecord);
        VerifyRecord.isValidTruncate(truncateRecord);
        assertTrue(consumer.isEmpty());
    }

    @Test
    @SkipWhenDatabaseVersion(check = EqualityCheck.LESS_THAN, major = 11, reason = "TRUNCATE events only supported in PG11+ PGOUTPUT Plugin")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Tests specifically that pgoutput handles TRUNCATE messages")
    public void shouldProcessTruncateMessagesWhenSkippedOperationsIsSuppliedWithoutTruncate() throws Exception {
        startConnector(builder -> builder
                .with(PostgresConnectorConfig.SKIPPED_OPERATIONS, "u"));
        waitForStreamingToStart();

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO test_table (text) values ('TRUNCATE TEST');");

        SourceRecord record = consumer.remove();
        assertEquals(TestHelper.topicName("public.test_table"), record.topic());
        VerifyRecord.isValidInsert(record, PK_FIELD, 2);

        consumer.expects(1);
        TestHelper.execute("TRUNCATE TABLE public.test_table RESTART IDENTITY CASCADE;");
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        assertFalse(consumer.isEmpty());
        SourceRecord truncateRecord = consumer.remove();
        assertNotNull(truncateRecord);
        VerifyRecord.isValidTruncate(truncateRecord);
        assertTrue(consumer.isEmpty());
    }

    @Test
    @SkipWhenDatabaseVersion(check = EqualityCheck.LESS_THAN, major = 11, reason = "TRUNCATE events only supported in PG11+ PGOUTPUT Plugin")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Tests specifically that pgoutput handles TRUNCATE messages")
    public void shouldSkipTruncateMessagesWithSkipped() throws Exception {
        startConnector(builder -> builder.with(PostgresConnectorConfig.SKIPPED_OPERATIONS, "t"));
        waitForStreamingToStart();

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO test_table (text) values ('TRUNCATE TEST');");

        SourceRecord record = consumer.remove();
        assertEquals(TestHelper.topicName("public.test_table"), record.topic());
        VerifyRecord.isValidInsert(record, PK_FIELD, 2);

        consumer.expects(0);
        TestHelper.execute("TRUNCATE TABLE public.test_table RESTART IDENTITY CASCADE;");
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        assertTrue(consumer.isEmpty());
    }

    @Test
    @SkipWhenDatabaseVersion(check = EqualityCheck.LESS_THAN, major = 11, reason = "TRUNCATE events only supported in PG11+ PGOUTPUT Plugin")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Tests specifically that pgoutput handled TRUNCATE these messages")
    public void shouldProcessTruncateMessagesForMultipleTableTruncateStatement() throws Exception {
        TestHelper.execute("CREATE TABLE test_table_2 (pk SERIAL, text TEXT, PRIMARY KEY(pk));");

        startConnector(builder -> builder.with(PostgresConnectorConfig.SKIPPED_OPERATIONS, "none"));
        waitForStreamingToStart();

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO test_table (text) values ('TRUNCATE TEST');");

        SourceRecord record = consumer.remove();
        assertEquals(TestHelper.topicName("public.test_table"), record.topic());
        VerifyRecord.isValidInsert(record, PK_FIELD, 2);

        executeAndWait("INSERT INTO test_table_2 (text) values ('TRUNCATE TEST 2');");

        SourceRecord record_2 = consumer.remove();
        assertEquals(TestHelper.topicName("public.test_table_2"), record_2.topic());
        VerifyRecord.isValidInsert(record_2, PK_FIELD, 1);

        consumer.expects(2);
        TestHelper.execute("TRUNCATE TABLE public.test_table, public.test_table_2;");
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        assertFalse(consumer.isEmpty());

        SourceRecord truncateRecord = consumer.remove();
        assertNotNull(truncateRecord);
        VerifyRecord.isValidTruncate(truncateRecord);

        SourceRecord truncateRecord_2 = consumer.remove();
        assertNotNull(truncateRecord_2);
        VerifyRecord.isValidTruncate(truncateRecord_2);
        assertTrue(consumer.isEmpty());

        assertEquals(truncateRecord.sourceOffset().get("lsn_commit"), truncateRecord_2.sourceOffset().get("lsn_commit"));
        assertEquals(truncateRecord.sourceOffset().get("lsn"), truncateRecord_2.sourceOffset().get("lsn"));
        assertEquals(truncateRecord.sourceOffset().get("txId"), truncateRecord_2.sourceOffset().get("txId"));

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO test_table (text) values ('TRUNCATE TEST');");
    }

    @Test
    @FixFor("DBZ-1413")
    public void shouldStreamChangesForDataTypeAlias() throws Exception {
        TestHelper.execute("CREATE DOMAIN money2 AS money DEFAULT 0.0;");
        TestHelper.execute("CREATE TABLE alias_table (pk SERIAL, data VARCHAR(50), salary money, salary2 money2, PRIMARY KEY(pk));");

        startConnector(config -> config
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.PRECISE)
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.alias_table"),
                false);

        waitForStreamingToStart();

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO alias_table (data, salary, salary2) values ('hello', 7.25, 8.25);");

        SourceRecord rec = assertRecordInserted("public.alias_table", PK_FIELD, 1);
        assertSourceInfo(rec, "postgres", "public", "alias_table");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField("pk", SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("data", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "hello"),
                new SchemaAndValueField("salary", Decimal.builder(2).optional().build(), new BigDecimal(7.25)),
                new SchemaAndValueField("salary2", Decimal.builder(2).optional().build(), new BigDecimal(8.25)));

        assertRecordSchemaAndValues(expected, rec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    @FixFor("DBZ-1413")
    public void shouldStreamChangesForDomainAliasAlterTable() throws Exception {
        TestHelper.execute("CREATE TABLE alias_table (pk SERIAL, data VARCHAR(50), salary money, PRIMARY KEY(pk));");
        startConnector(config -> config
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE)
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.alias_table")
                .with("column.propagate.source.type", "public.alias_table.salary3"),
                false);

        waitForStreamingToStart();

        // Now that streaming has started, alter the table schema
        TestHelper.execute("CREATE DOMAIN money2 AS money DEFAULT 0.0;");
        TestHelper.execute("CREATE DOMAIN money3 AS numeric(8,3) DEFAULT 0.0;");
        TestHelper.execute("ALTER TABLE alias_table ADD COLUMN salary2 money2 NOT NULL;");
        TestHelper.execute("ALTER TABLE alias_table ADD COLUMN salary3 money3 NOT NULL;");

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO alias_table (data, salary, salary2, salary3) values ('hello', 7.25, 8.25, 123.456);");

        SourceRecord rec = assertRecordInserted("public.alias_table", PK_FIELD, 1);
        assertSourceInfo(rec, "postgres", "public", "alias_table");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField("pk", SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("data", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "hello"),
                new SchemaAndValueField("salary", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA, 7.25),
                new SchemaAndValueField("salary2", SchemaBuilder.FLOAT64_SCHEMA, 8.25),
                new SchemaAndValueField("salary3", SchemaBuilder.float64()
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "MONEY3")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, "8")
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "3")
                        .build(), 123.456));

        assertRecordSchemaAndValues(expected, rec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    @FixFor("DBZ-1413")
    public void shouldStreamDomainAliasWithProperModifiers() throws Exception {
        TestHelper.execute("CREATE TABLE alias_table (pk SERIAL, PRIMARY KEY(pk));");
        startConnector(config -> config
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE)
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.alias_table"),
                false);

        waitForStreamingToStart();

        TestHelper.execute("CREATE DOMAIN varbit2 AS varbit(3);");
        TestHelper.execute("ALTER TABLE public.alias_table ADD COLUMN value varbit2 NOT NULL;");

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO public.alias_table (value) VALUES (B'101');");

        SourceRecord rec = assertRecordInserted("public.alias_table", PK_FIELD, 1);
        assertSourceInfo(rec, "postgres", "public", "alias_table");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField(PK_FIELD, SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("value", Bits.builder(3).build(), new byte[]{ 5 }));

        assertRecordSchemaAndValues(expected, rec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    @FixFor("DBZ-1413")
    public void shouldStreamValuesForDomainTypeOfDomainType() throws Exception {
        TestHelper.execute("CREATE DOMAIN numeric82 as numeric(8,2);");
        TestHelper.execute("CREATE DOMAIN numericex as numeric82;");
        TestHelper.execute("CREATE TABLE alias_table (pk SERIAL, value numericex, PRIMARY KEY (pk));");
        startConnector(config -> config
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE)
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.alias_table")
                .with("column.propagate.source.type", "public.alias_table.value"), false);

        waitForStreamingToStart();

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO alias_table (value) values (123.45);");

        SourceRecord rec = assertRecordInserted("public.alias_table", PK_FIELD, 1);
        assertSourceInfo(rec, "postgres", "public", "alias_table");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField(PK_FIELD, SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("value", SpecialValueDecimal.builder(DecimalMode.DOUBLE, 8, 2)
                        .optional()
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "NUMERICEX")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, "8")
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "2")
                        .build(), 123.45));

        assertRecordSchemaAndValues(expected, rec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    @FixFor("DBZ-1413")
    public void shouldStreamValuesForAliasLikeBaseTypes() throws Exception {
        TestHelper.execute("CREATE TABLE alias_table (pk SERIAL, PRIMARY KEY (pk));");
        startConnector(config -> config
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE)
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.alias_table"),
                false);

        waitForStreamingToStart();

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

        TestHelper.execute("ALTER TABLE alias_table "
                + "ADD COLUMN bit_base bit(3) NOT NULL, ADD COLUMN bit_alias bit2 NOT NULL, "
                + "ADD COLUMN smallint_base smallint NOT NULL, ADD COLUMN smallint_alias smallint2 NOT NULL, "
                + "ADD COLUMN integer_base integer NOT NULL, ADD COLUMN integer_alias integer2 NOT NULL, "
                + "ADD COLUMN bigint_base bigint NOT NULL, ADD COLUMN bigint_alias bigint2 NOT NULL, "
                + "ADD COLUMN real_base real NOT NULL, ADD COLUMN real_alias real2 NOT NULL, "
                + "ADD COLUMN float8_base float8 NOT NULL, ADD COLUMN float8_alias float82 NOT NULL, "
                + "ADD COLUMN numeric_base numeric(6,2) NOT NULL, ADD COLUMN numeric_alias numeric2 NOT NULL, "
                + "ADD COLUMN bool_base bool NOT NULL, ADD COLUMN bool_alias bool2 NOT NULL, "
                + "ADD COLUMN string_base varchar(25) NOT NULL, ADD COLUMN string_alias string2 NOT NULL, "
                + "ADD COLUMN date_base date NOT NULL, ADD COLUMN date_alias date2 NOT NULL, "
                + "ADD COLUMN time_base time NOT NULL, ADD COLUMN time_alias time2 NOT NULL, "
                + "ADD COLUMN timetz_base timetz NOT NULL, ADD COLUMN timetz_alias timetz2 NOT NULL, "
                + "ADD COLUMN timestamp_base timestamp NOT NULL, ADD COLUMN timestamp_alias timestamp2 NOT NULL, "
                + "ADD COLUMN timestamptz_base timestamptz NOT NULL, ADD COLUMN timestamptz_alias timestamptz2 NOT NULL, "
                + "ADD COLUMN timewottz_base time without time zone NOT NULL, ADD COLUMN timewottz_alias timewotz2 NOT NULL, "
                + "ADD COLUMN box_base box NOT NULL, ADD COLUMN box_alias box2 NOT NULL, "
                + "ADD COLUMN circle_base circle NOT NULL, ADD COLUMN circle_alias circle2 NOT NULL, "
                + "ADD COLUMN interval_base interval NOT NULL, ADD COLUMN interval_alias interval2 NOT NULL, "
                + "ADD COLUMN line_base line NOT NULL, ADD COLUMN line_alias line2 NOT NULL, "
                + "ADD COLUMN lseg_base lseg NOT NULL, ADD COLUMN lseg_alias lseg2 NOT NULL, "
                + "ADD COLUMN path_base path NOT NULL, ADD COLUMN path_alias path2 NOT NULL, "
                + "ADD COLUMN point_base point NOT NULL, ADD COLUMN point_alias point2 NOT NULL, "
                + "ADD COLUMN polygon_base polygon NOT NULL, ADD COLUMN polygon_alias polygon2 NOT NULL, "
                + "ADD COLUMN char_base char NOT NULL, ADD COLUMN char_alias char2 NOT NULL, "
                + "ADD COLUMN text_base text NOT NULL, ADD COLUMN text_alias text2 NOT NULL, "
                + "ADD COLUMN json_base json NOT NULL, ADD COLUMN json_alias json2 NOT NULL, "
                + "ADD COLUMN xml_base xml NOT NULL, ADD COLUMN xml_alias xml2 NOT NULL, "
                + "ADD COLUMN uuid_base UUID NOT NULL, ADD COLUMN uuid_alias uuid2 NOT NULL, "
                + "ADD COLUMN varbit_base varbit(3) NOT NULL, ADD COLUMN varbit_alias varbit2 NOT NULL,"
                + "ADD COLUMN inet_base inet NOT NULL, ADD COLUMN inet_alias inet2 NOT NULL, "
                + "ADD COLUMN cidr_base cidr NOT NULL, ADD COLUMN cidr_alias cidr2 NOT NULL, "
                + "ADD COLUMN macaddr_base macaddr NOT NULL, ADD COLUMN macaddr_alias macaddr2 NOT NULL");

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO alias_table ("
                + "bit_base, bit_alias, "
                + "smallint_base, smallint_alias, "
                + "integer_base, integer_alias, "
                + "bigint_base, bigint_alias, "
                + "real_base, real_alias, "
                + "float8_base, float8_alias, "
                + "numeric_base, numeric_alias, "
                + "bool_base, bool_alias, "
                + "string_base, string_alias, "
                + "date_base, date_alias, "
                + "time_base, time_alias, "
                + "timetz_base, timetz_alias, "
                + "timestamp_base, timestamp_alias, "
                + "timestamptz_base, timestamptz_alias, "
                + "timewottz_base, timewottz_alias, "
                + "box_base, box_alias, "
                + "circle_base, circle_alias, "
                + "interval_base, interval_alias, "
                + "line_base, line_alias, "
                + "lseg_base, lseg_alias, "
                + "path_base, path_alias, "
                + "point_base, point_alias, "
                + "polygon_base, polygon_alias, "
                + "char_base, char_alias, "
                + "text_base, text_alias, "
                + "json_base, json_alias, "
                + "xml_base, xml_alias, "
                + "uuid_base, uuid_alias, "
                + "varbit_base, varbit_alias, "
                + "inet_base, inet_alias, "
                + "cidr_base, cidr_alias, "
                + "macaddr_base, macaddr_alias "
                + ") VALUES ("
                + "B'101', B'101', "
                + "1, 1, "
                + "1, 1, "
                + "1000, 1000, "
                + "3.14, 3.14, "
                + "3.14, 3.14, "
                + "1234.12, 1234.12, "
                + "true, true, "
                + "'hello', 'hello', "
                + "'2019-10-02', '2019-10-02', "
                + "'01:02:03', '01:02:03', "
                + "'01:02:03.123789Z', '01:02:03.123789Z', "
                + "'2019-10-02T01:02:03.123456', '2019-10-02T01:02:03.123456', "
                + "'2019-10-02T13:51:30.123456+02:00'::TIMESTAMPTZ, '2019-10-02T13:51:30.123456+02:00'::TIMESTAMPTZ, "
                + "'01:02:03', '01:02:03', "
                + "'(0,0),(1,1)', '(0,0),(1,1)', "
                + "'10,4,10', '10,4,10', "
                + "'1 year 2 months 3 days 4 hours 5 minutes 6 seconds', '1 year 2 months 3 days 4 hours 5 minutes 6 seconds', "
                + "'(0,0),(0,1)', '(0,0),(0,1)', "
                + "'((0,0),(0,1))', '((0,0),(0,1))', "
                + "'((0,0),(0,1),(0,2))', '((0,0),(0,1),(0,2))', "
                + "'(1,1)', '(1,1)', "
                + "'((0,0),(0,1),(1,0),(0,0))', '((0,0),(0,1),(1,0),(0,0))', "
                + "'a', 'a', "
                + "'Hello World', 'Hello World', "
                + "'{\"key\": \"value\"}', '{\"key\": \"value\"}', "
                + "XML('<foo>Hello</foo>'), XML('<foo>Hello</foo>'), "
                + "'40e6215d-b5c6-4896-987c-f30f3678f608', '40e6215d-b5c6-4896-987c-f30f3678f608', "
                + "B'101', B'101', "
                + "'192.168.0.1', '192.168.0.1', "
                + "'192.168/24', '192.168/24', "
                + "'08:00:2b:01:02:03', '08:00:2b:01:02:03' "
                + ");");

        SourceRecord rec = assertRecordInserted("public.alias_table", PK_FIELD, 1);
        assertSourceInfo(rec, "postgres", "public", "alias_table");

        assertRecordSchemaAndValues(schemasAndValuesForDomainAliasTypes(true), rec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    @FixFor("DBZ-920")
    public void shouldStreamEnumAsKnownType() throws Exception {
        // Specifically enable `column.propagate.source.type` here to validate later that the actual
        // type, length, and scale values are resolved correctly when paired with Enum types.
        TestHelper.execute("CREATE TABLE enum_table (pk SERIAL, PRIMARY KEY (pk));");
        startConnector(config -> config
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with("column.propagate.source.type", "public.enum_table.value")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.enum_table"), false);

        waitForStreamingToStart();

        // We create the enum type after streaming started to simulate some future schema change
        TestHelper.execute("CREATE TYPE test_type AS ENUM ('V1','V2');");
        TestHelper.execute("ALTER TABLE enum_table ADD COLUMN value test_type NOT NULL");

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO enum_table (value) VALUES ('V1');");

        SourceRecord rec = assertRecordInserted("public.enum_table", PK_FIELD, 1);
        assertSourceInfo(rec, "postgres", "public", "enum_table");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField(PK_FIELD, SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("value", Enum.builder("V1,V2")
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "TEST_TYPE")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, String.valueOf(Integer.MAX_VALUE))
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                        .build(), "V1"));

        assertRecordSchemaAndValues(expected, rec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    @FixFor("DBZ-5038")
    public void shouldEmitEnumColumnDefaultValuesInSchema() throws Exception {
        // Specifically enable `column.propagate.source.type` here to validate later that the actual
        // type, length, and scale values are resolved correctly when paired with Enum types.
        TestHelper.execute("CREATE TABLE enum_table (pk SERIAL, PRIMARY KEY (pk));");
        startConnector(config -> config
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with("column.propagate.source.type", "public.enum_table.value")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.enum_table"), false);

        waitForStreamingToStart();

        // We create the enum type after streaming started to simulate some future schema change
        TestHelper.execute("CREATE TYPE test_type AS ENUM ('V1','V2');");
        TestHelper.execute("ALTER TABLE enum_table ADD COLUMN data varchar(50) NOT NULL");
        TestHelper.execute("ALTER TABLE enum_table ADD COLUMN value test_type NOT NULL DEFAULT 'V2'::test_type");

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO enum_table (data) VALUES ('V1');");

        SourceRecord rec = assertRecordInserted("public.enum_table", PK_FIELD, 1);
        assertSourceInfo(rec, "postgres", "public", "enum_table");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField(PK_FIELD, SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("data", SchemaBuilder.string().build(), "V1"),
                new SchemaAndValueField("value", Enum.builder("V1,V2")
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "TEST_TYPE")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, String.valueOf(Integer.MAX_VALUE))
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                        .defaultValue("V2")
                        .build(), "V2"));

        assertRecordSchemaAndValues(expected, rec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    public void shouldStreamEnumArrayAsKnownType() throws Exception {
        // Specifically enable `column.propagate.source.type` here to validate later that the actual
        // type, length, and scale values are resolved correctly when paired with Enum types.
        TestHelper.execute("CREATE TABLE enum_array_table (pk SERIAL, PRIMARY KEY (pk));");
        startConnector(config -> config
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, false)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with("column.propagate.source.type", "public.enum_array_table.value")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.enum_array_table"), false);

        waitForStreamingToStart();

        // We create the enum type after streaming started to simulate some future schema change
        TestHelper.execute("CREATE TYPE test_type AS ENUM ('V1','V2');");
        TestHelper.execute("ALTER TABLE enum_array_table ADD COLUMN value test_type[] NOT NULL;");

        consumer = testConsumer(1);

        // INSERT
        executeAndWait("INSERT INTO enum_array_table (value) VALUES ('{V1, V2}');");

        SourceRecord insertRec = assertRecordInserted("public.enum_array_table", PK_FIELD, 1);
        assertSourceInfo(insertRec, "postgres", "public", "enum_array_table");

        List<SchemaAndValueField> expectedInsert = Arrays.asList(
                new SchemaAndValueField(PK_FIELD, SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("value", SchemaBuilder.array(Enum.builder("V1,V2"))
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "_TEST_TYPE")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, String.valueOf(Integer.MAX_VALUE))
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                        .build(), Arrays.asList("V1", "V2")));
        assertRecordSchemaAndValues(expectedInsert, insertRec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();

        // UPDATE
        executeAndWait("UPDATE enum_array_table set value = '{V1}';");
        SourceRecord updateRec = consumer.remove();
        assertSourceInfo(updateRec, "postgres", "public", "enum_array_table");

        List<SchemaAndValueField> expectedUpdate = Arrays.asList(
                new SchemaAndValueField(PK_FIELD, SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("value", SchemaBuilder.array(Enum.builder("V1,V2"))
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "_TEST_TYPE")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, String.valueOf(Integer.MAX_VALUE))
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                        .build(), Arrays.asList("V1")));
        assertRecordSchemaAndValues(expectedUpdate, updateRec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();

        // DELETE
        executeAndWait("DELETE FROM enum_array_table;");
        SourceRecord deleteRec = consumer.remove();
        VerifyRecord.isValidDelete(deleteRec, PK_FIELD, 1);
        assertSourceInfo(updateRec, "postgres", "public", "enum_array_table");
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    @FixFor("DBZ-1969")
    public void shouldStreamTimeArrayTypesAsKnownTypes() throws Exception {
        TestHelper.execute("CREATE TABLE time_array_table (pk SERIAL, "
                + "timea time[] NOT NULL, "
                + "timetza timetz[] NOT NULL, "
                + "timestampa timestamp[] NOT NULL, "
                + "timestamptza timestamptz[] NOT NULL, primary key(pk));");
        startConnector(config -> config
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, false)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.time_array_table"), false);

        waitForStreamingToStart();

        consumer = testConsumer(1);

        // INSERT
        executeAndWait("INSERT INTO time_array_table (timea, timetza, timestampa, timestamptza) "
                + "values ("
                + "'{00:01:02,01:02:03}', "
                + "'{13:51:02+0200,14:51:03+0200}', "
                + "'{2020-04-01 00:01:02,2020-04-01 01:02:03}', "
                + "'{2020-04-01 13:51:02+02,2020-04-01 14:51:03+02}')");

        SourceRecord insert = assertRecordInserted("public.time_array_table", PK_FIELD, 1);
        assertSourceInfo(insert, "postgres", "public", "time_array_table");
        assertRecordSchemaAndValues(schemaAndValuesForTimeArrayTypes(), insert, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();

        // UPDATE
        executeAndWait("UPDATE time_array_table SET "
                + "timea = '{00:01:02,02:03:04}', "
                + "timetza = '{00:01:02-0400,01:03:04-0400}', "
                + "timestampa = '{2020-04-01 00:01:02,2020-04-25 03:04:05}', "
                + "timestamptza = '{2020-04-01 00:01:02-04,2020-04-25 03:04:05-04}'");

        SourceRecord update = consumer.remove();
        assertSourceInfo(update, "postgres", "public", "time_array_table");

        List<SchemaAndValueField> expectedUpdate = Arrays.asList(
                new SchemaAndValueField("timea",
                        SchemaBuilder.array(MicroTime.builder().optional().build()).build(),
                        Arrays.asList(LocalTime.parse("00:01:02").toNanoOfDay() / 1_000,
                                LocalTime.parse("02:03:04").toNanoOfDay() / 1_000)),
                new SchemaAndValueField("timetza",
                        SchemaBuilder.array(ZonedTime.builder().optional().build()).build(),
                        Arrays.asList("04:01:02Z", "05:03:04Z")),
                new SchemaAndValueField("timestampa",
                        SchemaBuilder.array(MicroTimestamp.builder().optional().build()).build(),
                        Arrays.asList(OffsetDateTime.of(2020, 4, 1, 0, 1, 2, 0, ZoneOffset.UTC).toInstant().toEpochMilli() * 1_000,
                                OffsetDateTime.of(2020, 4, 25, 3, 4, 5, 0, ZoneOffset.UTC).toInstant().toEpochMilli() * 1_000)),
                new SchemaAndValueField("timestamptza",
                        SchemaBuilder.array(ZonedTimestamp.builder().optional().build()).build(),
                        Arrays.asList("2020-04-01T04:01:02Z", "2020-04-25T07:04:05Z")));
        assertRecordSchemaAndValues(expectedUpdate, update, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();

        // DELETE
        executeAndWait("DELETE FROM time_array_table;");
        SourceRecord deleteRec = consumer.remove();
        VerifyRecord.isValidDelete(deleteRec, PK_FIELD, 1);
        assertSourceInfo(deleteRec, "postgres", "public", "time_array_table");
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    @FixFor({ "DBZ-1680", "DBZ-5038" })
    public void shouldStreamEnumsWhenIncludeUnknownDataTypesDisabled() throws Exception {
        // Specifically enable `column.propagate.source.type` here to validate later that the actual
        // type, length, and scale values are resolved correctly when paired with Enum types.
        TestHelper.execute("CREATE TYPE test_type AS ENUM ('V1','V2');");
        TestHelper.execute("CREATE TABLE enum_table (pk SERIAL, data varchar(25) NOT NULL, value test_type NOT NULL DEFAULT 'V1', PRIMARY KEY (pk));");
        startConnector(config -> config
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, false)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with("column.propagate.source.type", "public.enum_table.value")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.enum_table"), false);

        waitForStreamingToStart();

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO enum_table (data) VALUES ('hello');");

        SourceRecord rec = assertRecordInserted("public.enum_table", PK_FIELD, 1);
        assertSourceInfo(rec, "postgres", "public", "enum_table");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField(PK_FIELD, SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("data", Schema.STRING_SCHEMA, "hello"),
                new SchemaAndValueField("value", Enum.builder("V1,V2")
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "TEST_TYPE")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, String.valueOf(Integer.MAX_VALUE))
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                        .defaultValue("V1")
                        .build(), "V1"));

        assertRecordSchemaAndValues(expected, rec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();
    }

    private void testReceiveChangesForReplicaIdentityFullTableWithToastedValue(PostgresConnectorConfig.SchemaRefreshMode mode, boolean tablesBeforeStart)
            throws Exception {
        if (tablesBeforeStart) {
            TestHelper.execute(
                    "DROP TABLE IF EXISTS test_table;",
                    "CREATE TABLE test_table (id SERIAL, not_toast int, text TEXT);",
                    "ALTER TABLE test_table REPLICA IDENTITY FULL");

            awaitTableMetaDataIsQueryable(new TableId(null, "public", "test_table"));
        }

        startConnector(config -> config.with(PostgresConnectorConfig.SCHEMA_REFRESH_MODE, mode), false);
        assertConnectorIsRunning();
        consumer = testConsumer(1);

        final String toastedValue = RandomStringUtils.randomAlphanumeric(10000);

        if (!tablesBeforeStart) {
            waitForStreamingToStart();
            TestHelper.execute(
                    "DROP TABLE IF EXISTS test_table;",
                    "CREATE TABLE test_table (id SERIAL, not_toast int, text TEXT);",
                    "ALTER TABLE test_table REPLICA IDENTITY FULL");

            awaitTableMetaDataIsQueryable(new TableId(null, "public", "test_table"));

        }

        // INSERT
        String statement = "INSERT INTO test_table (not_toast, text) VALUES (10,'" + toastedValue + "');";
        assertInsert(
                statement,
                Arrays.asList(
                        new SchemaAndValueField("id", SchemaBuilder.int32().defaultValue(0).build(), 1), // SERIAL is NOT NULL implicitly
                        new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                        new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, toastedValue)));

        // UPDATE
        consumer.expects(1);
        executeAndWait("UPDATE test_table set not_toast = 20");
        SourceRecord updatedRecord = consumer.remove();

        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("id", SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, toastedValue)), updatedRecord, Envelope.FieldName.BEFORE);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("id", SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 20),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, toastedValue)), updatedRecord, Envelope.FieldName.AFTER);

        // DELETE
        consumer.expects(2);
        executeAndWait("DELETE FROM test_table");
        SourceRecord deletedRecord = consumer.remove();
        SourceRecord tombstoneRecord = consumer.remove();
        assertThat(tombstoneRecord.value()).isNull();
        assertThat(tombstoneRecord.valueSchema()).isNull();
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("id", SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 20),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, toastedValue)), deletedRecord, Envelope.FieldName.BEFORE);

        // INSERT null
        consumer.expects(1);
        statement = "INSERT INTO test_table (not_toast, text) VALUES (100, null);";
        assertInsert(
                statement,
                Arrays.asList(
                        new SchemaAndValueField("id", SchemaBuilder.int32().defaultValue(0).build(), 2), // SERIAL is NOT NULL implicitly
                        new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 100),
                        new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, null)));

        // UPDATE null
        consumer.expects(1);
        executeAndWait("UPDATE test_table set not_toast = 200 WHERE id=2");
        updatedRecord = consumer.remove();
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("id", SchemaBuilder.int32().defaultValue(0).build(), 2),
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 100),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, null)), updatedRecord, Envelope.FieldName.BEFORE);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("id", SchemaBuilder.int32().defaultValue(0).build(), 2),
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 200),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, null)), updatedRecord, Envelope.FieldName.AFTER);

        // DELETE null
        consumer.expects(2);
        executeAndWait("DELETE FROM test_table WHERE id=2");
        deletedRecord = consumer.remove();
        tombstoneRecord = consumer.remove();
        assertThat(tombstoneRecord.value()).isNull();
        assertThat(tombstoneRecord.valueSchema()).isNull();
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("id", SchemaBuilder.int32().defaultValue(0).build(), 2),
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 200),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, null)), deletedRecord, Envelope.FieldName.BEFORE);
    }

    /**
     * It appears in some cases retrieving column metadata "too quickly" raises
     * a PSQLException: ERROR: could not open relation with OID xyz.
     * This causes intermittent failures during schema refresh.
     * This is an attempt to avoid that situation by making sure the metadata can be retrieved
     * before proceeding.
     */
    private void awaitTableMetaDataIsQueryable(TableId tableId) {
        Awaitility.await()
                .atMost(TestHelper.waitTimeForRecords() * 10, TimeUnit.SECONDS)
                .ignoreException(PSQLException.class)
                .until(() -> {
                    try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
                        Tables tables = new Tables();
                        connection.readSchema(tables, null, "public", TableFilter.fromPredicate(t -> t.equals(tableId)), null, false);
                        return tables.forTable(tableId) != null;
                    }
                });
    }

    @Test()
    @FixFor("DBZ-1815")
    public void testHeartbeatActionQueryExecuted() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_table;" +
                        "CREATE TABLE test_table (id SERIAL, text TEXT);" +
                        "INSERT INTO test_table (text) VALUES ('mydata');");

        TestHelper.execute(
                "DROP TABLE IF EXISTS test_heartbeat_table;" +
                        "CREATE TABLE test_heartbeat_table (text TEXT);");

        // A low heartbeat interval should make sure that a heartbeat message is emitted at least once during the test.
        startConnector(config -> config
                .with(Heartbeat.HEARTBEAT_INTERVAL, "100")
                .with(DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY,
                        "INSERT INTO test_heartbeat_table (text) VALUES ('test_heartbeat');"));

        // Expecting 1 data change
        Awaitility.await().atMost(TestHelper.waitTimeForRecords() * 10, TimeUnit.SECONDS).until(() -> {
            final SourceRecord record = consumeRecord();
            return record != null && Envelope.isEnvelopeSchema(record.valueSchema());
        });

        // Confirm that the heartbeat.action.query was executed with the heartbeat. It is difficult to determine the
        // exact amount of times the heartbeat will fire because the run time of the test will vary, but if there is
        // anything in test_heartbeat_table then this test is confirmed.
        int numOfHeartbeatActions;
        final String slotQuery = "SELECT COUNT(*) FROM test_heartbeat_table;";
        final JdbcConnection.ResultSetMapper<Integer> slotQueryMapper = rs -> {
            rs.next();
            return rs.getInt(1);
        };
        try (PostgresConnection connection = TestHelper.create()) {
            numOfHeartbeatActions = connection.queryAndMap(slotQuery, slotQueryMapper);
        }
        assertTrue(numOfHeartbeatActions > 0);
    }

    @Test
    @FixFor({ "DBZ-1916", "DBZ-1830" })
    public void shouldPropagateSourceTypeByDatatype() throws Exception {
        TestHelper.execute("DROP TABLE IF EXISTS test_table;");
        TestHelper.execute("CREATE TABLE test_table (id SERIAL, c1 INT, c2 INT, c3a NUMERIC(5,2), c3b VARCHAR(128), f1 float(10), f2 decimal(8,4), primary key (id));");

        startConnector(config -> config
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with("datatype.propagate.source.type", ".+\\.NUMERIC,.+\\.VARCHAR,.+\\.FLOAT4"), false);

        waitForStreamingToStart();

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO test_table (id,c1,c2,c3a,c3b,f1,f2) values (1, 123, 456, 789.01, 'test', 1.228, 234.56);");

        final SourceRecord record = assertRecordInserted("public.test_table", "id", 1);
        final Field before = record.valueSchema().field("before");

        // no type info requested as per given data types
        assertThat(before.schema().field("id").schema().parameters()).isNull();
        assertThat(before.schema().field("c1").schema().parameters()).isNull();
        assertThat(before.schema().field("c2").schema().parameters()).isNull();

        assertThat(before.schema().field("c3a").schema().parameters()).contains(
                entry(TYPE_NAME_PARAMETER_KEY, "NUMERIC"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "5"),
                entry(TYPE_SCALE_PARAMETER_KEY, "2"));

        // variable width, name and length info
        assertThat(before.schema().field("c3b").schema().parameters()).contains(
                entry(TYPE_NAME_PARAMETER_KEY, "VARCHAR"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "128"));

        assertThat(before.schema().field("f2").schema().parameters()).contains(
                entry(TYPE_NAME_PARAMETER_KEY, "NUMERIC"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "8"),
                entry(TYPE_SCALE_PARAMETER_KEY, "4"));

        assertThat(before.schema().field("f1").schema().parameters()).contains(
                entry(TYPE_NAME_PARAMETER_KEY, "FLOAT4"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "8"),
                entry(TYPE_SCALE_PARAMETER_KEY, "8"));
    }

    @Test
    @FixFor({ "DBZ-3074" })
    public void shouldMaintainPrimaryKeyOrderOnSchemaChange() throws Exception {
        startConnector();
        consumer = testConsumer(1);
        executeAndWait("CREATE TABLE test_should_maintain_primary_key_order(b INTEGER, d INTEGER, c INTEGER, a INTEGER, val INTEGER, PRIMARY KEY (b, d, c, a));" +
                "INSERT INTO test_should_maintain_primary_key_order VALUES (1, 2, 3, 4, 5);");

        SourceRecord record = consumer.remove();
        assertEquals(1, ((Struct) record.value()).getStruct("after").getInt32("b").intValue());

        List<Field> fields = record.keySchema().fields();
        String[] expectedFieldOrder = new String[]{ "b", "d", "c", "a" };

        for (int i = 0; i < fields.size(); i++) {
            assertEquals("Key field names should in order", expectedFieldOrder[i], fields.get(i).name());
        }

        // Alter the table to trigger a schema change event. Validate that the new schema maintains the primary key order.
        consumer.expects(1);
        executeAndWait("ALTER TABLE test_should_maintain_primary_key_order ADD COLUMN val2 INTEGER;" +
                "INSERT INTO test_should_maintain_primary_key_order VALUES (10, 11, 12, 13, 14, 15);");

        record = consumer.remove();
        assertEquals(10, ((Struct) record.value()).getStruct("after").getInt32("b").intValue());

        fields = record.keySchema().fields();
        for (int i = 0; i < fields.size(); i++) {
            assertEquals("Key field names should in order", expectedFieldOrder[i], fields.get(i).name());
        }
    }

    @Test
    @FixFor("DBZ-1931")
    public void testStreamMoneyAsDefaultPrecise() throws Exception {
        TestHelper.execute("CREATE TABLE salary (pk SERIAL, name VARCHAR(50), salary money, PRIMARY KEY(pk));");

        startConnector(config -> config
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.salary"),
                false);

        waitForStreamingToStart();

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO salary (name, salary) values ('Joe', 123.45);");

        SourceRecord rec = assertRecordInserted("public.salary", PK_FIELD, 1);
        assertSourceInfo(rec, "postgres", "public", "salary");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField("pk", SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("name", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "Joe"),
                new SchemaAndValueField("salary", Decimal.builder(2).optional().build(), BigDecimal.valueOf(123.45)));

        assertRecordSchemaAndValues(expected, rec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    @FixFor("DBZ-1931")
    public void testStreamMoneyAsString() throws Exception {
        TestHelper.execute("CREATE TABLE salary (pk SERIAL, name VARCHAR(50), salary money, PRIMARY KEY(pk));");

        startConnector(config -> config
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.STRING)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.salary"),
                false);

        waitForStreamingToStart();

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO salary (name, salary) values ('Joe', 123.45);");

        SourceRecord rec = assertRecordInserted("public.salary", PK_FIELD, 1);
        assertSourceInfo(rec, "postgres", "public", "salary");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField("pk", SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("name", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "Joe"),
                new SchemaAndValueField("salary", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "123.45"));

        assertRecordSchemaAndValues(expected, rec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    @FixFor("DBZ-1931")
    public void testStreamMoneyAsDouble() throws Exception {
        TestHelper.execute("CREATE TABLE salary (pk SERIAL, name VARCHAR(50), salary money, PRIMARY KEY(pk));");

        startConnector(config -> config
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.salary"),
                false);

        waitForStreamingToStart();

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO salary (name, salary) values ('Joe', 123.45);");

        SourceRecord rec = assertRecordInserted("public.salary", PK_FIELD, 1);
        assertSourceInfo(rec, "postgres", "public", "salary");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField("pk", SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("name", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "Joe"),
                new SchemaAndValueField("salary", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA, 123.45));

        assertRecordSchemaAndValues(expected, rec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    @FixFor("DBZ-1931")
    public void testStreamMoneyPreciseDecimalFraction() throws Exception {
        TestHelper.execute("CREATE TABLE salary (pk SERIAL, name VARCHAR(50), salary money, PRIMARY KEY(pk));");

        startConnector(config -> config
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.PRECISE)
                .with(PostgresConnectorConfig.MONEY_FRACTION_DIGITS, 1)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.salary"),
                false);

        waitForStreamingToStart();

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO salary (name, salary) values ('Joe', 123.4567);");

        SourceRecord rec = assertRecordInserted("public.salary", PK_FIELD, 1);
        assertSourceInfo(rec, "postgres", "public", "salary");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField("pk", SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("name", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "Joe"),
                new SchemaAndValueField("salary", Decimal.builder(1).optional().build(), BigDecimal.valueOf(123.5)));

        assertRecordSchemaAndValues(expected, rec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();
    }

    private void assertHeartBeatRecord(SourceRecord heartbeat) {
        assertEquals("__debezium-heartbeat." + TestHelper.TEST_SERVER, heartbeat.topic());

        Struct key = (Struct) heartbeat.key();
        assertThat(key.get("serverName")).isEqualTo(TestHelper.TEST_SERVER);

        Struct value = (Struct) heartbeat.value();
        assertThat(value.getInt64("ts_ms")).isLessThanOrEqualTo(Instant.now().toEpochMilli());
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
            assertRecordOffsetAndSnapshotSource(record, SnapshotRecord.FALSE);
            assertSourceInfo(record, "postgres", table.schema(), table.table());
            assertRecordSchemaAndValues(expectedSchemaAndValuesByColumn, record, Envelope.FieldName.AFTER);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void assertDelete(String statement, Integer pk,
                              List<SchemaAndValueField> expectedSchemaAndValuesByColumn) {
        TableId table = tableIdFromDeleteStmt(statement);
        String expectedTopicName = table.schema() + "." + table.table();
        expectedTopicName = expectedTopicName.replaceAll("[ \"]", "_");

        try {
            executeAndWait(statement);
            SourceRecord record = assertRecordDeleted(expectedTopicName, pk != null ? PK_FIELD : null, pk);
            assertRecordOffsetAndSnapshotSource(record, SnapshotRecord.FALSE);
            assertSourceInfo(record, "postgres", table.schema(), table.table());
            assertRecordSchemaAndValues(expectedSchemaAndValuesByColumn, record, Envelope.FieldName.BEFORE);
            assertRecordSchemaAndValues(null, record, Envelope.FieldName.AFTER);
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

    private SourceRecord assertRecordDeleted(String expectedTopicName, String pkColumn, Integer pk) throws InterruptedException {
        assertFalse("records not generated", consumer.isEmpty());
        SourceRecord deletedRecord = consumer.remove();

        return assertRecordDeleted(deletedRecord, expectedTopicName, pkColumn, pk);
    }

    private SourceRecord assertRecordDeleted(SourceRecord deletedRecord, String expectedTopicName, String pkColumn, Integer pk) throws InterruptedException {
        assertEquals(topicName(expectedTopicName), deletedRecord.topic());

        if (pk != null) {
            VerifyRecord.isValidDelete(deletedRecord, pkColumn, pk);
        }
        else {
            VerifyRecord.isValidDelete(deletedRecord);
        }

        return deletedRecord;
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

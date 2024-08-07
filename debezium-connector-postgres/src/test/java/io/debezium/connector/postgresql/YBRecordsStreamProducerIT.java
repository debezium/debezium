/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import com.yugabyte.util.PSQLException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.postgresql.PostgresConnectorConfig.IntervalHandlingMode;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SchemaRefreshMode;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
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
import io.debezium.embedded.EmbeddedEngineConfig;
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
import io.debezium.util.HexConverter;
import io.debezium.util.Stopwatch;
import io.debezium.util.Testing;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.debezium.connector.postgresql.TestHelper.*;
import static io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIs.DecoderPluginName.PGOUTPUT;
import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Integration test for the {@link RecordsStreamProducer} class. This also tests indirectly the PG plugin functionality for
 * different use cases. This class is a copy of {@link RecordsStreamProducerIT} with source database
 * being YugabyteDB. This rewrite of the test class is needed since we use the plugin `yboutput` which essentially
 * causes a change in the structure of the record so we had to change the way records were asserted.
 *
 * @author Vaibhav Kushwaha (hchiorea@redhat.com)
 */
public class YBRecordsStreamProducerIT extends AbstractRecordsProducerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(YBRecordsStreamProducerIT.class);

    private TestConsumer consumer;

    @Rule
    public final TestRule skip = new SkipTestDependingOnDecoderPluginNameRule();

    @Rule
    public TestRule conditionalFail = new ConditionalFail();

    @Before
    public void before() throws Exception {
        // ensure the slot is deleted for each test
        TestHelper.dropAllSchemas();
        TestHelper.dropPublication();
//        TestHelper.executeDDL("init_postgis.ddl");
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

        Print.enable();
    }

    private void startConnector(Function<Configuration.Builder, Configuration.Builder> customConfig, boolean waitForSnapshot, Predicate<SourceRecord> isStopRecord)
            throws InterruptedException {
        start(YugabyteDBConnector.class, new PostgresConnectorConfig(customConfig.apply(TestHelper.defaultConfig()
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
        YBVerifyRecord.isValidUpdate(record, "pk", 1);
        YBVerifyRecord.isValid(record);
        return ((Struct) record.value()).getStruct("before");
    }

    @Ignore("YB Note: Replica identity cannot be changed at runtime")
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

    @Ignore("YB Note: Replica identity cannot be changed at runtime")
    @Test
    @FixFor("DBZ-1029")
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
        // YB Note: Table cannot be altered if it's a part of CDC replication - https://github.com/yugabyte/yugabyte-db/issues/16625
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
    public void shouldReceiveChangesForUpdatesWithColumnChanges() throws Exception {
        // add a new column
        String statements = "ALTER TABLE test_table ADD COLUMN uvc VARCHAR(2);" +
                "ALTER TABLE test_table REPLICA IDENTITY FULL;";

        execute(statements);

        startConnector();

        // Wait after starting connector.
        consumer = testConsumer(1);

        // Execute the update after starting the connector.
        executeAndWait("UPDATE test_table SET uvc ='aa' WHERE pk = 1;");

        // the update should be the last record
        SourceRecord updatedRecord = consumer.remove();
        String topicName = topicName("public.test_table");
        assertEquals(topicName, updatedRecord.topic());
        YBVerifyRecord.isValidUpdate(updatedRecord, PK_FIELD, 1);

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
        YBVerifyRecord.isValidUpdate(updatedRecord, PK_FIELD, 1);

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
        YBVerifyRecord.isValidUpdate(updatedRecord, PK_FIELD, 1);

        // change a column type
        statements = "ALTER TABLE test_table ADD COLUMN modtype INTEGER;" +
                "INSERT INTO test_table (pk,modtype) VALUES (2,1);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        YBVerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 2);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("modtype", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 1)), updatedRecord, Envelope.FieldName.AFTER);
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
        YBVerifyRecord.isValidDelete(deleteRecord, PK_FIELD, 1);

        // followed by a tombstone of the old pk
        SourceRecord tombstoneRecord = consumer.remove();
        assertEquals(topicName, tombstoneRecord.topic());
        YBVerifyRecord.isValidTombstone(tombstoneRecord, PK_FIELD, 1);

        // and finally insert of the new value
        SourceRecord insertRecord = consumer.remove();
        assertEquals(topicName, insertRecord.topic());
        YBVerifyRecord.isValidInsert(insertRecord, PK_FIELD, 2);
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
        YBVerifyRecord.isValidDelete(deleteRecord, PK_FIELD, 1);

        // followed by insert of the new value
        SourceRecord insertRecord = consumer.remove();
        assertEquals(topicName, insertRecord.topic());
        YBVerifyRecord.isValidInsert(insertRecord, PK_FIELD, 2);
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
        YBVerifyRecord.isValidInsert(insertRecord, PK_FIELD, 2);
        List<SchemaAndValueField> expectedSchemaAndValues = Arrays.asList(
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "update"),
                new SchemaAndValueField("default_column", SchemaBuilder.string().optional().defaultValue("default").build(), "default"));
        assertRecordSchemaAndValues(expectedSchemaAndValues, insertRecord, Envelope.FieldName.AFTER);
    }

    @Test
    public void shouldReceiveChangesForTypeConstraints() throws Exception {
        // add a new column
        String statements = "ALTER TABLE test_table ADD COLUMN num_val NUMERIC(5,2);" +
                "ALTER TABLE test_table REPLICA IDENTITY FULL;";

        // Alter the replica identity before starting connector.
        execute(statements);

        startConnector();
        consumer = testConsumer(1);
        executeAndWait("UPDATE test_table SET num_val = 123.45 WHERE pk = 1;");

        // the update should be the last record
        SourceRecord updatedRecord = consumer.remove();
        String topicName = topicName("public.test_table");
        assertEquals(topicName, updatedRecord.topic());
        YBVerifyRecord.isValidUpdate(updatedRecord, PK_FIELD, 1);

        // now check we got the updated value (the old value should be null, the new one whatever we set)
        List<SchemaAndValueField> expectedBefore = Collections.singletonList(new SchemaAndValueField("num_val", null, null));
        assertRecordSchemaAndValues(expectedBefore, updatedRecord, Envelope.FieldName.BEFORE);

        List<SchemaAndValueField> expectedAfter = Collections.singletonList(
                new SchemaAndValueField("num_val", Decimal.builder(2).parameter(TestHelper.PRECISION_PARAMETER_KEY, "5").optional().build(), new BigDecimal("123.45")));
        assertRecordSchemaAndValues(expectedAfter, updatedRecord, Envelope.FieldName.AFTER);

        if (YugabyteDBServer.isEnabled()) {
            // YB Note: Altering column for table part of CDC replication is not allowed, see https://github.com/yugabyte/yugabyte-db/issues/16625
            return;
        }

        // change a constraint
        statements = "ALTER TABLE test_table ALTER COLUMN num_val TYPE NUMERIC(6,1);" +
                "INSERT INTO test_table (pk,num_val) VALUES (2,123.41);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        YBVerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 2);
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
        YBVerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 3);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("num_val", VariableScaleDecimal.builder().optional().build(), dvs)), updatedRecord,
                Envelope.FieldName.AFTER);

        statements = "ALTER TABLE test_table ALTER COLUMN num_val TYPE DECIMAL(12,4);" +
                "INSERT INTO test_table (pk,num_val) VALUES (4,2.48);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        YBVerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 4);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("num_val", Decimal.builder(4).parameter(TestHelper.PRECISION_PARAMETER_KEY, "12").optional().build(),
                        new BigDecimal("2.4800"))),
                updatedRecord, Envelope.FieldName.AFTER);

        statements = "ALTER TABLE test_table ALTER COLUMN num_val TYPE DECIMAL(12);" +
                "INSERT INTO test_table (pk,num_val) VALUES (5,1238);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        YBVerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 5);
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
        YBVerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 6);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("num_val", VariableScaleDecimal.builder().optional().build(), dvs2)), updatedRecord,
                Envelope.FieldName.AFTER);

        statements = "ALTER TABLE test_table ALTER COLUMN num_val SET NOT NULL;" +
                "INSERT INTO test_table (pk,num_val) VALUES (7,1976);";

        consumer.expects(1);
        executeAndWait(statements);
        updatedRecord = consumer.remove();

        dvs2.put("scale", 0).put("value", new BigDecimal("1976").unscaledValue().toByteArray());
        YBVerifyRecord.isValidInsert(updatedRecord, PK_FIELD, 7);
        assertRecordSchemaAndValues(
                Collections.singletonList(new SchemaAndValueField("num_val", VariableScaleDecimal.builder().build(), dvs2)), updatedRecord, Envelope.FieldName.AFTER);
    }

    @Test
    public void verifyAllWorkingTypesInATableWithYbOutput() throws Exception {
        verifyAllWorkingTypesInATable(PostgresConnectorConfig.LogicalDecoder.YBOUTPUT);
    }

    @Test
    public void verifyAllWorkingTypesInATableWithPgOutput() throws Exception {
        verifyAllWorkingTypesInATable(PostgresConnectorConfig.LogicalDecoder.PGOUTPUT);
    }

    public void verifyAllWorkingTypesInATable(PostgresConnectorConfig.LogicalDecoder logicalDecoder) throws Exception {
        String createStmt = "CREATE TABLE all_types (id serial PRIMARY KEY, bigintcol bigint, " +
                              "bitcol bit(5), varbitcol varbit(5), booleanval boolean, " +
                              "byteaval bytea, ch char(5), vchar varchar(25), cidrval cidr, " +
                              "dt date, dp double precision, inetval inet, intervalval interval, " +
                              "jsonval json, jsonbval jsonb, mc macaddr, mc8 macaddr8, mn money, " +
                              "rl real, si smallint, i4r int4range, i8r int8range, " +
                              "nr numrange, tsr tsrange, tstzr tstzrange, dr daterange, txt text, " +
                              "tm time, tmtz timetz, ts timestamp, tstz timestamptz, uuidval uuid)";

        execute(createStmt);

        if (logicalDecoder == PostgresConnectorConfig.LogicalDecoder.PGOUTPUT) {
            LOGGER.info("Changing replica identity of the table to default");
            TestHelper.execute("ALTER TABLE all_types REPLICA IDENTITY DEFAULT;");
            TestHelper.execute("ALTER TABLE test_table REPLICA IDENTITY DEFAULT;");
            TestHelper.execute("ALTER TABLE table_with_interval REPLICA IDENTITY DEFAULT;");
            TestHelper.waitFor(Duration.ofSeconds(10));
        }

        TestHelper.dropPublication();

        start(YugabyteDBConnector.class,
              TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.all_types")
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, "filtered")
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, "never")
                .with(PostgresConnectorConfig.PLUGIN_NAME, logicalDecoder.getPostgresPluginName())
                .build());
        assertConnectorIsRunning();
        waitForStreamingToStart();
        consumer = testConsumer(1);

        String insertStmt =
          "INSERT INTO all_types (bigintcol, bitcol, varbitcol, booleanval, byteaval, ch, vchar, cidrval, dt, " +
            "dp, inetval, intervalval, jsonval, jsonbval, mc, mc8, mn, rl, si, i4r, i8r, nr, tsr, tstzr, dr, " +
            "txt, tm, tmtz, ts, tstz, uuidval) VALUES (123456, '11011', '10101', FALSE, E'\\\\001', 'five5', " +
            "'sample_text', '10.1.0.0/16', '2022-02-24', 12.345, '127.0.0.1', " +
            "'2020-03-10 00:00:00'::timestamp-'2020-02-10 00:00:00'::timestamp, '{\"a\":\"b\"}', " +
            "'{\"a\":\"b\"}', '2C:54:91:88:C9:E3', '22:00:5c:03:55:08:01:02', '$100.5', " +
            "32.145, 12, '(1, 10)', '(100, 200)', '(10.45, 21.32)', " +
            "'(1970-01-01 00:00:00, 2000-01-01 12:00:00)', '(2017-07-04 12:30:30 UTC, 2021-07-04 12:30:30+05:30)', " +
            "'(2019-10-07, 2021-10-07)', 'text to verify behaviour', '12:47:32', '12:00:00+05:30', " +
            "'2021-11-25 12:00:00.123456', '2021-11-25 12:00:00+05:30', 'ffffffff-ffff-ffff-ffff-ffffffffffff');";

        consumer.expects(1);
        executeAndWait(insertStmt);

        SourceRecord record = consumer.remove();

        assertValueField(record, getResolvedColumnName("after/bigintcol", logicalDecoder), 123456);
        assertValueField(record, getResolvedColumnName("after/bitcol", logicalDecoder), new byte[]{27});
        assertValueField(record, getResolvedColumnName("after/varbitcol", logicalDecoder), new byte[]{21});
        assertValueField(record, getResolvedColumnName("after/booleanval", logicalDecoder), false);
        assertValueField(record, getResolvedColumnName("after/byteaval", logicalDecoder), ByteBuffer.wrap(HexConverter.convertFromHex("01")));
        assertValueField(record, getResolvedColumnName("after/ch", logicalDecoder), "five5");
        assertValueField(record, getResolvedColumnName("after/vchar", logicalDecoder), "sample_text");
        assertValueField(record, getResolvedColumnName("after/cidrval", logicalDecoder), "10.1.0.0/16");
        assertValueField(record, getResolvedColumnName("after/dt", logicalDecoder), 19047);
        assertValueField(record, getResolvedColumnName("after/dp", logicalDecoder), 12.345);
        assertValueField(record, getResolvedColumnName("after/inetval", logicalDecoder), "127.0.0.1");
        assertValueField(record, getResolvedColumnName("after/intervalval", logicalDecoder), 2505600000000L);
        assertValueField(record, getResolvedColumnName("after/jsonval", logicalDecoder), "{\"a\":\"b\"}");
        assertValueField(record, getResolvedColumnName("after/jsonbval", logicalDecoder), "{\"a\": \"b\"}");
        assertValueField(record, getResolvedColumnName("after/mc", logicalDecoder), "2c:54:91:88:c9:e3");
        assertValueField(record, getResolvedColumnName("after/mc8", logicalDecoder), "22:00:5c:03:55:08:01:02");
        assertValueField(record, getResolvedColumnName("after/mn", logicalDecoder), 100.50);
        assertValueField(record, getResolvedColumnName("after/rl", logicalDecoder), 32.145);
        assertValueField(record, getResolvedColumnName("after/si", logicalDecoder), 12);
        assertValueField(record, getResolvedColumnName("after/i4r", logicalDecoder), "[2,10)");
        assertValueField(record, getResolvedColumnName("after/i8r", logicalDecoder), "[101,200)");
        assertValueField(record, getResolvedColumnName("after/nr", logicalDecoder), "(10.45,21.32)");
        assertValueField(record, getResolvedColumnName("after/tsr", logicalDecoder), "(\"1970-01-01 00:00:00\",\"2000-01-01 12:00:00\")");
        assertValueField(record, getResolvedColumnName("after/tstzr", logicalDecoder), "(\"2017-07-04 18:00:30+05:30\",\"2021-07-04 12:30:30+05:30\")");
        assertValueField(record, getResolvedColumnName("after/dr", logicalDecoder), "[2019-10-08,2021-10-07)");
        assertValueField(record, getResolvedColumnName("after/txt", logicalDecoder), "text to verify behaviour");
        assertValueField(record, getResolvedColumnName("after/tm", logicalDecoder), 46052000000L);
        assertValueField(record, getResolvedColumnName("after/tmtz", logicalDecoder), "06:30:00Z");
        assertValueField(record, getResolvedColumnName("after/ts", logicalDecoder), 1637841600123456L);
        assertValueField(record, getResolvedColumnName("after/tstz", logicalDecoder), "2021-11-25T06:30:00.000000Z");
        assertValueField(record, getResolvedColumnName("after/uuidval", logicalDecoder), "ffffffff-ffff-ffff-ffff-ffffffffffff");
    }

    private String getResolvedColumnName(String columnName, PostgresConnectorConfig.LogicalDecoder logicalDecoder) {
        if (logicalDecoder == PostgresConnectorConfig.LogicalDecoder.PGOUTPUT) {
            return columnName;
        } else if (logicalDecoder == PostgresConnectorConfig.LogicalDecoder.YBOUTPUT) {
            return columnName + "/value";
        } else {
            throw new RuntimeException("Logical decoder name value incorrect, check configuration");
        }
    }

    @Test
    public void verifyUpdatesForColumnsOfAllTypesForYbOutput() throws Exception {
        verifyUpdatesForColumnsOfAllTypes(PostgresConnectorConfig.LogicalDecoder.YBOUTPUT);
    }

    @Test
    public void verifyUpdatesForColumnsOfAllTypesForPgOutput() throws Exception {
        verifyUpdatesForColumnsOfAllTypes(PostgresConnectorConfig.LogicalDecoder.PGOUTPUT);
    }

    public void verifyUpdatesForColumnsOfAllTypes(PostgresConnectorConfig.LogicalDecoder logicalDecoder) throws Exception {
        String createStmt = "CREATE TABLE all_types (id serial PRIMARY KEY, bigintcol bigint, " +
                "bitcol bit(5), varbitcol varbit(5), booleanval boolean, " +
                "byteaval bytea, ch char(5), vchar varchar(25), cidrval cidr, " +
                "dt date, dp double precision, inetval inet, intervalval interval, " +
                "jsonval json, jsonbval jsonb, mc macaddr, mc8 macaddr8, mn money, " +
                "rl real, si smallint, i4r int4range, i8r int8range, " +
                "nr numrange, tsr tsrange, tstzr tstzrange, dr daterange, txt text, " +
                "tm time, tmtz timetz, ts timestamp, tstz timestamptz, uuidval uuid)";

        execute(createStmt);

        if (logicalDecoder == PostgresConnectorConfig.LogicalDecoder.PGOUTPUT) {
            LOGGER.info("Changing replica identity of all the tables to default");
            TestHelper.execute("ALTER TABLE all_types REPLICA IDENTITY DEFAULT;");
            TestHelper.execute("ALTER TABLE test_table REPLICA IDENTITY DEFAULT;");
            TestHelper.execute("ALTER TABLE table_with_interval REPLICA IDENTITY DEFAULT;");
            TestHelper.waitFor(Duration.ofSeconds(10));
        }

        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();

        start(YugabyteDBConnector.class,
                TestHelper.defaultConfig()
                        .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.all_types")
                        .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, "filtered")
                        .with(PostgresConnectorConfig.SNAPSHOT_MODE, "never")
                        .with(PostgresConnectorConfig.PLUGIN_NAME, logicalDecoder.getPostgresPluginName())
                        .build());
        assertConnectorIsRunning();
        waitForStreamingToStart();
        consumer = testConsumer(1);

        String insertStmt =
                "INSERT INTO all_types (bigintcol, bitcol, varbitcol, booleanval, byteaval, ch, vchar, cidrval, dt, " +
                        "dp, inetval, intervalval, jsonval, jsonbval, mc, mc8, mn, rl, si, i4r, i8r, nr, tsr, tstzr, dr, " +
                        "txt, tm, tmtz, ts, tstz, uuidval) VALUES (123456, '11011', '10101', FALSE, E'\\\\001', 'five5', " +
                        "'sample_text', '10.1.0.0/16', '2022-02-24', 12.345, '127.0.0.1', " +
                        "'2020-03-10 00:00:00'::timestamp-'2020-02-10 00:00:00'::timestamp, '{\"a\":\"b\"}', " +
                        "'{\"a\":\"b\"}', '2C:54:91:88:C9:E3', '22:00:5c:03:55:08:01:02', '$100.5', " +
                        "32.145, 12, '(1, 10)', '(100, 200)', '(10.45, 21.32)', " +
                        "'(1970-01-01 00:00:00, 2000-01-01 12:00:00)', '(2017-07-04 12:30:30 UTC, 2021-07-04 12:30:30+05:30)', " +
                        "'(2019-10-07, 2021-10-07)', 'text to verify behaviour', '12:47:32', '12:00:00+05:30', " +
                        "'2021-11-25 12:00:00.123456', '2021-11-25 12:00:00+05:30', 'ffffffff-ffff-ffff-ffff-ffffffffffff');";

        consumer.expects(1);
        executeAndWait(insertStmt);

        SourceRecord record = consumer.remove();

        assertValueField(record, getResolvedColumnName("after/bigintcol", logicalDecoder), 123456);
        assertValueField(record, getResolvedColumnName("after/bitcol", logicalDecoder), new byte[]{27});
        assertValueField(record, getResolvedColumnName("after/varbitcol", logicalDecoder), new byte[]{21});
        assertValueField(record, getResolvedColumnName("after/booleanval", logicalDecoder), false);
        assertValueField(record, getResolvedColumnName("after/byteaval", logicalDecoder), ByteBuffer.wrap(HexConverter.convertFromHex("01")));
        assertValueField(record, getResolvedColumnName("after/ch", logicalDecoder), "five5");
        assertValueField(record, getResolvedColumnName("after/vchar", logicalDecoder), "sample_text");
        assertValueField(record, getResolvedColumnName("after/cidrval", logicalDecoder), "10.1.0.0/16");
        assertValueField(record, getResolvedColumnName("after/dt", logicalDecoder), 19047);
        assertValueField(record, getResolvedColumnName("after/dp", logicalDecoder), 12.345);
        assertValueField(record, getResolvedColumnName("after/inetval", logicalDecoder), "127.0.0.1");
        assertValueField(record, getResolvedColumnName("after/intervalval", logicalDecoder), 2505600000000L);
        assertValueField(record, getResolvedColumnName("after/jsonval", logicalDecoder), "{\"a\":\"b\"}");
        assertValueField(record, getResolvedColumnName("after/jsonbval", logicalDecoder), "{\"a\": \"b\"}");
        assertValueField(record, getResolvedColumnName("after/mc", logicalDecoder), "2c:54:91:88:c9:e3");
        assertValueField(record, getResolvedColumnName("after/mc8", logicalDecoder), "22:00:5c:03:55:08:01:02");
        assertValueField(record, getResolvedColumnName("after/mn", logicalDecoder), 100.50);
        assertValueField(record, getResolvedColumnName("after/rl", logicalDecoder), 32.145);
        assertValueField(record, getResolvedColumnName("after/si", logicalDecoder), 12);
        assertValueField(record, getResolvedColumnName("after/i4r", logicalDecoder), "[2,10)");
        assertValueField(record, getResolvedColumnName("after/i8r", logicalDecoder), "[101,200)");
        assertValueField(record, getResolvedColumnName("after/nr", logicalDecoder), "(10.45,21.32)");
        assertValueField(record, getResolvedColumnName("after/tsr", logicalDecoder), "(\"1970-01-01 00:00:00\",\"2000-01-01 12:00:00\")");
        assertValueField(record, getResolvedColumnName("after/tstzr", logicalDecoder), "(\"2017-07-04 18:00:30+05:30\",\"2021-07-04 12:30:30+05:30\")");
        assertValueField(record, getResolvedColumnName("after/dr", logicalDecoder), "[2019-10-08,2021-10-07)");
        assertValueField(record, getResolvedColumnName("after/txt", logicalDecoder), "text to verify behaviour");
        assertValueField(record, getResolvedColumnName("after/tm", logicalDecoder), 46052000000L);
        assertValueField(record, getResolvedColumnName("after/tmtz", logicalDecoder), "06:30:00Z");
        assertValueField(record, getResolvedColumnName("after/ts", logicalDecoder), 1637841600123456L);
        assertValueField(record, getResolvedColumnName("after/tstz", logicalDecoder), "2021-11-25T06:30:00.000000Z");
        assertValueField(record, getResolvedColumnName("after/uuidval", logicalDecoder), "ffffffff-ffff-ffff-ffff-ffffffffffff");

        // Update each column one by one.
        TestHelper.execute("UPDATE all_types SET bigintcol = 234567 WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET bitcol = '11111' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET varbitcol = '00011' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET booleanval = TRUE WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET byteaval = null WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET ch = 'four4' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET vchar = 'sample_text_updated' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET cidrval = '192.0.2.0/24' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET dt = '2024-08-06' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET dp = 23.456 WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET inetval = '192.168.1.1' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET intervalval = '2020-03-11 00:00:00'::timestamp-'2020-02-10 00:00:00'::timestamp WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET jsonval = '{\"c\":\"d\",\"e\":123}' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET jsonbval = '{\"c\":\"d\",\"e\":123}' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET mc = '2c:54:91:99:c9:e3' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET mc8 = '22:00:5c:3d:55:08:01:02' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET mn = '$200.5' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET rl = 44.556 WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET si = 11 WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET i4r = '(10, 100)' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET i8r = '(200, 10000)' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET nr = '(12.35, 56.78)' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET tsr = '(2000-01-01 00:00:00, 2000-01-02 00:00:00)' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET tstzr = '(2000-01-01 00:05:30+05:30, 2000-01-02 00:00:00 UTC)' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET dr = '(2000-01-01, 2000-01-03)' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET txt = 'updated text to verify behaviour' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET tm = '14:15:16' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET tmtz = '05:30:00+05:30' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET ts = '2024-08-06 12:00:00.123456' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET tstz = '2024-08-06 12:00:00+05:30' WHERE id = 1;");
        TestHelper.execute("UPDATE all_types SET uuidval = 'ffffffff-ffff-ffff-ffff-123456789abc' WHERE id = 1;");

        // This excludes the primary key column.
        List<String> columnsInAllTypes = List.of("bigintcol", "bitcol", "varbitcol", "booleanval", "byteaval", "ch", "vchar", "cidrval", "dt", "dp", "inetval",
                "intervalval", "jsonval", "jsonbval", "mc", "mc8", "mn", "rl", "si", "i4r", "i8r", "nr", "tsr", "tstzr", "dr", "txt", "tm", "tmtz", "ts", "tstz",
                "uuidval");

        SourceRecords allRecords = consumeRecordsByTopic(31 /* total records for updated */);
        List<SourceRecord> records = allRecords.allRecordsInOrder();

        assertThat(records.size()).isEqualTo(31);

        assertColumnInUpdate(columnsInAllTypes, records.get(0), "after/bigintcol", 234567, logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(1), "after/bitcol", new byte[]{31}, logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(2), "after/varbitcol", new byte[]{3}, logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(3), "after/booleanval", true, logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(4), "after/byteaval", null, logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(5), "after/ch", "four4", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(6), "after/vchar", "sample_text_updated", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(7), "after/cidrval", "192.0.2.0/24", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(8), "after/dt", 19941, logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(9), "after/dp", 23.456, logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(10), "after/inetval", "192.168.1.1", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(11), "after/intervalval", 2592000000000L, logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(12), "after/jsonval", "{\"c\":\"d\",\"e\":123}", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(13), "after/jsonbval", "{\"c\": \"d\", \"e\": 123}", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(14), "after/mc", "2c:54:91:99:c9:e3", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(15), "after/mc8", "22:00:5c:3d:55:08:01:02", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(16), "after/mn", 200.50, logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(17), "after/rl", 44.556, logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(18), "after/si", 11, logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(19), "after/i4r", "[11,100)", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(20), "after/i8r", "[201,10000)", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(21), "after/nr", "(12.35,56.78)", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(22), "after/tsr", "(\"2000-01-01 00:00:00\",\"2000-01-02 00:00:00\")", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(23), "after/tstzr", "(\"2000-01-01 00:05:30+05:30\",\"2000-01-02 05:30:00+05:30\")", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(24), "after/dr", "[2000-01-02,2000-01-03)", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(25), "after/txt", "updated text to verify behaviour", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(26), "after/tm", 51316000000L, logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(27), "after/tmtz", "00:00:00Z", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(28), "after/ts", 1722945600123456L, logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(29), "after/tstz", "2024-08-06T06:30:00.000000Z", logicalDecoder);
        assertColumnInUpdate(columnsInAllTypes, records.get(30), "after/uuidval", "ffffffff-ffff-ffff-ffff-123456789abc", logicalDecoder);
    }

    @Test
    public void verifyOperationsForTableWithMixedColumnsYbOutput() throws Exception {
        verifyOperationsInTableWithMixedColumns(PostgresConnectorConfig.LogicalDecoder.YBOUTPUT);
    }

    @Test
    public void verifyOperationsForTableWithMixedColumnsPgOutput() throws Exception {
        verifyOperationsInTableWithMixedColumns(PostgresConnectorConfig.LogicalDecoder.PGOUTPUT);
    }

    public void verifyOperationsInTableWithMixedColumns(PostgresConnectorConfig.LogicalDecoder logicalDecoder) throws Exception {
        String createStmt = "create table public.test_mixed (" +
                "  id character varying not null," +
                "  i_key character varying not null," +
                "  c_id character varying not null," +
                "  p_type character varying not null," +
                "  p_id character varying," +
                "  tx_id character varying," +
                "  status character varying not null," +
                "  amount numeric not null," +
                "  currency character varying not null," +
                "  loc character varying," +
                "  quantity numeric," +
                "  o_type character varying," +
                "  o_created_at timestamp without time zone," +
                "  o_updated_at timestamp without time zone," +
                "  dis_details jsonb," +
                "  o_metadata jsonb," +
                "  tx_data jsonb," +
                "  tx_ref_d jsonb," +
                "  rw_d jsonb," +
                "  meta jsonb," +
                "  created_at timestamp without time zone," +
                "  updated_at timestamp without time zone not null," +
                "  deleted_at timestamp without time zone," +
                "  version integer not null default 0," +
                "  primary key (updated_at, id, c_id)" +
                "); " +
                "create unique index orders_i_key_key on test_mixed using lsm (i_key); " +
                "create index idx_updated_at on test_mixed using lsm (updated_at);";

        execute(createStmt);

        if (logicalDecoder == PostgresConnectorConfig.LogicalDecoder.PGOUTPUT) {
            LOGGER.info("Changing replica identity of all the tables to default");
            TestHelper.execute("ALTER TABLE test_mixed REPLICA IDENTITY DEFAULT;");
            TestHelper.execute("ALTER TABLE test_table REPLICA IDENTITY DEFAULT;");
            TestHelper.execute("ALTER TABLE table_with_interval REPLICA IDENTITY DEFAULT;");
            TestHelper.waitFor(Duration.ofSeconds(10));
        }

        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();

        start(YugabyteDBConnector.class,
                TestHelper.defaultConfig()
                        .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.test_mixed")
                        .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, "filtered")
                        .with(PostgresConnectorConfig.SNAPSHOT_MODE, "never")
                        .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE)
                        .with(PostgresConnectorConfig.PLUGIN_NAME, logicalDecoder.getPostgresPluginName())
                        .build());
        assertConnectorIsRunning();
        waitForStreamingToStart();

        String insertStmt = "insert into test_mixed values ('id_val', 'i_key', 'c_id', 'p_type_val', " +
                "'p_id_val', 'ffffff-ffff', 'DONE', 30.5, 'INR', 'JJXX+HR8', 10.0, null, '2024-08-06 17:30:00', " +
                "'2024-08-06 17:30:00', '{\"name\":\"Something for display\"}', '{\"year\": 2000}', '{\"tx_seq\":500}', " +
                "null, null, null, '2024-08-06 17:30:00', '2024-08-06 17:30:00', null, 1)";

        execute(insertStmt);
        execute("UPDATE test_mixed SET status = 'NOT AVAILABLE', version = 2 WHERE id = 'id_val' AND c_id = 'c_id' AND updated_at = '2024-08-06 17:30:00';");

        SourceRecords allRecords = consumeRecordsByTopic(2);
        SourceRecord insertRecord = allRecords.allRecordsInOrder().get(0);
        assertValueField(insertRecord, getResolvedColumnName("after/id", logicalDecoder), "id_val");
        assertValueField(insertRecord, getResolvedColumnName("after/i_key", logicalDecoder), "i_key");
        assertValueField(insertRecord, getResolvedColumnName("after/c_id", logicalDecoder), "c_id");
        assertValueField(insertRecord, getResolvedColumnName("after/p_type", logicalDecoder), "p_type_val");
        assertValueField(insertRecord, getResolvedColumnName("after/p_id", logicalDecoder), "p_id_val");
        assertValueField(insertRecord, getResolvedColumnName("after/tx_id", logicalDecoder), "ffffff-ffff");
        assertValueField(insertRecord, getResolvedColumnName("after/status", logicalDecoder), "DONE");
        assertValueField(insertRecord, getResolvedColumnName("after/amount", logicalDecoder), 30.5);
        assertValueField(insertRecord, getResolvedColumnName("after/currency", logicalDecoder), "INR");
        assertValueField(insertRecord, getResolvedColumnName("after/loc", logicalDecoder), "JJXX+HR8");
        assertValueField(insertRecord, getResolvedColumnName("after/quantity", logicalDecoder), 10.0);
        assertValueField(insertRecord, getResolvedColumnName("after/o_type", logicalDecoder), null);
        assertValueField(insertRecord, getResolvedColumnName("after/o_created_at", logicalDecoder), 1722965400000000L);
        assertValueField(insertRecord, getResolvedColumnName("after/o_updated_at", logicalDecoder), 1722965400000000L);
        assertValueField(insertRecord, getResolvedColumnName("after/dis_details", logicalDecoder), "{\"name\": \"Something for display\"}");
        assertValueField(insertRecord, getResolvedColumnName("after/o_metadata", logicalDecoder), "{\"year\": 2000}");
        assertValueField(insertRecord, getResolvedColumnName("after/tx_data", logicalDecoder), "{\"tx_seq\": 500}");
        assertValueField(insertRecord, getResolvedColumnName("after/tx_ref_d", logicalDecoder), null);
        assertValueField(insertRecord, getResolvedColumnName("after/rw_d", logicalDecoder), null);
        assertValueField(insertRecord, getResolvedColumnName("after/meta", logicalDecoder), null);
        assertValueField(insertRecord, getResolvedColumnName("after/created_at", logicalDecoder), 1722965400000000L);
        assertValueField(insertRecord, getResolvedColumnName("after/updated_at", logicalDecoder), 1722965400000000L);
        assertValueField(insertRecord, getResolvedColumnName("after/deleted_at", logicalDecoder), null);
        assertValueField(insertRecord, getResolvedColumnName("after/version", logicalDecoder), 1);

        SourceRecord updateRecord = allRecords.allRecordsInOrder().get(1);

        assertValueField(updateRecord, getResolvedColumnName("after/id", logicalDecoder), "id_val");
        assertValueField(updateRecord, getResolvedColumnName("after/c_id", logicalDecoder), "c_id");
        assertValueField(updateRecord, getResolvedColumnName("after/updated_at", logicalDecoder), 1722965400000000L);
        assertValueField(updateRecord, getResolvedColumnName("after/status", logicalDecoder), "NOT AVAILABLE");
        assertValueField(updateRecord, getResolvedColumnName("after/version", logicalDecoder), 2);

        if (logicalDecoder.isYBOutput()) {
            // If decoder is not yboutput then all the other columns will be present as well.
            assertValueField(updateRecord, "after/i_key", null);
            assertValueField(updateRecord, "after/p_type", null);
            assertValueField(updateRecord, "after/p_id", null);
            assertValueField(updateRecord, "after/tx_id", null);
            assertValueField(updateRecord, "after/amount", null);
            assertValueField(updateRecord, "after/currency", null);
            assertValueField(updateRecord, "after/loc", null);
            assertValueField(updateRecord, "after/quantity", null);
            assertValueField(updateRecord, "after/o_type", null);
            assertValueField(updateRecord, "after/o_created_at", null);
            assertValueField(updateRecord, "after/o_updated_at", null);
            assertValueField(updateRecord, "after/dis_details", null);
            assertValueField(updateRecord, "after/o_metadata", null);
            assertValueField(updateRecord, "after/tx_data", null);
            assertValueField(updateRecord, "after/tx_ref_d", null);
            assertValueField(updateRecord, "after/rw_d", null);
            assertValueField(updateRecord, "after/meta", null);
            assertValueField(updateRecord, "after/created_at", null);
            assertValueField(updateRecord, "after/deleted_at", null);
        }
        else {
            // If decoder is not yboutput then all the other columns will be present as well.
            assertValueField(updateRecord, getResolvedColumnName("after/i_key", logicalDecoder), "i_key");
            assertValueField(updateRecord, getResolvedColumnName("after/p_type", logicalDecoder), "p_type_val");
            assertValueField(updateRecord, getResolvedColumnName("after/p_id", logicalDecoder), "p_id_val");
            assertValueField(updateRecord, getResolvedColumnName("after/tx_id", logicalDecoder), "ffffff-ffff");
            assertValueField(updateRecord, getResolvedColumnName("after/amount", logicalDecoder), 30.5);
            assertValueField(updateRecord, getResolvedColumnName("after/currency", logicalDecoder), "INR");
            assertValueField(updateRecord, getResolvedColumnName("after/loc", logicalDecoder), "JJXX+HR8");
            assertValueField(updateRecord, getResolvedColumnName("after/quantity", logicalDecoder), 10.0);
            assertValueField(updateRecord, getResolvedColumnName("after/o_type", logicalDecoder), null);
            assertValueField(updateRecord, getResolvedColumnName("after/o_created_at", logicalDecoder), 1722965400000000L);
            assertValueField(updateRecord, getResolvedColumnName("after/o_updated_at", logicalDecoder), 1722965400000000L);
            assertValueField(updateRecord, getResolvedColumnName("after/dis_details", logicalDecoder), "{\"name\": \"Something for display\"}");
            assertValueField(updateRecord, getResolvedColumnName("after/o_metadata", logicalDecoder), "{\"year\": 2000}");
            assertValueField(updateRecord, getResolvedColumnName("after/tx_data", logicalDecoder), "{\"tx_seq\": 500}");
            assertValueField(updateRecord, getResolvedColumnName("after/tx_ref_d", logicalDecoder), null);
            assertValueField(updateRecord, getResolvedColumnName("after/rw_d", logicalDecoder), null);
            assertValueField(updateRecord, getResolvedColumnName("after/meta", logicalDecoder), null);
            assertValueField(updateRecord, getResolvedColumnName("after/created_at", logicalDecoder), 1722965400000000L);
            assertValueField(updateRecord, getResolvedColumnName("after/updated_at", logicalDecoder), 1722965400000000L);
            assertValueField(updateRecord, getResolvedColumnName("after/deleted_at", logicalDecoder), null);
        }
    }

    public void assertColumnInUpdate(List<String> allColumns, SourceRecord record, String column, Object expectedValue,
                                     PostgresConnectorConfig.LogicalDecoder logicalDecoder) {
        if (logicalDecoder.isYBOutput()) {
            YBVerifyRecord.isValidUpdate(record, "id", 1);

            // Assert that the other columns are null - note that this is only supposed to work with CHANGE.
            for (String columnName : allColumns) {
                if (!column.contains(columnName)) {
                    assertValueField(record, "after/" + columnName, null);
                }
            }
        } else {
            VerifyRecord.isValidUpdate(record, "id", 1);
        }

        assertValueField(record, getResolvedColumnName(column, logicalDecoder), expectedValue);
    }

    /*
     * create table public.orders (
     *   order_reference_id character varying not null,
     *   idempotency_key character varying not null,
     *   customer_id character varying not null,
     *   product_type character varying not null,
     *   product_id character varying,
     *   txn_reference_id character varying,
     *   order_status character varying not null,
     *   order_amount numeric not null,
     *   order_currency character varying not null,
     *   location character varying,
     *   order_quantity numeric,
     *   order_type character varying,
     *   order_created_at timestamp without time zone,
     *   order_updated_at timestamp without time zone,
     *   order_display_details jsonb,
     *   order_metadata jsonb,
     *   txn_data jsonb,
     *   txn_refund_data jsonb,
     *   reward_data jsonb,
     *   metadata jsonb,
     *   created_at timestamp without time zone,
     *   updated_at timestamp without time zone not null,
     *   deleted_at timestamp without time zone,
     *   version integer not null default 0,
     *   primary key (updated_at, order_reference_id, customer_id)
     * );
     * create unique index orders_idempotency_key_key on orders using lsm (idempotency_key);
     * create index idx_updated_at on orders using lsm (updated_at);
     */

    @Ignore
    @Test
    public void shouldWorkForNumericTypesWithoutLengthAndScale() throws Exception {
        /*
            Fails with exception -

            org.apache.kafka.connect.errors.DataException: Invalid Java object for schema
            "io.debezium.data.VariableScaleDecimal" with type STRUCT: class [B for field: "value"
         */
        String createStmt = "CREATE TABLE numeric_type (id serial PRIMARY KEY, nm numeric);";

        execute(createStmt);

        start(YugabyteDBConnector.class,
          TestHelper.defaultConfig()
            .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.numeric_type")
            .with(PostgresConnectorConfig.SNAPSHOT_MODE, "never")
            .build());
        assertConnectorIsRunning();
        waitForStreamingToStart();
        consumer = testConsumer(1);

        consumer.expects(1);
        executeAndWait("INSERT INTO numeric_type VALUES (1, 12.34);");

        SourceRecord record = consumer.remove();
        assertValueField(record, "after/nm/value", 12.34);
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
        YBVerifyRecord.isValidDelete(record, PK_FIELD, 1);

        // followed by a tombstone
        record = consumer.remove();
        assertEquals(topicName, record.topic());
        YBVerifyRecord.isValidTombstone(record, PK_FIELD, 1);

        // second entry removed
        record = consumer.remove();
        assertEquals(topicName, record.topic());
        YBVerifyRecord.isValidDelete(record, PK_FIELD, 2);

        // followed by a tombstone
        record = consumer.remove();
        assertEquals(topicName, record.topic());
        YBVerifyRecord.isValidTombstone(record, PK_FIELD, 2);
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
        YBVerifyRecord.isValidDelete(record, PK_FIELD, 1);

        // second entry removed
        record = consumer.remove();
        assertEquals(topicName, record.topic());
        YBVerifyRecord.isValidDelete(record, PK_FIELD, 2);
    }

    @Test
    @FixFor("DBZ-4137")
    public void shouldReceiveNumericTypeAsDoubleWithNullDefaults() throws Exception {
        LogInterceptor logInterceptor = new LogInterceptor(PostgresStreamingChangeEventSource.class);
        TestHelper.execute(
                "DROP TABLE IF EXISTS numeric_table_with_n_defaults;",
                "CREATE TABLE numeric_table_with_n_defaults (\n" +
                        "    pk int4 PRIMARY KEY NOT NULL,\n" +
                        "    r_numeric numeric(19, 4) NULL DEFAULT NULL,\n" +
                        "    r_int int4 NULL DEFAULT NULL);",
                "ALTER TABLE numeric_table_with_n_defaults REPLICA IDENTITY FULL");

        TestHelper.waitFor(Duration.ofSeconds(10));

        startConnector(config -> config.with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE),
                false);

        consumer = testConsumer(1);

        // INSERT
        String statement = "INSERT INTO numeric_table_with_n_defaults (pk) VALUES (1);";

        Awaitility.await()
            .atMost(Duration.ofSeconds(50))
            .pollInterval(Duration.ofSeconds(1))
            .until(() -> logInterceptor.containsMessage("Processing messages"));

        assertInsert(
                statement,
                1,
                Arrays.asList(
                        new SchemaAndValueField("pk", Schema.INT32_SCHEMA, 1),
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
                        "    pk int4 PRIMARY KEY NOT NULL,\n" +
                        "    r_numeric numeric(19, 4) NOT NULL DEFAULT 1,\n" +
                        "    r_int int4 NOT NULL DEFAULT 2);",
                "ALTER TABLE numeric_table_with_defaults REPLICA IDENTITY FULL");

        TestHelper.waitFor(Duration.ofSeconds(10));

        startConnector(config -> config.with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE),
                false);
        consumer = testConsumer(1);

        // INSERT
        String statement = "INSERT INTO numeric_table_with_defaults (pk) VALUES (1);";
        assertInsert(
                statement,
                1,
                Arrays.asList(
                        new SchemaAndValueField("pk", Schema.INT32_SCHEMA, 1),
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
        YBVerifyRecord.isValidDelete(record, pk, 1);

        // followed by a tombstone
        record = consumer.remove();
        assertEquals(topicName, record.topic());
        YBVerifyRecord.isValidTombstone(record, pk, 1);
    }

    @Test
    @FixFor("DBZ-911")
    public void shouldRefreshSchemaOnUnchangedToastedDataWhenSchemaChanged() throws Exception {
        startConnector(config -> config
                .with(PostgresConnectorConfig.SCHEMA_REFRESH_MODE, SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST));

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

    @Ignore("Altering column not allowed while in replication, see https://github.com/yugabyte/yugabyte-db/issues/16625")
    @Test
    @FixFor("DBZ-842")
    public void shouldNotPropagateUnchangedToastedData() throws Exception {
        startConnector(config -> config
                .with(PostgresConnectorConfig.SCHEMA_REFRESH_MODE, SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST));

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

    @Ignore("Altering column not allowed while in replication, see https://github.com/yugabyte/yugabyte-db/issues/16625")
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

    @Ignore("Altering column not allowed while in replication, see https://github.com/yugabyte/yugabyte-db/issues/16625")
    @Test
    @FixFor("DBZ-6122")
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

    @Ignore("Altering column not allowed while in replication, see https://github.com/yugabyte/yugabyte-db/issues/16625")
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

    @Ignore("Altering column not allowed while in replication, see https://github.com/yugabyte/yugabyte-db/issues/16625")
    @Test
    @FixFor("DBZ-6122")
    public void shouldHandleToastedByteArrayColumn() throws Exception {
        Print.enable();
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY);");
        startConnector(Function.identity(), false);
        List<Integer> intList = IntStream.range(1, 100000).boxed().map((x) -> 19338).collect(Collectors.toList());
        final String toastedValue = RandomStringUtils.randomNumeric(10000);

        String statement = "ALTER TABLE test_toast_table ADD COLUMN not_toast integer;"
                + "ALTER TABLE test_toast_table ADD COLUMN bytea_array bytea[];"
                + "ALTER TABLE test_toast_table ALTER COLUMN bytea_array SET STORAGE EXTENDED;"
                + "INSERT INTO test_toast_table (not_toast, bytea_array) values (10, ARRAY ['" + toastedValue + "'::bytea]);";
        consumer = testConsumer(1);
        executeAndWait(statement);

        // after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("bytea_array",
                        SchemaBuilder.array(Schema.OPTIONAL_BYTES_SCHEMA).optional().build(), Arrays.asList(ByteBuffer.wrap(toastedValue.getBytes())))),
                consumer.remove(),
                Envelope.FieldName.AFTER);
        statement = "UPDATE test_toast_table SET not_toast = 2;";

        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "not_toast", "bytea_array"), tbl.retrieveColumnNames());
            });
        });
        final var record = consumer.remove();
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 2)),
                record,
                Envelope.FieldName.AFTER);
        final var after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        final var byteaArray = after.getArray("bytea_array");
        Assertions.assertThat(byteaArray).hasSize(1);
        Assertions.assertThat(byteaArray.get(0)).isEqualTo(DecoderDifferences.mandatoryToastedValueBinaryPlaceholder());
        Assertions.assertThat(after.schema().field("bytea_array").schema())
                .isEqualTo(SchemaBuilder.array(Schema.OPTIONAL_BYTES_SCHEMA).optional().build());
    }

    @Ignore("Altering column not allowed while in replication, see https://github.com/yugabyte/yugabyte-db/issues/16625")
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

    @Ignore("Altering column not allowed while in replication, see https://github.com/yugabyte/yugabyte-db/issues/16625")
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

    @Ignore("Altering column not allowed while in replication, see https://github.com/yugabyte/yugabyte-db/issues/16625")
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
    @FixFor("DBZ-6379")
    public void shouldHandleToastedHstoreInHstoreMapMode() throws Exception {
        TestHelper.execute("CREATE EXTENSION IF NOT EXISTS hstore SCHEMA public;");
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY, text TEXT, col hstore);");
        startConnector(config -> config.with(PostgresConnectorConfig.HSTORE_HANDLING_MODE, PostgresConnectorConfig.HStoreHandlingMode.MAP));
        final String toastedValue = RandomStringUtils.randomAlphanumeric(100000);
        String statement = "INSERT INTO test_toast_table (id, col) values (10, 'a=>" + toastedValue + "');";
        consumer = testConsumer(1);
        executeAndWait(statement);

        // after record should contain the toasted value
        HashMap colValue = new HashMap();
        colValue.put("a", toastedValue);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("col", SchemaBuilder.map(SchemaBuilder.STRING_SCHEMA,
                        SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build(), colValue)),
                consumer.remove(),
                Envelope.FieldName.AFTER);
        statement = "UPDATE test_toast_table SET text = 'text';";

        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "text", "col"), tbl.retrieveColumnNames());
            });
        });

        // YB Note: Value for 'col' will not be present since replica identity is CHANGE.
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "text")),
                consumer.remove(),
                Envelope.FieldName.AFTER);

        colValue.clear();
        colValue.put("a", "123456");
        consumer.expects(1);
        executeAndWait("UPDATE test_toast_table SET col = col || 'a=>\"123456\"'::hstore;");

        assertRecordSchemaAndValues(Arrays.asList(
                        new SchemaAndValueField("col", SchemaBuilder.map(SchemaBuilder.STRING_SCHEMA,
                                SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build(), colValue)),
                consumer.remove(),
                Envelope.FieldName.AFTER);
    }

    @Test
    public void shouldHandleHstoreWithPgOutput() throws Exception {
        TestHelper.execute("CREATE EXTENSION IF NOT EXISTS hstore SCHEMA public;");
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_hstore;",
                "CREATE TABLE test_hstore (id SERIAL PRIMARY KEY, text TEXT, col hstore);");

        // We will need to change the replica identity of all the tables so that the service
        // doesn't throw error.
        TestHelper.execute("ALTER TABLE test_hstore REPLICA IDENTITY DEFAULT;");
        TestHelper.execute("ALTER TABLE test_table REPLICA IDENTITY DEFAULT;");
        TestHelper.execute("ALTER TABLE table_with_interval REPLICA IDENTITY DEFAULT;");

        startConnector(config -> config
                .with(PostgresConnectorConfig.HSTORE_HANDLING_MODE, PostgresConnectorConfig.HStoreHandlingMode.MAP)
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT));
        waitForStreamingToStart();

        HashMap colValue = new HashMap();
        TestHelper.execute("INSERT INTO test_hstore values (1, 'text_val', 'a=>\"hstoreValue\"');");
        TestHelper.execute("UPDATE test_hstore SET text = 'text';");
        TestHelper.execute("UPDATE test_hstore SET col = col || 'a=>\"123456\"'::hstore;");

        SourceRecords allRecords = consumeRecordsByTopic(3);
        List<SourceRecord> records = allRecords.allRecordsInOrder();

        assertThat(records.size()).isEqualTo(3);

        colValue.put("a", "hstoreValue");

        // Assert insert record.
        VerifyRecord.isValidInsert(records.get(0), "id", 1);
        assertValueField(records.get(0), "after/id", 1);
        assertValueField(records.get(0), "after/text", "text_val");
        assertValueField(records.get(0), "after/col", colValue);

        // Assert update record.
        VerifyRecord.isValidUpdate(records.get(1), "id", 1);
        assertValueField(records.get(1), "after/id", 1);
        assertValueField(records.get(1), "after/text", "text");
        assertValueField(records.get(1), "after/col", colValue);

        colValue.clear();
        colValue.put("a", "123456");

        // Assert update record.
        VerifyRecord.isValidUpdate(records.get(2), "id", 1);
        assertValueField(records.get(2), "after/id", 1);
        assertValueField(records.get(2), "after/text", "text");
        assertValueField(records.get(2), "after/col", colValue);
    }

    @Ignore("Altering column not allowed while in replication, see https://github.com/yugabyte/yugabyte-db/issues/16625")
    @Test
    @FixFor("DBZ-6720")
    public void shouldHandleToastedUuidArrayColumn() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY, text TEXT);");
        startConnector(Function.identity(), false);
        final List<String> toastedValueList = Stream.generate(UUID::randomUUID).map(String::valueOf).limit(10000).collect(Collectors.toList());
        final String[] toastedValueArray = toastedValueList.toArray(new String[toastedValueList.size()]);
        final String toastedValueQuotedString = toastedValueList.stream().map(uuid_str -> ("'" + uuid_str + "'")).collect(Collectors.joining(","));

        String statement = "ALTER TABLE test_toast_table ADD COLUMN not_toast integer;"
                + "ALTER TABLE test_toast_table ADD COLUMN uuid_array uuid[];"
                + "ALTER TABLE test_toast_table ALTER COLUMN uuid_array SET STORAGE EXTENDED;"
                + "INSERT INTO test_toast_table (not_toast, text, uuid_array) "
                + "VALUES (10, 'text', ARRAY [" + toastedValueQuotedString + "]::uuid[]);";
        consumer = testConsumer(1);
        executeAndWait(statement);

        // after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "text"),
                new SchemaAndValueField("uuid_array", SchemaBuilder.array(
                        io.debezium.data.Uuid.builder().optional().build()).optional().build(),
                        Arrays.asList(toastedValueArray))),
                consumer.remove(),
                Envelope.FieldName.AFTER);
        statement = "UPDATE test_toast_table SET not_toast = 2;";

        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "text", "not_toast", "uuid_array"), tbl.retrieveColumnNames());
            });
        });
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 2),
                new SchemaAndValueField("text", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "text"),
                new SchemaAndValueField("uuid_array", SchemaBuilder.array(
                        io.debezium.data.Uuid.builder().optional().build()).optional().build(),
                        Arrays.asList(DecoderDifferences.mandatoryToastedValueUuidPlaceholder()))),
                consumer.remove(),
                Envelope.FieldName.AFTER);
    }

    @Ignore("Altering column not allowed while in replication, see https://github.com/yugabyte/yugabyte-db/issues/16625")
    @Test
    @FixFor("DBZ-7193")
    public void shouldHandleToastedArrayColumnForReplicaIdentityFullTable() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY);");

        startConnector(Function.identity(), false);
        assertConnectorIsRunning();
        final String toastedValue = RandomStringUtils.randomAlphanumeric(10000);

        // INSERT
        String statement = "ALTER TABLE test_toast_table ADD COLUMN not_toast integer;"
                + "ALTER TABLE test_toast_table ADD COLUMN mandatory_text_array TEXT[] NOT NULL;"
                + "ALTER TABLE test_toast_table ALTER COLUMN mandatory_text_array SET STORAGE EXTENDED;"
                + "ALTER TABLE test_toast_table REPLICA IDENTITY FULL;"
                + "INSERT INTO test_toast_table (not_toast, mandatory_text_array) values (10, ARRAY ['" + toastedValue + "']);";
        consumer = testConsumer(1);
        executeAndWait(statement);

        // after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("mandatory_text_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build(), Arrays.asList(toastedValue))),
                consumer.remove(),
                Envelope.FieldName.AFTER);

        // UPDATE
        statement = "UPDATE test_toast_table SET not_toast = 20;";
        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "not_toast", "mandatory_text_array"), tbl.retrieveColumnNames());
            });
        });
        SourceRecord updatedRecord = consumer.remove();

        // before and after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("mandatory_text_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build(), Arrays.asList(toastedValue))),
                updatedRecord, Envelope.FieldName.BEFORE);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 20),
                new SchemaAndValueField("mandatory_text_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build(), Arrays.asList(toastedValue))),
                updatedRecord, Envelope.FieldName.AFTER);
    }

    @Ignore("Altering column not allowed while in replication, see https://github.com/yugabyte/yugabyte-db/issues/16625")
    @Test
    @FixFor("DBZ-7193")
    public void shouldHandleToastedArrayColumnCharacterVaryingForReplicaIdentityFullTable() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY);");

        startConnector(Function.identity(), false);
        assertConnectorIsRunning();
        final String toastedValue = RandomStringUtils.randomAlphanumeric(10000);

        // INSERT
        String statement = "ALTER TABLE test_toast_table ADD COLUMN not_toast integer;"
                + "ALTER TABLE test_toast_table ADD COLUMN mandatory_text_array character varying(20000)[] NOT NULL;"
                + "ALTER TABLE test_toast_table ALTER COLUMN mandatory_text_array SET STORAGE EXTENDED;"
                + "ALTER TABLE test_toast_table REPLICA IDENTITY FULL;"
                + "INSERT INTO test_toast_table (not_toast, mandatory_text_array) values (10, ARRAY ['" + toastedValue + "']);";
        consumer = testConsumer(1);
        executeAndWait(statement);

        // after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("mandatory_text_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build(), Arrays.asList(toastedValue))),
                consumer.remove(),
                Envelope.FieldName.AFTER);

        // UPDATE
        statement = "UPDATE test_toast_table SET not_toast = 20;";
        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "not_toast", "mandatory_text_array"), tbl.retrieveColumnNames());
            });
        });
        SourceRecord updatedRecord = consumer.remove();

        // before and after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("mandatory_text_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build(), Arrays.asList(toastedValue))),
                updatedRecord, Envelope.FieldName.BEFORE);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 20),
                new SchemaAndValueField("mandatory_text_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build(), Arrays.asList(toastedValue))),
                updatedRecord, Envelope.FieldName.AFTER);
    }

    @Ignore("Altering column not allowed while in replication, see https://github.com/yugabyte/yugabyte-db/issues/16625")
    @Test
    @FixFor("DBZ-7193")
    public void shouldHandleToastedDateArrayColumnForReplicaIdentityFullTable() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY);");

        startConnector(Function.identity(), false);
        assertConnectorIsRunning();
        List<Integer> intList = IntStream.range(1, 100000).boxed().map((x) -> 19338).collect(Collectors.toList());
        final String toastedValue = intList.stream().map((x) -> "'2022-12-12'::date").collect(Collectors.joining(","));

        // INSERT
        String statement = "ALTER TABLE test_toast_table ADD COLUMN not_toast integer;"
                + "ALTER TABLE test_toast_table ADD COLUMN date_array date[];"
                + "ALTER TABLE test_toast_table ALTER COLUMN date_array SET STORAGE EXTENDED;"
                + "ALTER TABLE test_toast_table REPLICA IDENTITY FULL;"
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

        // UPDATE
        statement = "UPDATE test_toast_table SET not_toast = 20;";
        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "not_toast", "date_array"), tbl.retrieveColumnNames());
            });
        });
        SourceRecord updatedRecord = consumer.remove();

        // before and after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("date_array",
                        SchemaBuilder.array(SchemaBuilder.int32().name("io.debezium.time.Date").optional().version(1).build()).optional().build(),
                        intList)),
                updatedRecord, Envelope.FieldName.BEFORE);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 20),
                new SchemaAndValueField("date_array",
                        SchemaBuilder.array(SchemaBuilder.int32().name("io.debezium.time.Date").optional().version(1).build()).optional().build(),
                        intList)),
                updatedRecord, Envelope.FieldName.AFTER);
    }

    @Ignore("Altering column not allowed while in replication, see https://github.com/yugabyte/yugabyte-db/issues/16625")
    @Test
    @FixFor("DBZ-7193")
    public void shouldHandleToastedByteArrayColumnForReplicaIdentityFullTable() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY);");

        startConnector(Function.identity(), false);
        assertConnectorIsRunning();
        List<Integer> intList = IntStream.range(1, 100000).boxed().map((x) -> 19338).collect(Collectors.toList());
        final String toastedValue = RandomStringUtils.randomNumeric(10000);

        // INSERT
        String statement = "ALTER TABLE test_toast_table ADD COLUMN not_toast integer;"
                + "ALTER TABLE test_toast_table ADD COLUMN bytea_array bytea[];"
                + "ALTER TABLE test_toast_table ALTER COLUMN bytea_array SET STORAGE EXTENDED;"
                + "ALTER TABLE test_toast_table REPLICA IDENTITY FULL;"
                + "INSERT INTO test_toast_table (not_toast, bytea_array) values (10, ARRAY ['" + toastedValue + "'::bytea]);";
        consumer = testConsumer(1);
        executeAndWait(statement);

        // after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("bytea_array",
                        SchemaBuilder.array(Schema.OPTIONAL_BYTES_SCHEMA).optional().build(), Arrays.asList(ByteBuffer.wrap(toastedValue.getBytes())))),
                consumer.remove(),
                Envelope.FieldName.AFTER);

        // UPDATE
        statement = "UPDATE test_toast_table SET not_toast = 20;";
        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "not_toast", "bytea_array"), tbl.retrieveColumnNames());
            });
        });
        SourceRecord updatedRecord = consumer.remove();

        // before and after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("bytea_array",
                        SchemaBuilder.array(Schema.OPTIONAL_BYTES_SCHEMA).optional().build(),
                        Arrays.asList(ByteBuffer.wrap(toastedValue.getBytes())))),
                updatedRecord, Envelope.FieldName.BEFORE);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 20),
                new SchemaAndValueField("bytea_array",
                        SchemaBuilder.array(Schema.OPTIONAL_BYTES_SCHEMA).optional().build(),
                        Arrays.asList(ByteBuffer.wrap(toastedValue.getBytes())))),
                updatedRecord, Envelope.FieldName.AFTER);
    }

    @Ignore("Altering column not allowed while in replication, see https://github.com/yugabyte/yugabyte-db/issues/16625")
    @Test
    @FixFor("DBZ-7193")
    public void shouldHandleToastedIntegerArrayColumnForReplicaIdentityFullTable() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY);");

        startConnector(Function.identity(), false);
        assertConnectorIsRunning();
        List<Integer> intList = IntStream.range(1, 10000).boxed().collect(Collectors.toList());
        final String toastedValue = intList.stream().map(String::valueOf)
                .collect(Collectors.joining(","));

        // INSERT
        String statement = "ALTER TABLE test_toast_table ADD COLUMN not_toast integer;"
                + "ALTER TABLE test_toast_table ADD COLUMN int_array int[];"
                + "ALTER TABLE test_toast_table ALTER COLUMN int_array SET STORAGE EXTENDED;"
                + "ALTER TABLE test_toast_table REPLICA IDENTITY FULL;"
                + "INSERT INTO test_toast_table (not_toast, int_array) values (10, ARRAY [" + toastedValue + "]);";
        consumer = testConsumer(1);
        executeAndWait(statement);

        // after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("int_array", SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(), intList)),
                consumer.remove(),
                Envelope.FieldName.AFTER);

        // UPDATE
        statement = "UPDATE test_toast_table SET not_toast = 20;";
        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "not_toast", "int_array"), tbl.retrieveColumnNames());
            });
        });
        SourceRecord updatedRecord = consumer.remove();

        // before and after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("int_array", SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(), intList)),
                updatedRecord, Envelope.FieldName.BEFORE);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 20),
                new SchemaAndValueField("int_array", SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(), intList)),
                updatedRecord, Envelope.FieldName.AFTER);
    }

    @Ignore("Altering column not allowed while in replication, see https://github.com/yugabyte/yugabyte-db/issues/16625")
    @Test
    @FixFor("DBZ-7193")
    public void shouldHandleToastedBigIntArrayColumnForReplicaIdentityFullTable() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY);");

        startConnector(Function.identity(), false);
        assertConnectorIsRunning();
        List<Long> bigintList = LongStream.range(1, 10000).boxed().collect(Collectors.toList());
        final String toastedValue = bigintList.stream().map(String::valueOf)
                .collect(Collectors.joining(","));

        // INSERT
        String statement = "ALTER TABLE test_toast_table ADD COLUMN not_toast integer;"
                + "ALTER TABLE test_toast_table ADD COLUMN bigint_array bigint[];"
                + "ALTER TABLE test_toast_table ALTER COLUMN bigint_array SET STORAGE EXTENDED;"
                + "ALTER TABLE test_toast_table REPLICA IDENTITY FULL;"
                + "INSERT INTO test_toast_table (not_toast, bigint_array) values (10, ARRAY [" + toastedValue + "]);";
        consumer = testConsumer(1);
        executeAndWait(statement);

        // after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("bigint_array", SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build(), bigintList)),
                consumer.remove(),
                Envelope.FieldName.AFTER);

        // UPDATE
        statement = "UPDATE test_toast_table SET not_toast = 20;";
        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "not_toast", "bigint_array"), tbl.retrieveColumnNames());
            });
        });
        SourceRecord updatedRecord = consumer.remove();

        // before and after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("bigint_array", SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build(), bigintList)),
                updatedRecord, Envelope.FieldName.BEFORE);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 20),
                new SchemaAndValueField("bigint_array", SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build(), bigintList)),
                updatedRecord, Envelope.FieldName.AFTER);
    }

    @Ignore("Altering column not allowed while in replication, see https://github.com/yugabyte/yugabyte-db/issues/16625")
    @Test
    @FixFor("DBZ-7193")
    public void shouldHandleToastedUuidArrayColumnForReplicaIdentityFullTable() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS test_toast_table;",
                "CREATE TABLE test_toast_table (id SERIAL PRIMARY KEY);");

        startConnector(Function.identity(), false);
        assertConnectorIsRunning();
        final List<String> toastedValueList = Stream.generate(UUID::randomUUID).map(String::valueOf).limit(10000).collect(Collectors.toList());
        final String[] toastedValueArray = toastedValueList.toArray(new String[toastedValueList.size()]);
        final String toastedValueQuotedString = toastedValueList.stream().map(uuid_str -> ("'" + uuid_str + "'")).collect(Collectors.joining(","));

        // INSERT
        String statement = "ALTER TABLE test_toast_table ADD COLUMN not_toast integer;"
                + "ALTER TABLE test_toast_table ADD COLUMN uuid_array uuid[];"
                + "ALTER TABLE test_toast_table ALTER COLUMN uuid_array SET STORAGE EXTENDED;"
                + "ALTER TABLE test_toast_table REPLICA IDENTITY FULL;"
                + "INSERT INTO test_toast_table (not_toast, uuid_array) "
                + "VALUES (10, ARRAY [" + toastedValueQuotedString + "]::uuid[]);";
        consumer = testConsumer(1);
        executeAndWait(statement);

        // after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("uuid_array",
                        SchemaBuilder.array(io.debezium.data.Uuid.builder().optional().build()).optional().build(),
                        Arrays.asList(toastedValueArray))),
                consumer.remove(),
                Envelope.FieldName.AFTER);

        // UPDATE
        statement = "UPDATE test_toast_table SET not_toast = 20;";
        consumer.expects(1);
        executeAndWait(statement);
        consumer.process(record -> {
            assertWithTask(task -> {
                Table tbl = ((PostgresConnectorTask) task).getTaskContext().schema().tableFor(TableId.parse("public.test_toast_table", false));
                assertEquals(Arrays.asList("id", "not_toast", "uuid_array"), tbl.retrieveColumnNames());
            });
        });
        SourceRecord updatedRecord = consumer.remove();

        // before and after record should contain the toasted value
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10),
                new SchemaAndValueField("uuid_array",
                        SchemaBuilder.array(io.debezium.data.Uuid.builder().optional().build()).optional().build(),
                        Arrays.asList(toastedValueArray))),
                updatedRecord, Envelope.FieldName.BEFORE);
        assertRecordSchemaAndValues(Arrays.asList(
                new SchemaAndValueField("not_toast", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 20),
                new SchemaAndValueField("uuid_array",
                        SchemaBuilder.array(io.debezium.data.Uuid.builder().optional().build()).optional().build(),
                        Arrays.asList(toastedValueArray))),
                updatedRecord, Envelope.FieldName.AFTER);
    }

    @Ignore("Replica identity cannot be altered at runtime")
    @Test
    @FixFor("DBZ-1146")
    public void shouldReceiveChangesForReplicaIdentityFullTableWithToastedValueTableFromSnapshot() throws Exception {
        testReceiveChangesForReplicaIdentityFullTableWithToastedValue(SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST, true);
    }

    @Ignore("Replica identity cannot be altered at runtime")
    @Test
    @FixFor("DBZ-1146")
    public void shouldReceiveChangesForReplicaIdentityFullTableWithToastedValueTableFromStreaming() throws Exception {
        testReceiveChangesForReplicaIdentityFullTableWithToastedValue(SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST, false);
    }

    @Ignore("Replica identity cannot be altered at runtime")
    @Test
    @FixFor("DBZ-1146")
    public void shouldReceiveChangesForReplicaIdentityFullTableWithToastedValueTableFromSnapshotFullDiff() throws Exception {
        testReceiveChangesForReplicaIdentityFullTableWithToastedValue(SchemaRefreshMode.COLUMNS_DIFF, true);
    }

    @Ignore("Replica identity cannot be altered at runtime")
    @Test
    @FixFor("DBZ-1146")
    public void shouldReceiveChangesForReplicaIdentityFullTableWithToastedValueTableFromStreamingFullDiff() throws Exception {
        testReceiveChangesForReplicaIdentityFullTableWithToastedValue(SchemaRefreshMode.COLUMNS_DIFF, false);
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
        assertSourceInfo(rec, "yugabyte", "public", "test_table");

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
        assertSourceInfo(rec, "yugabyte", "public", "test_table");

        Struct source = ((Struct) rec.value()).getStruct("source");
        assertThat(source.getInt64("xmin")).isGreaterThan(0L);

        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    public void shouldProcessLargerTx() throws Exception {
        Print.disable();
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
            YBVerifyRecord.isValidInsert(record, PK_FIELD, i + 2);
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
            YBVerifyRecord.isValidInsert(record, PK_FIELD, i + 1002);
        }
        logger.info("Many tx duration = {} ms", stopwatch.durations().statistics().getTotal().toMillis());
    }

    @Test
    @FixFor("DBZ-1824")
    public void stopInTheMiddleOfTxAndResume() throws Exception {
        Print.enable();
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
            YBVerifyRecord.isValidInsert(record, PK_FIELD, i + 2);
        }

        stopConnector();

        startConnector(Function.identity(), false);
        consumer.expects(expectSecondRun);
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        // 20..51
        for (int i = 0; i < expectSecondRun; i++) {
            SourceRecord record = consumer.remove();
            assertEquals(topicName, record.topic());
            YBVerifyRecord.isValidInsert(record, PK_FIELD, STOP_ID + i);
        }
    }

    @Test
    @FixFor("DBZ-2397")
    public void restartConnectorInTheMiddleOfUncommittedTx() throws Exception {
        Print.enable();

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
        Print.enable();

        startConnector(config -> config
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false)
                .with(EmbeddedEngineConfig.OFFSET_STORAGE, MemoryOffsetBackingStore.class), true);
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
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(EmbeddedEngineConfig.OFFSET_STORAGE, MemoryOffsetBackingStore.class), false);

        consumer.expects(3);
        consumer.await(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS);

        // After loss of offset and not doing snapshot we always stream the first record available in replication slot
        // even if we have seen it as it is not possible to make a difference from plain snapshot never mode
        assertThat(((Struct) consumer.remove().value()).getStruct("after").getStruct("text").getString("value")).isEqualTo("insert2");

        assertThat(((Struct) consumer.remove().value()).getStruct("after").getStruct("text").getString("value")).isEqualTo("insert3");
        assertThat(((Struct) consumer.remove().value()).getStruct("after").getStruct("text").getString("value")).isEqualTo("insert4");

        stopConnector();
    }

    @Ignore("YB Note: Truncate events are unsupported")
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
        YBVerifyRecord.isValidInsert(record, PK_FIELD, 2);

        consumer.expects(1);
        TestHelper.execute("TRUNCATE TABLE public.test_table RESTART IDENTITY CASCADE;");
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        assertFalse(consumer.isEmpty());
        SourceRecord truncateRecord = consumer.remove();
        assertNotNull(truncateRecord);
        YBVerifyRecord.isValidTruncate(truncateRecord);
        assertTrue(consumer.isEmpty());
    }

    @Ignore("Decimal handling mode precise is unsupported")
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
        assertSourceInfo(rec, "yugabyte", "public", "alias_table");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField("pk", SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("data", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "hello"),
                new SchemaAndValueField("salary", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA, 7.25),
                new SchemaAndValueField("salary2", SchemaBuilder.FLOAT64_SCHEMA, 8.25),
                new SchemaAndValueField("salary3", SchemaBuilder.float64()
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "MONEY3")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, "8")
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "3")
                        .parameter(TestHelper.COLUMN_NAME_PARAMETER_KEY, "salary3")
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
        assertSourceInfo(rec, "yugabyte", "public", "alias_table");

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
        assertSourceInfo(rec, "yugabyte", "public", "alias_table");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField(PK_FIELD, SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("value", SpecialValueDecimal.builder(DecimalMode.DOUBLE, 8, 2)
                        .optional()
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "NUMERICEX")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, "8")
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "2")
                        .parameter(TestHelper.COLUMN_NAME_PARAMETER_KEY, "value")
                        .build(), 123.45));

        assertRecordSchemaAndValues(expected, rec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    public void shouldStreamValudForAliasLikeIntegerType() throws Exception {
        TestHelper.execute("CREATE DOMAIN integer_alias AS integer;");
        TestHelper.execute("CREATE TABLE test_alias_table (pk SERIAL PRIMARY KEY, alias_col integer_alias);");
        startConnector(config -> config
               .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
               .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.test_alias_table"),
               false);

        waitForStreamingToStart();
        TestHelper.waitFor(Duration.ofSeconds(30));

        TestHelper.execute("INSERT INTO test_alias_table (alias_col) VALUES (1234);");

        SourceRecords allRecords = consumeRecordsByTopic(1);
        assertEquals(1, allRecords.allRecordsInOrder().size());

        SourceRecord r = allRecords.recordsForTopic(topicName("public.test_alias_table")).get(0);

        assertValueField(r, "after/pk/value", 1);
        assertValueField(r, "after/alias_col/value", 1234);
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
        TestHelper.execute("CREATE DOMAIN interval2 as interval;");
        TestHelper.execute("CREATE DOMAIN char2 as char;");
        TestHelper.execute("CREATE DOMAIN text2 as text;");
        TestHelper.execute("CREATE DOMAIN json2 as json;");
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
                + "ADD COLUMN interval_base interval NOT NULL, ADD COLUMN interval_alias interval2 NOT NULL, "
                + "ADD COLUMN char_base char NOT NULL, ADD COLUMN char_alias char2 NOT NULL, "
                + "ADD COLUMN text_base text NOT NULL, ADD COLUMN text_alias text2 NOT NULL, "
                + "ADD COLUMN json_base json NOT NULL, ADD COLUMN json_alias json2 NOT NULL, "
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
                + "interval_base, interval_alias, "
                + "char_base, char_alias, "
                + "text_base, text_alias, "
                + "json_base, json_alias, "
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
                + "'1 year 2 months 3 days 4 hours 5 minutes 6 seconds', '1 year 2 months 3 days 4 hours 5 minutes 6 seconds', "
                + "'a', 'a', "
                + "'Hello World', 'Hello World', "
                + "'{\"key\": \"value\"}', '{\"key\": \"value\"}', "
                + "'40e6215d-b5c6-4896-987c-f30f3678f608', '40e6215d-b5c6-4896-987c-f30f3678f608', "
                + "B'101', B'101', "
                + "'192.168.0.1', '192.168.0.1', "
                + "'192.168/24', '192.168/24', "
                + "'08:00:2b:01:02:03', '08:00:2b:01:02:03' "
                + ");");

        SourceRecord rec = assertRecordInserted("public.alias_table", PK_FIELD, 1);
        assertSourceInfo(rec, "yugabyte", "public", "alias_table");

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
        assertSourceInfo(rec, "yugabyte", "public", "enum_table");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField(PK_FIELD, SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("value", Enum.builder("V1,V2")
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "TEST_TYPE")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, String.valueOf(Integer.MAX_VALUE))
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                        .parameter(TestHelper.COLUMN_NAME_PARAMETER_KEY, "value")
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
        assertSourceInfo(rec, "yugabyte", "public", "enum_table");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField(PK_FIELD, SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("data", SchemaBuilder.string().build(), "V1"),
                new SchemaAndValueField("value", Enum.builder("V1,V2")
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "TEST_TYPE")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, String.valueOf(Integer.MAX_VALUE))
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                        .parameter(TestHelper.COLUMN_NAME_PARAMETER_KEY, "value")
                        .defaultValue("V2")
                        .build(), "V2"));

        assertRecordSchemaAndValues(expected, rec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Ignore("YB Note: Enum array unsupported")
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
        TestHelper.waitFor(Duration.ofSeconds(10));

        SourceRecord insertRec = assertRecordInserted("public.enum_array_table", PK_FIELD, 1);
        assertSourceInfo(insertRec, "yugabyte", "public", "enum_array_table");

        List<SchemaAndValueField> expectedInsert = Arrays.asList(
                new SchemaAndValueField(PK_FIELD, SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("value", SchemaBuilder.array(Enum.builder("V1,V2"))
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "_TEST_TYPE")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, String.valueOf(Integer.MAX_VALUE))
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                        .parameter(TestHelper.COLUMN_NAME_PARAMETER_KEY, "value")
                        .build(), Arrays.asList("V1", "V2")));
        assertRecordSchemaAndValues(expectedInsert, insertRec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();

        // UPDATE
        executeAndWait("UPDATE enum_array_table set value = '{V1}';");
        SourceRecord updateRec = consumer.remove();
        assertSourceInfo(updateRec, "yugabyte", "public", "enum_array_table");

        List<SchemaAndValueField> expectedUpdate = Arrays.asList(
                new SchemaAndValueField(PK_FIELD, SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("value", SchemaBuilder.array(Enum.builder("V1,V2"))
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "_TEST_TYPE")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, String.valueOf(Integer.MAX_VALUE))
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                        .parameter(TestHelper.COLUMN_NAME_PARAMETER_KEY, "value")
                        .build(), Arrays.asList("V1")));
        assertRecordSchemaAndValues(expectedUpdate, updateRec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();

        // DELETE
        executeAndWait("DELETE FROM enum_array_table;");
        SourceRecord deleteRec = consumer.remove();
        YBVerifyRecord.isValidDelete(deleteRec, PK_FIELD, 1);
        assertSourceInfo(updateRec, "yugabyte", "public", "enum_array_table");
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
        assertSourceInfo(insert, "yugabyte", "public", "time_array_table");
        assertRecordSchemaAndValues(schemaAndValuesForTimeArrayTypes(), insert, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();

        // UPDATE
        executeAndWait("UPDATE time_array_table SET "
                + "timea = '{00:01:02,02:03:04}', "
                + "timetza = '{00:01:02-0400,01:03:04-0400}', "
                + "timestampa = '{2020-04-01 00:01:02,2020-04-25 03:04:05}', "
                + "timestamptza = '{2020-04-01 00:01:02-04,2020-04-25 03:04:05-04}'");

        SourceRecord update = consumer.remove();
        assertSourceInfo(update, "yugabyte", "public", "time_array_table");

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
                        Arrays.asList("2020-04-01T04:01:02.000000Z", "2020-04-25T07:04:05.000000Z")));
        assertRecordSchemaAndValues(expectedUpdate, update, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();

        // DELETE
        executeAndWait("DELETE FROM time_array_table;");
        SourceRecord deleteRec = consumer.remove();
        YBVerifyRecord.isValidDelete(deleteRec, PK_FIELD, 1);
        assertSourceInfo(deleteRec, "yugabyte", "public", "time_array_table");
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
        assertSourceInfo(rec, "yugabyte", "public", "enum_table");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField(PK_FIELD, SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("data", Schema.STRING_SCHEMA, "hello"),
                new SchemaAndValueField("value", Enum.builder("V1,V2")
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "TEST_TYPE")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, String.valueOf(Integer.MAX_VALUE))
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                        .parameter(TestHelper.COLUMN_NAME_PARAMETER_KEY, "value")
                        .defaultValue("V1")
                        .build(), "V1"));

        assertRecordSchemaAndValues(expected, rec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();
    }

    private void testReceiveChangesForReplicaIdentityFullTableWithToastedValue(SchemaRefreshMode mode, boolean tablesBeforeStart)
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

    @Ignore("YB Note: We do not populate length and scale")
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
        assertEquals(1, ((Struct) record.value()).getStruct("after").getStruct("b").getInt32("value").intValue());

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
        assertEquals(10, ((Struct) record.value()).getStruct("after").getStruct("b").getInt32("value").intValue());

        fields = record.keySchema().fields();
        for (int i = 0; i < fields.size(); i++) {
            assertEquals("Key field names should in order", expectedFieldOrder[i], fields.get(i).name());
        }
    }

    @Ignore("Decimal handling mode precise unsupported")
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
        assertSourceInfo(rec, "yugabyte", "public", "salary");

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
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.salary"),
                false);

        waitForStreamingToStart();

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO salary (name, salary) values ('Joe', 123.45);");

        SourceRecord rec = assertRecordInserted("public.salary", PK_FIELD, 1);
        assertSourceInfo(rec, "yugabyte", "public", "salary");

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
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public.salary"),
                false);

        waitForStreamingToStart();

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO salary (name, salary) values ('Joe', 123.45);");

        SourceRecord rec = assertRecordInserted("public.salary", PK_FIELD, 1);
        assertSourceInfo(rec, "yugabyte", "public", "salary");

        List<SchemaAndValueField> expected = Arrays.asList(
                new SchemaAndValueField("pk", SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("name", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "Joe"),
                new SchemaAndValueField("salary", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA, 123.45));

        assertRecordSchemaAndValues(expected, rec, Envelope.FieldName.AFTER);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Ignore("Decimal handling mode precise unsupported")
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

    @Test
    @FixFor("DBZ-6648")
    public void shouldHandleNonNullIntervalFiledDelete() throws Exception {
        TestHelper.execute("CREATE TABLE test_interval (pk SERIAL, i interval NOT NULL, PRIMARY KEY(pk));");
        // add a new entry and remove both
        String statements = "INSERT INTO test_interval (pk, i) VALUES (1, '2 Months 3 Days');" +
                "DELETE FROM test_interval WHERE pk = 1;";

        startConnector(config -> config.with(PostgresConnectorConfig.INTERVAL_HANDLING_MODE, IntervalHandlingMode.STRING));
        waitForStreamingToStart();

        consumer = testConsumer(3);
        executeAndWait(statements);

        String topicPrefix = "public.test_interval";
        String topicName = topicName(topicPrefix);
        assertRecordInserted(topicPrefix, PK_FIELD, 1);

        // entry removed
        SourceRecord record = consumer.remove();
        assertEquals(topicName, record.topic());
        YBVerifyRecord.isValidDelete(record, PK_FIELD, 1);

        // followed by a tombstone
        record = consumer.remove();
        assertEquals(topicName, record.topic());
        YBVerifyRecord.isValidTombstone(record, PK_FIELD, 1);
    }

    private void assertHeartBeatRecord(SourceRecord heartbeat) {
        assertEquals("__debezium-heartbeat." + TestHelper.TEST_SERVER, heartbeat.topic());

        Struct key = (Struct) heartbeat.key();
        assertThat(key.get("serverName")).isEqualTo(TestHelper.TEST_SERVER);

        Struct value = (Struct) heartbeat.value();
        assertThat(value.getInt64("ts_ms")).isLessThanOrEqualTo(Instant.now().toEpochMilli());
    }

    private void waitForSeveralHeartbeats() {
        final AtomicInteger heartbeatCount = new AtomicInteger();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
            final SourceRecord record = consumeRecord();
            if (record != null) {
                if (record.topic().equalsIgnoreCase("__debezium-heartbeat.test_server")) {
                    assertHeartBeatRecord(record);
                    heartbeatCount.incrementAndGet();
                }
            }
            return heartbeatCount.get() > 10;
        });
    }

    private String getReplicationSlotChangesQuery() {
        switch (TestHelper.decoderPlugin()) {
            case DECODERBUFS:
                return "SELECT pg_logical_slot_get_binary_changes('" + ReplicationConnection.Builder.DEFAULT_SLOT_NAME + "', " +
                        "NULL, NULL)";
            case PGOUTPUT:
                return "SELECT pg_logical_slot_get_binary_changes('" + ReplicationConnection.Builder.DEFAULT_SLOT_NAME + "', " +
                        "NULL, NULL, 'proto_version', '1', 'publication_names', '" + ReplicationConnection.Builder.DEFAULT_PUBLICATION_NAME + "')";
        }
        throw new UnsupportedOperationException("Test must be updated for new logical decoder type.");
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
            assertSourceInfo(record, "yugabyte", table.schema(), table.table());
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
            YBVerifyRecord.isValidInsert(insertedRecord, pkColumn, pk);
        }
        else {
            YBVerifyRecord.isValidInsert(insertedRecord);
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
            YBVerifyRecord.isValidDelete(deletedRecord, pkColumn, pk);
        }
        else {
            YBVerifyRecord.isValidDelete(deletedRecord);
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

    @Override
    protected Consumer<SourceRecord> getConsumer(Predicate<SourceRecord> isStopRecord, Consumer<SourceRecord> recordArrivedListener, boolean ignoreRecordsAfterStop) {
        return (record) -> {
            // YB Note: Do not consume heartbeat record.
            if (record.topic().equals(TestHelper.getDefaultHeartbeatTopic())) {
                return;
            }

            if (isStopRecord != null && isStopRecord.test(record)) {
                logger.error("Stopping connector after record as requested");
                throw new ConnectException("Stopping connector after record as requested");
            }
            // Test stopped the connector, remaining records are ignored
            if (ignoreRecordsAfterStop && (!isEngineRunning.get() || Thread.currentThread().isInterrupted())) {
                return;
            }
            while (!consumedLines.offer(record)) {
                if (ignoreRecordsAfterStop && (!isEngineRunning.get() || Thread.currentThread().isInterrupted())) {
                    return;
                }
            }
            recordArrivedListener.accept(record);
        };
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.logging.LogInterceptor;

// TODO Vaibhav: Enabling this test doesn't make sense unless we populate the default value of the
//   columns in the schema.
public class PostgresDefaultValueConverterIT extends AbstractConnectorTest {

    @Before
    public void before() throws SQLException {
        initializeConnectorTestFramework();

        TestHelper.dropAllSchemas();
    }

    @After
    public void after() {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    @Test
    @FixFor({ "DBZ-4736", "DBZ-5384" })
    public void shouldSetTheNullValueInSnapshot() throws Exception {
        createTableAndInsertData();

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1");
        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted("postgres", TestHelper.TEST_SERVER);
        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("test_server.s1.a")).hasSize(1);

        final SourceRecord sourceRecord = records.allRecordsInOrder().get(0);
        assertDefaultValueChangeRecord(sourceRecord);
    }

    @Test
    @FixFor({ "DBZ-4736", "DBZ-5384" })
    public void shouldSetTheNullValueInStreaming() throws Exception {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1");
        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted("postgres", TestHelper.TEST_SERVER);

        createTableAndInsertData();

        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("test_server.s1.a")).hasSize(1);

        final SourceRecord sourceRecord = records.allRecordsInOrder().get(0);
        assertDefaultValueChangeRecord(sourceRecord);
    }

    @Test
    @FixFor("DBZ-5340")
    @SkipWhenDatabaseVersion(check = EqualityCheck.LESS_THAN, major = 13, reason = "gen_random_uuid() available on PG13+ without explicitly using pgcrypto")
    public void testShouldHandleDefaultValueFunctionsWithSchemaPrefixes() throws Exception {
        final String ddl = "DROP SCHEMA IF EXISTS s1 CASCADE;"
                + "CREATE SCHEMA s1;"
                + "CREATE SCHEMA s2;"
                + "CREATE OR REPLACE FUNCTION s2.tst_generate_random_uuid() returns uuid as 'select gen_random_uuid()' language sql;"
                + "CREATE TABLE s1.dbz5340 (id uuid default s2.tst_generate_random_uuid() not null, data text);"
                + "ALTER TABLE s1.dbz5340 REPLICA IDENTITY FULL;";

        final LogInterceptor logInterceptor = new LogInterceptor(PostgresDefaultValueConverter.class);

        TestHelper.execute(ddl);

        Configuration config = TestHelper.defaultConfig().build();
        start(YugabyteDBConnector.class, config);

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        assertThat(logInterceptor.containsMessage("Cannot parse column default value 's2.tst_generate_random_uuid()' to type 'uuid'")).isFalse();

        // Verify default value sets the right schema value
        TestHelper.execute("INSERT INTO s1.dbz5340 (data) values ('test');");

        final SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("s1.dbz5340"));

        assertThat(recordsForTopic).hasSize(1);

        final Struct after = ((Struct) recordsForTopic.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("id")).isNotNull();
        assertThat(after.get("data")).isEqualTo("test");

        final Object defaultValue = after.schema().field("id").schema().defaultValue();
        assertThat(defaultValue).isEqualTo("00000000-0000-0000-0000-000000000000");
    }

    private void createTableAndInsertData() {
        final String dml = "INSERT INTO s1.a (pk) VALUES (1);";
        final String ddl = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
                "CREATE SCHEMA s1; " +
                "CREATE TABLE s1.a (pk SERIAL, dint integer DEFAULT NULL::integer, "
                + "dvc1 varchar(64) DEFAULT NULL::character varying, "
                + "dvc2 varchar(64) DEFAULT 'NULL', "
                + "dvc3 varchar(64) DEFAULT 'MYVALUE', "
                + "dvc4 varchar(64) DEFAULT 'NULL'::character varying, "
                + "dvc5 varchar(64) DEFAULT 'NULL::character varying', "
                + "dvc6 varchar(64) DEFAULT NULL, "
                + "dt1 timestamp DEFAULT CURRENT_TIMESTAMP, "
                + "dt2 date DEFAULT CURRENT_DATE, "
                + "dt3 time DEFAULT CURRENT_TIME, "
                + "PRIMARY KEY(pk));";
        TestHelper.execute(ddl);
        TestHelper.execute(dml);
    }

    private void assertDefaultValueChangeRecord(SourceRecord sourceRecord) {
        final Schema valueSchema = sourceRecord.valueSchema();

        assertThat(((Struct) sourceRecord.value()).getStruct("after").getStruct("dint").get("value")).isNull();
        assertThat(((Struct) sourceRecord.value()).getStruct("after").getStruct("dvc1").get("value")).isNull();
        assertThat(((Struct) sourceRecord.value()).getStruct("after").getStruct("dvc2").get("value")).isEqualTo("NULL");
        assertThat(((Struct) sourceRecord.value()).getStruct("after").getStruct("dvc3").get("value")).isEqualTo("MYVALUE");
        assertThat(((Struct) sourceRecord.value()).getStruct("after").getStruct("dvc4").get("value")).isEqualTo("NULL");
        assertThat(((Struct) sourceRecord.value()).getStruct("after").getStruct("dvc5").get("value")).isEqualTo("NULL::character varying");
        assertThat(((Struct) sourceRecord.value()).getStruct("after").getStruct("dvc6").get("value")).isNull();
        assertThat(((Struct) sourceRecord.value()).getStruct("after").getStruct("dt1").get("value")).isNotNull();
        assertThat(((Struct) sourceRecord.value()).getStruct("after").getStruct("dt2").get("value")).isNotNull();
        assertThat(((Struct) sourceRecord.value()).getStruct("after").getStruct("dt3").get("value")).isNotNull();

        // YB Note: We do not populate the default value while sending replication messages.
        assertThat(valueSchema.field("after").schema().field("dint").schema().defaultValue()).isNull();
        assertThat(valueSchema.field("after").schema().field("dvc1").schema().defaultValue()).isNull();
        assertThat(valueSchema.field("after").schema().field("dvc2").schema().defaultValue()).isEqualTo("NULL");
        assertThat(valueSchema.field("after").schema().field("dvc3").schema().defaultValue()).isEqualTo("MYVALUE");
        assertThat(valueSchema.field("after").schema().field("dvc4").schema().defaultValue()).isEqualTo("NULL");
        assertThat(valueSchema.field("after").schema().field("dvc5").schema().defaultValue()).isEqualTo("NULL::character varying");
        assertThat(valueSchema.field("after").schema().field("dvc6").schema().defaultValue()).isNull();
        assertThat(valueSchema.field("after").schema().field("dt1").schema().defaultValue()).isEqualTo(0L);
        assertThat(valueSchema.field("after").schema().field("dt2").schema().defaultValue()).isEqualTo(0);
        assertThat(valueSchema.field("after").schema().field("dt3").schema().defaultValue()).isEqualTo(0L);
    }
}

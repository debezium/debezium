/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;

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
    @FixFor("DBZ-4736")
    public void shouldSetTheNullValueInSnapshot() throws Exception {
        final String dml = "INSERT INTO s1.a (pk) VALUES (1);";
        final String ddl = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
                "CREATE SCHEMA s1; " +
                "CREATE TABLE s1.a (pk SERIAL, dint integer DEFAULT NULL::integer, "
                + "dvc1 varchar(64) DEFAULT NULL::character varying, "
                + "dvc2 varchar(64) DEFAULT 'NULL', "
                + "dvc3 varchar(64) DEFAULT 'MYVALUE', "
                + "dvc4 varchar(64) DEFAULT 'NULL'::character varying, "
                + "dvc5 varchar(64) DEFAULT 'NULL::character varying', "
                + "dvc6 varchar(64) DEFAULT NULL, PRIMARY KEY(pk));";

        TestHelper.execute(ddl);
        TestHelper.execute(dml);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1");
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted("postgres", TestHelper.TEST_SERVER);
        final SourceRecords records = consumeRecordsByTopic(1);
        Assertions.assertThat(records.recordsForTopic("test_server.s1.a")).hasSize(1);

        final SourceRecord sourceRecord = records.allRecordsInOrder().get(0);
        final Schema valueSchema = sourceRecord.valueSchema();

        Assertions.assertThat(((Struct) sourceRecord.value()).getStruct("after").getInt32("dint")).isNull();
        Assertions.assertThat(((Struct) sourceRecord.value()).getStruct("after").getString("dvc1")).isNull();
        Assertions.assertThat(((Struct) sourceRecord.value()).getStruct("after").getString("dvc2")).isEqualTo("NULL");
        Assertions.assertThat(((Struct) sourceRecord.value()).getStruct("after").getString("dvc3")).isEqualTo("MYVALUE");
        Assertions.assertThat(((Struct) sourceRecord.value()).getStruct("after").getString("dvc4")).isEqualTo("NULL");
        Assertions.assertThat(((Struct) sourceRecord.value()).getStruct("after").getString("dvc5")).isEqualTo("NULL::character varying");
        Assertions.assertThat(((Struct) sourceRecord.value()).getStruct("after").getString("dvc6")).isNull();

        Assertions.assertThat(valueSchema.field("after").schema().field("dint").schema().defaultValue()).isNull();
        Assertions.assertThat(valueSchema.field("after").schema().field("dvc1").schema().defaultValue()).isNull();
        Assertions.assertThat(valueSchema.field("after").schema().field("dvc2").schema().defaultValue()).isEqualTo("NULL");
        Assertions.assertThat(valueSchema.field("after").schema().field("dvc3").schema().defaultValue()).isEqualTo("MYVALUE");
        Assertions.assertThat(valueSchema.field("after").schema().field("dvc4").schema().defaultValue()).isEqualTo("NULL");
        Assertions.assertThat(valueSchema.field("after").schema().field("dvc5").schema().defaultValue()).isEqualTo("NULL::character varying");
        Assertions.assertThat(valueSchema.field("after").schema().field("dvc6").schema().defaultValue()).isNull();
    }

    @Test
    @FixFor("DBZ-4736")
    public void shouldSetTheNullValueInStreaming() throws Exception {
        final String dml = "INSERT INTO s1.a (pk) VALUES (1);";
        final String ddl = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
                "CREATE SCHEMA s1; " +
                "CREATE TABLE s1.a (pk SERIAL, dint integer DEFAULT NULL::integer, "
                + "dvc1 varchar(64) DEFAULT NULL::character varying, "
                + "dvc2 varchar(64) DEFAULT 'NULL', "
                + "dvc3 varchar(64) DEFAULT 'MYVALUE', "
                + "dvc4 varchar(64) DEFAULT 'NULL'::character varying, "
                + "dvc5 varchar(64) DEFAULT 'NULL::character varying', "
                + "dvc6 varchar(64) DEFAULT NULL, PRIMARY KEY(pk));";

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1");
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute(ddl);
        TestHelper.execute(dml);

        final SourceRecords records = consumeRecordsByTopic(1);
        Assertions.assertThat(records.recordsForTopic("test_server.s1.a")).hasSize(1);

        final SourceRecord sourceRecord = records.allRecordsInOrder().get(0);
        final Schema valueSchema = sourceRecord.valueSchema();

        Assertions.assertThat(((Struct) sourceRecord.value()).getStruct("after").getInt32("dint")).isNull();
        Assertions.assertThat(((Struct) sourceRecord.value()).getStruct("after").getString("dvc1")).isNull();
        Assertions.assertThat(((Struct) sourceRecord.value()).getStruct("after").getString("dvc2")).isEqualTo("NULL");
        Assertions.assertThat(((Struct) sourceRecord.value()).getStruct("after").getString("dvc3")).isEqualTo("MYVALUE");
        Assertions.assertThat(((Struct) sourceRecord.value()).getStruct("after").getString("dvc4")).isEqualTo("NULL");
        Assertions.assertThat(((Struct) sourceRecord.value()).getStruct("after").getString("dvc5")).isEqualTo("NULL::character varying");
        Assertions.assertThat(((Struct) sourceRecord.value()).getStruct("after").getString("dvc6")).isNull();

        Assertions.assertThat(valueSchema.field("after").schema().field("dint").schema().defaultValue()).isNull();
        Assertions.assertThat(valueSchema.field("after").schema().field("dvc1").schema().defaultValue()).isNull();
        Assertions.assertThat(valueSchema.field("after").schema().field("dvc2").schema().defaultValue()).isEqualTo("NULL");
        Assertions.assertThat(valueSchema.field("after").schema().field("dvc3").schema().defaultValue()).isEqualTo("MYVALUE");
        Assertions.assertThat(valueSchema.field("after").schema().field("dvc4").schema().defaultValue()).isEqualTo("NULL");
        Assertions.assertThat(valueSchema.field("after").schema().field("dvc5").schema().defaultValue()).isEqualTo("NULL::character varying");
        Assertions.assertThat(valueSchema.field("after").schema().field("dvc6").schema().defaultValue()).isNull();
    }
}

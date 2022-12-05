/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration test for the user-facing history topic of the Debezium Oracle Server connector.
 * <p>
 * The tests should verify the {@code CREATE} schema events from snapshot and the {@code CREATE} and
 * the {@code ALTER} schema events from streaming
 *
 * @author Jiri Pechanec
 */
public class SchemaHistoryTopicIT extends AbstractConnectorTest {

    private static OracleConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        TestHelper.dropTable(connection, "debezium.tablea");
        TestHelper.dropTable(connection, "debezium.tableb");
        TestHelper.dropTable(connection, "debezium.tablec");
        connection.execute(
                "CREATE TABLE debezium.tablea (id numeric(9,0) not null, cola varchar2(30), primary key(id))",
                "CREATE TABLE debezium.tableb (id numeric(9,0) not null, colb varchar2(30), primary key(id))",
                "CREATE TABLE debezium.tablec (id numeric(9,0) not null, colc varchar2(30), primary key(id))");

        connection.execute("GRANT SELECT ON debezium.tablea to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.tablea ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        connection.execute("GRANT SELECT ON debezium.tableb to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.tableb ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        connection.execute("GRANT SELECT ON debezium.tablec to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.tablec ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            TestHelper.dropTable(connection, "debezium.tablea");
            TestHelper.dropTable(connection, "debezium.tableb");
            TestHelper.dropTable(connection, "debezium.tablec");
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-1904")
    public void snapshotSchemaChanges() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.TABLE[ABC]")
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute(
                    "INSERT INTO debezium.tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO debezium.tableb VALUES(" + id + ", 'b')");
        }

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        Testing.Print.enable();

        // DDL for 3 tables
        SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> schemaRecords = records.allRecordsInOrder();
        assertThat(schemaRecords).hasSize(3);
        schemaRecords.forEach(record -> {
            assertThat(record.topic()).isEqualTo("server1");
            assertThat(((Struct) record.key()).getString("databaseName")).isEqualTo(TestHelper.getDatabaseName());
            assertThat(record.sourceOffset().get("snapshot")).isEqualTo(true);
        });
        assertThat(((Struct) schemaRecords.get(0).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        assertThat(((Struct) schemaRecords.get(1).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        assertThat(((Struct) schemaRecords.get(2).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        assertThat(((Struct) schemaRecords.get(0).value()).getStruct("source").getString("schema")).isEqualTo("DEBEZIUM");
        assertThat(((Struct) schemaRecords.get(0).value()).getString("ddl")).contains("CREATE TABLE");
        assertThat(((Struct) schemaRecords.get(0).value()).getString("schemaName")).isEqualTo("DEBEZIUM");

        final List<Struct> tableChanges = ((Struct) schemaRecords.get(0).value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertThat(tableChanges.get(0).get("type")).isEqualTo("CREATE");

        records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        assertThat(records.recordsForTopic("server1.DEBEZIUM.TABLEA")).hasSize(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("server1.DEBEZIUM.TABLEB")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("server1.DEBEZIUM.TABLEB").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name("server1.DEBEZIUM.TABLEB.Value")
                            .field("ID", Schema.INT32_SCHEMA)
                            .field("COLB", Schema.OPTIONAL_STRING_SCHEMA)
                            .build());
        });
    }
}

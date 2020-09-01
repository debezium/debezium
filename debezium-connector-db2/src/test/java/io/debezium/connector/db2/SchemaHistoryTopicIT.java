/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.Db2ConnectorConfig.SnapshotMode;
import io.debezium.connector.db2.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration test for the user-facing history topic of the Debezium Db2 Server connector.
 * <p>
 * The tests should verify the {@code CREATE} schema events from snapshot and the {@code CREATE} and
 * the {@code ALTER} schema events from streaming
 *
 * @author Jiri Pechanec
 */
public class SchemaHistoryTopicIT extends AbstractConnectorTest {

    private Db2Connection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
        connection.execute(
                "CREATE TABLE tablea (id int not null, cola varchar(30), primary key(id))",
                "CREATE TABLE tableb (id int not null, colb varchar(30), primary key(id))",
                "CREATE TABLE tablec (id int not null, colc varchar(30), primary key(id))");
        TestHelper.enableTableCdc(connection, "TABLEA");
        TestHelper.enableTableCdc(connection, "TABLEB");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            TestHelper.disableDbCdc(connection);
            TestHelper.disableTableCdc(connection, "TABLEB");
            TestHelper.disableTableCdc(connection, "TABLEA");
            connection.execute("DROP TABLE tablea", "DROP TABLE tableb", "DROP TABLE tablec");
            connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_COLVERSION");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_TABVERSION");
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
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(Db2ConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        start(Db2Connector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        Testing.Print.enable();

        // DDL for 3 tables
        SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> schemaRecords = records.allRecordsInOrder();
        Assertions.assertThat(schemaRecords).hasSize(3);
        schemaRecords.forEach(record -> {
            Assertions.assertThat(record.topic()).isEqualTo("testdb");
            Assertions.assertThat(((Struct) record.key()).getString("databaseName")).isEqualTo("TESTDB");
            Assertions.assertThat(record.sourceOffset().get("snapshot")).isEqualTo(true);
        });
        Assertions.assertThat(((Struct) schemaRecords.get(0).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        Assertions.assertThat(((Struct) schemaRecords.get(1).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        Assertions.assertThat(((Struct) schemaRecords.get(2).value()).getStruct("source").getString("snapshot")).isEqualTo("true");

        final List<Struct> tableChanges = ((Struct) schemaRecords.get(0).value()).getArray("tableChanges");
        Assertions.assertThat(tableChanges).hasSize(1);
        Assertions.assertThat(tableChanges.get(0).get("type")).isEqualTo("CREATE");

        records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        Assertions.assertThat(records.recordsForTopic("testdb.DB2INST1.TABLEA")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("testdb.DB2INST1.TABLEB")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("testdb.DB2INST1.TABLEB").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name("testdb.DB2INST1.TABLEB.Value")
                            .field("ID", Schema.INT32_SCHEMA)
                            .field("COLB", Schema.OPTIONAL_STRING_SCHEMA)
                            .build());
        });
    }
}

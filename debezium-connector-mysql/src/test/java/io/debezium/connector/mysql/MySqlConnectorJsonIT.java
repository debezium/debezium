/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Struct;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class MySqlConnectorJsonIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-json.txt").toAbsolutePath();

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        } finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-126")
    public void shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = Configuration.create()
                              .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname"))
                              .with(MySqlConnectorConfig.PORT, System.getProperty("database.port"))
                              .with(MySqlConnectorConfig.USER, "snapper")
                              .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                              .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED.name().toLowerCase())
                              .with(MySqlConnectorConfig.SERVER_ID, 18765)
                              .with(MySqlConnectorConfig.SERVER_NAME, "jsonit_binlog")
                              .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                              .with(MySqlConnectorConfig.DATABASE_WHITELIST, "json_test")
                              .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                              .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.toString())
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int numCreateDatabase = 1;
        int numCreateTables = 1;
        int numDataRecords = 1;
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic("jsonit_binlog").size()).isEqualTo(numCreateDatabase + numCreateTables);
        assertThat(records.recordsForTopic("jsonit_binlog.json_test.dbz_126_jsontable").size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(1 + numCreateTables);
        assertThat(records.databaseNames().size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase("json_test").size()).isEqualTo(numCreateDatabase + numCreateTables);
        assertThat(records.ddlRecordsForDatabase("regression_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        records.ddlRecordsForDatabase("json_test").forEach(this::print);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
        List<String> errors = new ArrayList<>();
        records.forEach(record -> {
            Struct value = (Struct) record.value();
            if (record.topic().endsWith("dbz_126_jsontable")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                Integer i = after.getInt32("id");
                assertThat(i).isNotNull();
                String json = after.getString("json");
                String expectedBinlog = after.getString("expectedBinlogStr");
                check(json,expectedBinlog,errors::add);
            }
        });
        if (!errors.isEmpty()) {
            fail("" + errors.size() + " errors with JSON records..." + System.lineSeparator() +
                 String.join(System.lineSeparator(), errors));
        }
    }

    @Test
    public void shouldConsumeAllEventsFromDatabaseUsingSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = Configuration.create()
                              .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname"))
                              .with(MySqlConnectorConfig.PORT, System.getProperty("database.port"))
                              .with(MySqlConnectorConfig.USER, "snapper")
                              .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                              .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED.name().toLowerCase())
                              .with(MySqlConnectorConfig.SERVER_ID, 18765)
                              .with(MySqlConnectorConfig.SERVER_NAME, "jsonit_snap")
                              .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                              .with(MySqlConnectorConfig.DATABASE_WHITELIST, "json_test")
                              .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        //Testing.Debug.enable();
        int numTables = 1;
        int numDataRecords = 1;
        int numDdlRecords = numTables * 2 + 3; // for each table (1 drop + 1 create) + for each db (1 create + 1 drop + 1 use)
        int numSetVariables = 1;
        SourceRecords records = consumeRecordsByTopic(numDdlRecords + numSetVariables + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic("jsonit_snap").size()).isEqualTo(numDdlRecords + numSetVariables);
        assertThat(records.recordsForTopic("jsonit_snap.json_test.dbz_126_jsontable").size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(numTables + 1);
        assertThat(records.databaseNames().size()).isEqualTo(2);
        assertThat(records.databaseNames()).containsOnly("json_test", "");
        assertThat(records.ddlRecordsForDatabase("json_test").size()).isEqualTo(numDdlRecords);
        assertThat(records.ddlRecordsForDatabase("regression_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("").size()).isEqualTo(1); // SET statement
        records.ddlRecordsForDatabase("json_test").forEach(this::print);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
        List<String> errors = new ArrayList<>();
        records.forEach(record -> {
            Struct value = (Struct) record.value();
            if (record.topic().endsWith("dbz_126_jsontable")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                Integer i = after.getInt32("id");
                assertThat(i).isNotNull();
                String json = after.getString("json");
                String expectedJdbc = after.getString("expectedJdbcStr");
                check(json,expectedJdbc,errors::add);
            }
        });
        if (!errors.isEmpty()) {
            fail("" + errors.size() + " errors with JSON records..." + System.lineSeparator() +
                 String.join(System.lineSeparator(), errors));
        }
    }
    
    protected void check(String json, String expectedBinlog, Consumer<String> msg ) {
        if ((json == null && expectedBinlog != null) || (json != null && !json.equals(expectedBinlog))) {
            msg.accept("JSON was:     " + json + System.lineSeparator() + "but expected: " + expectedBinlog);
        } else {
            assertThat(json).isEqualTo(expectedBinlog);
        }
    }

}

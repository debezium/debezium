/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.junit.SkipTestDependingOnDatabaseRule;
import io.debezium.connector.mysql.junit.SkipWhenDatabaseIs;
import io.debezium.connector.mysql.junit.SkipWhenDatabaseIs.Type;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
@SkipWhenDatabaseIs(value = Type.MYSQL, versions = @SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 7, reason = "JSON data type was not added until MySQL 5.7"))
@SkipWhenDatabaseIs(value = Type.MARIADB, reason = "MariaDB does not support JSON natively, its treated as long text as an alias")
public class MySqlConnectorJsonIT extends AbstractAsyncEngineConnectorTest {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-json.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("jsonit", "json_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @Rule
    public TestRule skipRule = new SkipTestDependingOnDatabaseRule();

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-126")
    public void shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
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
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(numCreateDatabase + numCreateTables);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_126_jsontable")).size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(1 + numCreateTables);
        assertThat(records.databaseNames().size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(numCreateDatabase + numCreateTables);
        assertThat(records.ddlRecordsForDatabase("regression_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).forEach(this::print);

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
                check(json, expectedBinlog, errors::add);
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
        config = DATABASE.defaultConfig().build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int numTables = 1;
        int numDataRecords = 1;
        int numDdlRecords = numTables * 2 + 3; // for each table (1 drop + 1 create) + for each db (1 create + 1 drop + 1 use)
        int numSetVariables = 1;
        SourceRecords records = consumeRecordsByTopic(numDdlRecords + numSetVariables + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(numDdlRecords + numSetVariables);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_126_jsontable")).size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(numTables + 1);
        assertThat(records.databaseNames().size()).isEqualTo(2);
        assertThat(records.databaseNames()).containsOnly(DATABASE.getDatabaseName(), "");
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(numDdlRecords);
        assertThat(records.ddlRecordsForDatabase("regression_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("").size()).isEqualTo(1); // SET statement
        records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).forEach(this::print);

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
                check(json, expectedJdbc, errors::add);
            }
        });
        if (!errors.isEmpty()) {
            fail("" + errors.size() + " errors with JSON records..." + System.lineSeparator() +
                    String.join(System.lineSeparator(), errors));
        }
    }

    @Test
    @FixFor("DBZ-4605")
    public void shouldProcessUpdate() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        Testing.Debug.enable();
        final int numCreateDatabase = 1;
        final int numCreateTables = 1;
        final int numDataRecords = 41;
        final SourceRecords recordsInitial = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numDataRecords);
        assertThat(recordsInitial).isNotNull();
        assertThat(recordsInitial.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(numCreateDatabase + numCreateTables);
        assertThat(recordsInitial.recordsForTopic(DATABASE.topicForTable("dbz_126_jsontable")).size()).isEqualTo(numDataRecords);

        try (MySqlTestConnection conn = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            conn.execute(
                    "CREATE TABLE IF NOT EXISTS deals ( id int(11) unsigned NOT NULL AUTO_INCREMENT, company_id int(11) unsigned DEFAULT NULL, title varchar(255) DEFAULT NULL, custom_fields json DEFAULT NULL, PRIMARY KEY (id), KEY idx_company_id (company_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8",
                    "INSERT INTO deals (title, custom_fields) VALUES ('test', '"
                            + "{"
                            + "\"17fc9889474028063990914001f6854f6b8b5784\":\"test_field_for_remove_fields_behaviour_2\","
                            + "\"1f3a2ea5bc1f60258df20521bee9ac636df69a3a\":{\"currency\":\"USD\"},"
                            + "\"4f4d99a438f334d7dbf83a1816015b361b848b3b\":{\"currency\":\"USD\"},"
                            + "\"9021162291be72f5a8025480f44bf44d5d81d07c\":\"test_field_for_remove_fields_behaviour_3_will_be_removed\","
                            + "\"9b0ed11532efea688fdf12b28f142b9eb08a80c5\":{\"currency\":\"USD\"},"
                            + "\"e65ad0762c259b05b4866f7249eabecabadbe577\":\"test_field_for_remove_fields_behaviour_1_updated\","
                            + "\"ff2c07edcaa3e987c23fb5cc4fe860bb52becf00\":{\"currency\":\"USD\"}"
                            + "}')",
                    "UPDATE deals SET custom_fields = JSON_REMOVE(custom_fields, '$.\"17fc9889474028063990914001f6854f6b8b5784\"')");
        }
        final SourceRecords records = consumeRecordsByTopic(3);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("deals")).size()).isEqualTo(2);
        final SourceRecord update = records.allRecordsInOrder().get(2);
        assertThat(((Struct) update.value()).getStruct("after").getString("custom_fields")).isEqualTo(
                "{"
                        + "\"1f3a2ea5bc1f60258df20521bee9ac636df69a3a\":{\"currency\":\"USD\"},"
                        + "\"4f4d99a438f334d7dbf83a1816015b361b848b3b\":{\"currency\":\"USD\"},"
                        + "\"9021162291be72f5a8025480f44bf44d5d81d07c\":\"test_field_for_remove_fields_behaviour_3_will_be_removed\","
                        + "\"9b0ed11532efea688fdf12b28f142b9eb08a80c5\":{\"currency\":\"USD\"},"
                        + "\"e65ad0762c259b05b4866f7249eabecabadbe577\":\"test_field_for_remove_fields_behaviour_1_updated\","
                        + "\"ff2c07edcaa3e987c23fb5cc4fe860bb52becf00\":{\"currency\":\"USD\"}"
                        + "}");
        stopConnector();
    }

    protected void check(String json, String expectedBinlog, Consumer<String> msg) {
        if ((json == null && expectedBinlog != null) || (json != null && !json.equals(expectedBinlog))) {
            msg.accept("JSON was:     " + json + System.lineSeparator() + "but expected: " + expectedBinlog);
        }
        else {
            assertThat(json).isEqualTo(expectedBinlog);
        }
    }

}

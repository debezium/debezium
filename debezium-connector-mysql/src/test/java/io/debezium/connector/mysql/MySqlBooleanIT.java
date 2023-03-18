/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Testing;

/**
 * Tests for the MySQL {@code BOOLEAN} data type synonym.
 *
 * @author Chris Cranford
 */
public class MySqlBooleanIT extends AbstractConnectorTest {

    private static final String TABLE_NAME = "BOOLEAN_TEST";
    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-boolean.text").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("booleanit", "boolean_test").withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

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
    @FixFor("DBZ-6225")
    public void testBooleanDataTypeMappedConsistentlyBetweenSnapshotAndStreamingAfterTheFact() throws Exception {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(TABLE_NAME) + "," + DATABASE.qualifiedTableName("BOOlEAN_TEST2"))
                .with(MySqlConnectorConfig.PROPAGATE_COLUMN_SOURCE_TYPE, ".*")
                .build();

        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(2 + 4 + 1);
        assertThat(records).isNotNull();

        List<SourceRecord> tableRecords = records.recordsForTopic(DATABASE.topicForTable(TABLE_NAME));
        assertThat(tableRecords).hasSize(1);

        SourceRecord record = tableRecords.get(0);
        System.out.println(record);

        Schema after = record.valueSchema().field("after").schema();

        // Assert how the BOOLEAN data type is mapped during the snapshot phase.
        assertThat(after.field("b1").schema().type()).isEqualTo(Schema.Type.INT16);
        assertThat(after.field("b1").schema().parameters().get("__debezium.source.column.type")).isEqualTo("TINYINT");
        assertThat(after.field("b1").schema().parameters().get("__debezium.source.column.length")).isEqualTo("1");
        assertThat(after.field("b2").schema().type()).isEqualTo(Schema.Type.INT16);
        assertThat(after.field("b2").schema().parameters().get("__debezium.source.column.type")).isEqualTo("TINYINT");
        assertThat(after.field("b2").schema().parameters().get("__debezium.source.column.length")).isEqualTo("1");

        // Create the table after-the-fact
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            try (JdbcConnection conn = db.connect()) {
                conn.execute("CREATE TABLE BOOLEAN_TEST2 (`id` INT NOT NULL AUTO_INCREMENT, " +
                        "`b1` boolean default true, `b2` boolean default false, " +
                        "primary key (`ID`)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;");
                conn.execute("INSERT INTO BOOLEAN_TEST2 (b1,b2) VALUES (true, false)");
            }
        }

        records = consumeRecordsByTopic(2);
        assertThat(records).isNotNull();

        tableRecords = records.recordsForTopic(DATABASE.topicForTable(TABLE_NAME + "2"));
        assertThat(tableRecords).hasSize(1);

        record = tableRecords.get(0);
        System.out.println(record);

        after = record.valueSchema().field("after").schema();

        // Assert the created table after-the-fact record is identical to the snapshot
        assertThat(after.field("b1").schema().type()).isEqualTo(Schema.Type.INT16);
        assertThat(after.field("b1").schema().parameters().get("__debezium.source.column.type")).isEqualTo("TINYINT");
        assertThat(after.field("b1").schema().parameters().get("__debezium.source.column.length")).isEqualTo("1");
        assertThat(after.field("b2").schema().type()).isEqualTo(Schema.Type.INT16);
        assertThat(after.field("b2").schema().parameters().get("__debezium.source.column.type")).isEqualTo("TINYINT");
        assertThat(after.field("b2").schema().parameters().get("__debezium.source.column.length")).isEqualTo("1");

        stopConnector();
    }
}

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

/**
 * Tests for the MySQL {@code NCHAR} and {@code NVARCHAR} data types.
 *
 * @author Chris Cranford
 */
public class MySqlNationalizedCharacterIT extends AbstractConnectorTest {

    private static final String TABLE_NAME = "NC_TEST";
    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-boolean.text").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("nctestit", "nationalized_character_test").withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-6225")
    public void testNationalizedCharacterDataTypeMappedConsistentlyBetweenSnapshotAndStreamingAfterTheFact() throws Exception {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(TABLE_NAME) + "," + DATABASE.qualifiedTableName("NC_TEST2"))
                .with(MySqlConnectorConfig.PROPAGATE_COLUMN_SOURCE_TYPE, ".*")
                .build();

        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(2 + 4 + 1);
        assertThat(records).isNotNull();

        List<SourceRecord> tableRecords = records.recordsForTopic(DATABASE.topicForTable(TABLE_NAME));
        assertThat(tableRecords).hasSize(1);

        SourceRecord record = tableRecords.get(0);

        Schema after = record.valueSchema().field("after").schema();

        // Assert how the BOOLEAN data type is mapped during the snapshot phase.
        assertThat(after.field("nc1").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(after.field("nc1").schema().parameters().get("__debezium.source.column.type")).isEqualTo("NCHAR");
        assertThat(after.field("nc1").schema().parameters().get("__debezium.source.column.length")).isEqualTo("1");
        assertThat(after.field("nc2").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(after.field("nc2").schema().parameters().get("__debezium.source.column.type")).isEqualTo("NCHAR");
        assertThat(after.field("nc2").schema().parameters().get("__debezium.source.column.length")).isEqualTo("5");
        assertThat(after.field("nc3").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(after.field("nc3").schema().parameters().get("__debezium.source.column.type")).isEqualTo("NVARCHAR");
        assertThat(after.field("nc3").schema().parameters().get("__debezium.source.column.length")).isEqualTo("25");

        // Create the table after-the-fact
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            try (JdbcConnection conn = db.connect()) {
                conn.execute("CREATE TABLE NC_TEST2 (`id` INT NOT NULL AUTO_INCREMENT, " +
                        "`nc1` nchar, `nc2` nchar(5), `nc3` nvarchar(25), " +
                        "primary key (`ID`)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;");
                conn.execute("INSERT INTO NC_TEST2 (nc1,nc2,nc3) VALUES ('b', '456', 'world')");
            }
        }

        records = consumeRecordsByTopic(2);
        assertThat(records).isNotNull();

        tableRecords = records.recordsForTopic(DATABASE.topicForTable(TABLE_NAME + "2"));
        assertThat(tableRecords).hasSize(1);

        record = tableRecords.get(0);

        after = record.valueSchema().field("after").schema();
        System.out.println(after.field("nc1").schema().parameters());
        System.out.println(after.field("nc2").schema().parameters());
        System.out.println(after.field("nc3").schema().parameters());

        // Assert the created table after-the-fact record is identical to the snapshot
        assertThat(after.field("nc1").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(after.field("nc1").schema().parameters().get("__debezium.source.column.type")).isEqualTo("NCHAR");
        assertThat(after.field("nc1").schema().parameters().get("__debezium.source.column.length")).isEqualTo("1");
        assertThat(after.field("nc2").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(after.field("nc2").schema().parameters().get("__debezium.source.column.type")).isEqualTo("NCHAR");
        assertThat(after.field("nc2").schema().parameters().get("__debezium.source.column.length")).isEqualTo("5");
        assertThat(after.field("nc3").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(after.field("nc3").schema().parameters().get("__debezium.source.column.type")).isEqualTo("NVARCHAR");
        assertThat(after.field("nc3").schema().parameters().get("__debezium.source.column.length")).isEqualTo("25");

        stopConnector();
    }
}

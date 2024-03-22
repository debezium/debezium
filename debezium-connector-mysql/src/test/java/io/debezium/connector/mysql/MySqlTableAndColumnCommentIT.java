/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

public class MySqlTableAndColumnCommentIT extends AbstractAsyncEngineConnectorTest {

    private static final String COLUMN_COMMENT_PARAMETER_KEY = "__debezium.source.column.comment";

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-comment.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("commentit", "table_column_comment_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

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
    @FixFor({ "DBZ-4000", "DBZ-4998" })
    public void shouldParseComment() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_COMMENTS, true)
                .with("datatype.propagate.source.type", ".+\\.BIGINT,.+\\.VARCHAR")
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // Testing.Debug.enable();
        final int numDatabase = 3;
        final int numTables = 2;
        final int numInserts = 1;
        final int numOthers = 1; // SET

        SourceRecords records = consumeRecordsByTopic(numDatabase + numTables + numInserts + numOthers);

        assertThat(records).isNotNull();
        records.forEach(this::validate);

        List<SourceRecord> ddls = records.ddlRecordsForDatabase(DATABASE.getDatabaseName());
        assertThat(ddls).hasSize(5);
        SourceRecord createTable = ddls.get(4);
        List<Struct> tableChanges = (List<Struct>) ((Struct) createTable.value()).get("tableChanges");
        Struct table = (Struct) tableChanges.get(0).get("table");
        List<Struct> columns = (List<Struct>) table.get("columns");

        // Check field ts_ms from snapshot phase
        assertThat(((Struct) createTable.value()).get("ts_ms")).isNotNull();

        // Table comment
        assertThat(table.get("comment")).isEqualTo("table for dbz-4000");

        // Column comments
        Optional<Struct> idOpt = getColumnOpt(columns, "id");
        assertThat(idOpt.get().get("comment")).isEqualTo("pk");

        Optional<Struct> nameOpt = getColumnOpt(columns, "name");
        assertThat(nameOpt.get().get("comment")).isEqualTo("this is name column");

        Optional<Struct> valueOpt = getColumnOpt(columns, "value");
        assertThat(valueOpt.get().get("comment")).isEqualTo("the value is bigint type");

        List<SourceRecord> dmls = records.recordsForTopic(DATABASE.topicForTable("dbz_4000_comment_test"));
        assertThat(dmls).hasSize(1);

        SourceRecord insert = dmls.get(0);
        Field before = insert.valueSchema().field("before");

        // no type info requested as per given regexps
        Map<String, String> idSchemaParameters = before
                .schema()
                .field("id")
                .schema()
                .parameters();
        assertThat(idSchemaParameters).isNull();

        // Varchar info
        Map<String, String> nameSchemaParameters = before
                .schema()
                .field("name")
                .schema()
                .parameters();
        assertThat(nameSchemaParameters).contains(entry(COLUMN_COMMENT_PARAMETER_KEY, "this is name column"));

        // Bigint info
        Map<String, String> valueSchemaParameters = before
                .schema()
                .field("value")
                .schema()
                .parameters();
        assertThat(valueSchemaParameters).contains(entry(COLUMN_COMMENT_PARAMETER_KEY, "the value is bigint type"));

        // Add a column with comment
        try (Connection conn = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName()).connection()) {
            conn.createStatement().execute("ALTER TABLE dbz_4000_comment_test ADD COLUMN remark TEXT COMMENT 'description'");
        }
        records = consumeRecordsByTopic(1);

        assertThat(records).isNotNull();
        records.forEach(this::validate);

        ddls = records.ddlRecordsForDatabase(DATABASE.getDatabaseName());
        SourceRecord ddl = ddls.get(0);
        table = (Struct) ((List<Struct>) ((Struct) ddl.value()).get("tableChanges")).get(0).get("table");
        columns = (List<Struct>) table.get("columns");

        Optional<Struct> remarkOpt = getColumnOpt(columns, "remark");
        assertThat(remarkOpt.get().get("comment")).isEqualTo("description");

        // Check field ts_ms from streaming phase
        assertThat(((Struct) ddl.value()).get("ts_ms")).isNotNull();

        // Stop the connector
        stopConnector();
    }

    private Optional<Struct> getColumnOpt(List<Struct> columns, String columnName) {
        return columns.stream().filter(f -> f.get("name").toString().equalsIgnoreCase(columnName)).findFirst();
    }
}

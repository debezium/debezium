/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogJsonIT;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;

/**
 * @author Randall Hauch
 */
public class MySqlConnectorJsonIT extends BinlogJsonIT<MySqlConnector> implements MySqlCommon {

    /**
     * The number of rows in the {@code json_test} fixture.
     */
    private static final int ROW_COUNT = 41;

    /**
     * Matches doubles the server renders in scientific notation, e.g. {@code 1.8446744073709552e19}.
     */
    private static final Pattern SCIENTIFIC_NOTATION = Pattern.compile("-?[0-9.]+e-?[0-9]+");

    private static final Path FORMATTING_SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-json-formatting.txt").toAbsolutePath();

    private final UniqueDatabase formattingDatabase = TestHelper.getUniqueDatabase("jsonfmtit", "json_test")
            .withDbHistoryPath(FORMATTING_SCHEMA_HISTORY_PATH);

    @BeforeEach
    void beforeEachFormattingTest() {
        Files.delete(FORMATTING_SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void afterEachFormattingTest() {
        Files.delete(FORMATTING_SCHEMA_HISTORY_PATH);
    }

    @Test
    @FixFor("debezium/dbz#2376")
    public void shouldMatchJdbcFormatWhenStreamingWithDatabaseJsonStringFormattingMode() throws SQLException, InterruptedException {
        formattingDatabase.create();

        final Configuration config = formattingDatabase.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with(MySqlConnectorConfig.JSON_STRING_FORMATTING_MODE, MySqlConnectorConfig.JsonStringFormattingMode.DATABASE)
                .build();

        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), formattingDatabase.getServerName(), getStreamingNamespace());
        formattingDatabase.initialize();

        final int numDdlRecords = 2; // create database + create table
        final SourceRecords records = consumeRecordsByTopic(numDdlRecords + ROW_COUNT);
        stopConnector();

        final List<SourceRecord> tableRecords = records.recordsForTopic(formattingDatabase.topicForTable("dbz_126_jsontable"));
        assertThat(tableRecords).hasSize(ROW_COUNT);
        records.forEach(this::validate);

        // Streamed values are expected to equal what the server returns over JDBC, which is what a snapshot
        // would emit. The values are read back live rather than taken from the expectedJdbcStr column, as
        // some fixture rows depend on the server environment (UNIX_TIMESTAMP varies with the time zone).
        final Map<Integer, String> jdbcValues = new HashMap<>();
        try (BinlogTestConnection conn = getTestDatabaseConnection(formattingDatabase.getDatabaseName())) {
            conn.query("SELECT id, json FROM dbz_126_jsontable", rs -> {
                while (rs.next()) {
                    jdbcValues.put(rs.getInt(1), rs.getString(2));
                }
            });
        }
        assertThat(jdbcValues).hasSize(ROW_COUNT);

        final List<String> errors = new ArrayList<>();
        final List<Integer> skipped = new ArrayList<>();
        for (SourceRecord record : tableRecords) {
            final Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            final int id = after.getInt32("id");
            final String expected = jdbcValues.get(id);
            if (expected != null && SCIENTIFIC_NOTATION.matcher(expected).matches()) {
                // The binlog client renders doubles of large or small magnitude differently from the server
                // whatever the formatting mode
                skipped.add(id);
                continue;
            }
            check(after.getString("json"), expected, errors::add);
        }

        if (!errors.isEmpty()) {
            fail(errors.size() + " errors with JSON records..." + System.lineSeparator() +
                    String.join(System.lineSeparator(), errors));
        }
        // The fixture holds exactly one such value; asserting the count keeps the skip from hiding a regression
        assertThat(skipped).hasSize(1);
    }
}

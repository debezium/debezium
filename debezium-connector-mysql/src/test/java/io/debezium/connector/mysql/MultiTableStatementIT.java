/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * @author Jiri Pechanec
 */
public class MultiTableStatementIT extends AbstractConnectorTest {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-json.txt")
            .toAbsolutePath();
    private UniqueDatabase DATABASE;

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE = new UniqueDatabase("multitable", "multitable_dbz_871")
                .withDbHistoryPath(SCHEMA_HISTORY_PATH);
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
    public void shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshot() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // Testing.Print.enable();
        // CREATE DB + 4 * CREATE TABLE + DROP TABLE
        SourceRecords records = consumeRecordsByTopic(1 + 4 + 1);
        final List<String> tableNames = new ArrayList<>();
        records.forEach(record -> {
            final Struct source = ((Struct) record.value()).getStruct("source");
            assertThat(source.getString("db")).isEqualTo(DATABASE.getDatabaseName());
            tableNames.add(source.getString("table"));
        });
        assertThat(tableNames.subList(0, 5)).containsExactly(
                null,
                "t1",
                "t2",
                "t3",
                "t4");
        String[] dropTableNames = tableNames.get(5).split(",");
        assertThat(dropTableNames).containsOnly("t1", "t2", "t3", "t4");

        stopConnector();
    }
}

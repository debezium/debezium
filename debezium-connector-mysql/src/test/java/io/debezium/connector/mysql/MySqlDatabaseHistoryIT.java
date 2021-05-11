/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.nio.file.Path;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
*
* The test to verify whether DDL is stored correctly in database history.
*
* @author Jiri Pechanec
*/
public class MySqlDatabaseHistoryIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-json.txt")
            .toAbsolutePath();

    private static final int TABLE_COUNT = 1;

    private UniqueDatabase DATABASE;

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE = new UniqueDatabase("history", "history-dbz")
                .withDbHistoryPath(DB_HISTORY_PATH);
        DATABASE.createAndInitialize();

        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-3485")
    public void shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshot() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        Testing.Print.enable();
        // SET + USE + Drop DB + create DB + CREATE/DROP for each table
        SourceRecords records = consumeRecordsByTopic(1 + 1 + 1 + 1 + TABLE_COUNT * 2);

        stopConnector();

        start(MySqlConnector.class, config);
        assertConnectorIsRunning();
        stopConnector();
    }
}

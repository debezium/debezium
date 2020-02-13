/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Verify correct range of TINYINT.
 *
 * @author Jiri Pechanec
 */
public class MySqlTinyIntIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-year.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("tinyintit", "tinyint_test")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
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
    @FixFor("DBZ-1773")
    public void shouldHandleTinyIntAsNumber() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        final int numDatabase = 2;
        final int numTables = 2;
        final int numOthers = 2;
        consumeRecords(numDatabase + numTables + numOthers);

        assertChangeRecord();

        try (final Connection conn = MySQLConnection.forTestDatabase(DATABASE.getDatabaseName()).connection()) {
            conn.createStatement().execute("INSERT INTO DBZ1773 VALUES (DEFAULT, 100, 5, 50)");
        }
        assertChangeRecord();

        stopConnector();
    }

    private void assertChangeRecord() throws InterruptedException {
        final SourceRecord record = consumeRecord();
        Assertions.assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        Assertions.assertThat(change.getInt16("ti")).isEqualTo((short) 100);
        Assertions.assertThat(change.getInt16("ti1")).isEqualTo((short) 5);
        Assertions.assertThat(change.getInt16("ti2")).isEqualTo((byte) 50);
    }
}

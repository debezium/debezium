/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Testing;

public class CustomSnapshotterIT extends AbstractConnectorTest {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-connect.txt").toAbsolutePath();

    private final UniqueDatabase DATABASE_CUSTOM_SNAPSHOT = new UniqueDatabase("myServer1", "custom_snapshot")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE_CUSTOM_SNAPSHOT.createAndInitialize();
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
    public void shouldAllowForCustomSnapshot() throws InterruptedException, SQLException {

        final String pkField = "pk";

        Configuration config = DATABASE_CUSTOM_SNAPSHOT.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.CUSTOM.getValue())
                .with(MySqlConnectorConfig.SNAPSHOT_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
                .with(CommonConnectorConfig.SNAPSHOT_QUERY_MODE, CommonConnectorConfig.SnapshotQueryMode.CUSTOM)
                .with(CommonConnectorConfig.SNAPSHOT_QUERY_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
                .build();

        start(MySqlConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(10);

        List<SourceRecord> s1recs = actualRecords.recordsForTopic(DATABASE_CUSTOM_SNAPSHOT.topicForTable("a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(DATABASE_CUSTOM_SNAPSHOT.topicForTable("b"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs).isNull();

        SourceRecord record = s1recs.get(0);
        VerifyRecord.isValidRead(record, pkField, 1);

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE_CUSTOM_SNAPSHOT.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {

                connection.execute("INSERT INTO a (aa) VALUES (1);");
                connection.execute("INSERT INTO b (aa) VALUES (1);");
            }
        }
        actualRecords = consumeRecordsByTopic(2);

        s1recs = actualRecords.recordsForTopic(DATABASE_CUSTOM_SNAPSHOT.topicForTable("a"));
        s2recs = actualRecords.recordsForTopic(DATABASE_CUSTOM_SNAPSHOT.topicForTable("b"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        record = s1recs.get(0);
        VerifyRecord.isValidInsert(record, pkField, 2);
        record = s2recs.get(0);
        VerifyRecord.isValidInsert(record, pkField, 2);
        stopConnector();

        // TODO Maybe it can be enabled when DBZ-7308 is done.
        /*
         * config = DATABASE_CUSTOM_SNAPSHOT.defaultConfig()
         * .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.CUSTOM.getValue())
         * .with(MySqlConnectorConfig.SNAPSHOT_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
         * .with(MySqlConnectorConfig.SNAPSHOT_QUERY_MODE, MySqlConnectorConfig.SnapshotQueryMode.CUSTOM)
         * .with(MySqlConnectorConfig.SNAPSHOT_QUERY_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
         * .build();
         *
         * start(MySqlConnector.class, config);
         * assertConnectorIsRunning();
         * actualRecords = consumeRecordsByTopic(4);
         *
         * s1recs = actualRecords.recordsForTopic(DATABASE_CUSTOM_SNAPSHOT.topicForTable("a"));
         * s2recs = actualRecords.recordsForTopic(DATABASE_CUSTOM_SNAPSHOT.topicForTable("b"));
         * assertThat(s1recs.size()).isEqualTo(2);
         * assertThat(s2recs.size()).isEqualTo(2);
         * VerifyRecord.isValidRead(s1recs.get(0), pkField, 1);
         * VerifyRecord.isValidRead(s1recs.get(1), pkField, 2);
         * VerifyRecord.isValidRead(s2recs.get(0), pkField, 1);
         * VerifyRecord.isValidRead(s2recs.get(1), pkField, 2);
         */
    }
}

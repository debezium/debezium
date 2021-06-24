/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Tests around {@code NUMERIC} columns. Keep in sync with {@link MySqlDecimalColumnIT}.
 *
 * @author Gunnar Morling
 */
public class TopicNameSanitizationIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-topic-name-sanitization.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("topic-name-sanitization-it", "topic_name_sanitization_test")
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
    @FixFor("DBZ-878")
    public void shouldReplaceInvalidTopicNameCharacters() throws SQLException, InterruptedException {
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
        int numCreateTables = 2;
        int numInserts = 2;
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numInserts);
        stopConnector();
        assertThat(records).isNotNull();
        records.forEach(this::validate);

        List<SourceRecord> dmls = records.recordsForTopic(DATABASE.topicForTable("dbz_878_some_test_data"));
        assertThat(dmls).hasSize(1);

        SourceRecord insert = dmls.get(0);
        assertThat(insert.valueSchema().name()).endsWith("dbz_878_some_test_data.Envelope");
        VerifyRecord.isValidInsert(insert, "id", 1);

        String sourceTable = ((Struct) insert.value()).getStruct("source").getString("table");
        assertThat(sourceTable).isEqualTo("dbz_878_some|test@data");
    }

    @Test
    @FixFor("DBZ-1834")
    public void shouldAcceptDotInTableName() throws SQLException, InterruptedException {
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
        int numCreateTables = 2;
        int numInserts = 2;
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numInserts);
        stopConnector();
        assertThat(records).isNotNull();
        records.forEach(this::validate);

        List<SourceRecord> dmls = records.recordsForTopic(DATABASE.topicForTable("DBZ.1834"));
        assertThat(dmls).hasSize(1);

        SourceRecord insert = dmls.get(0);
        assertThat(insert.valueSchema().name()).endsWith("DBZ.1834.Envelope");
        VerifyRecord.isValidInsert(insert, "id", 1);

        String sourceTable = ((Struct) insert.value()).getStruct("source").getString("table");
        assertThat(sourceTable).isEqualTo("DBZ.1834");
    }
}

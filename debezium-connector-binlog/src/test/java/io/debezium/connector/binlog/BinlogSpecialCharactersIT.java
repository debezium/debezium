/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;

public abstract class BinlogSpecialCharactersIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final String TABLE_NAME = "special\"[`]$'";
    private static final String TOPIC_PREFIX = "special_characters";

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-special-characters.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("special_\"[`]$'_db", "special_characters_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

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
    public void testSnapshotSpecialCharactersTable() throws Exception {
        Configuration config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL_ONLY)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(TABLE_NAME))
                .with(BinlogConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .build();

        start(getConnectorClass(), config);
        SourceRecords records = consumeRecordsByTopic(1);
        stopConnector();

        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic(TOPIC_PREFIX).size()).isEqualTo(1);
    }
}

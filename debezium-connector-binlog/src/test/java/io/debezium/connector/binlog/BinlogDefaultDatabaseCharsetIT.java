/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.jdbc.JdbcConnection;

/**
 * @author Chris Cranford
 */
public abstract class BinlogDefaultDatabaseCharsetIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-connect.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("myServer1", "db_default_charset", "utf8mb4")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

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
    public void databaseWithDefaultCharsetTest() throws InterruptedException, SQLException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("DATA") + "," + DATABASE.qualifiedTableName("DATASTREAM"))
                .build();
        start(getConnectorClass(), config);

        Print.enable();

        SourceRecords records = consumeRecordsByTopic(7);
        final SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("DATA")).get(0);

        assertThat(((Struct) record.value()).getStruct("after").getString("MESSAGE")).isEqualTo("Žluťoučký");

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("CREATE TABLE DATASTREAM (MESSAGE TEXT);");
                connection.execute("INSERT INTO DATASTREAM VALUES ('Žluťoučký');");
            }
        }

        records = consumeRecordsByTopic(2);
        final SourceRecord recordStream = records.recordsForTopic(DATABASE.topicForTable("DATASTREAM")).get(0);

        assertThat(((Struct) recordStream.value()).getStruct("after").getString("MESSAGE")).isEqualTo("Žluťoučký");

    }
}

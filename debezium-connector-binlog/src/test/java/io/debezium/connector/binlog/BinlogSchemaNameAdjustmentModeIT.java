/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig.SchemaNameAdjustmentMode;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;

/**
 * @author Chris Cranford
 */
public abstract class BinlogSchemaNameAdjustmentModeIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-json.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("adjustment1", "schema_name_adjustment")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    @Before
    public void beforeEach() throws SQLException {
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

        dropAllDatabases();
    }

    @Test
    public void shouldAdjustNamesForAvro() throws InterruptedException {
        Struct data = consume(SchemaNameAdjustmentMode.AVRO);

        assertThat(data.schema().name()).contains("name_adjustment");
    }

    @Test
    public void shouldNotAdjustNames() throws InterruptedException {
        skipAvroValidation();
        Struct data = consume(SchemaNameAdjustmentMode.NONE);

        assertThat(data.schema().name()).contains("name-adjustment");
    }

    private Struct consume(SchemaNameAdjustmentMode adjustmentMode) throws InterruptedException {
        final Configuration config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("name-adjustment"))
                .with(BinlogConnectorConfig.SCHEMA_NAME_ADJUSTMENT_MODE, adjustmentMode)
                .build();

        start(getConnectorClass(), config);

        SourceRecords records = consumeRecordsByTopic(6 + 1); // 6 DDL changes, 1 INSERT
        final List<SourceRecord> results = records.recordsForTopic(DATABASE.topicForTable("name-adjustment"));
        assertThat(results).hasSize(1);

        return (Struct) results.get(0).value();
    }
}

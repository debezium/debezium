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

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig.SchemaNameAdjustmentMode;
import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

public class MySqlSchemaNameAdjustmentModeIT extends AbstractConnectorTest {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-json.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("adjustment1", "schema_name_adjustment")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    @Before
    public void beforeEach() throws SQLException {
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
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("name-adjustment"))
                .with(MySqlConnectorConfig.SCHEMA_NAME_ADJUSTMENT_MODE, adjustmentMode)
                .build();

        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(6 + 1); // 6 DDL changes, 1 INSERT
        final List<SourceRecord> results = records.recordsForTopic(DATABASE.topicForTable("name-adjustment"));
        assertThat(results).hasSize(1);

        return (Struct) results.get(0).value();
    }
}

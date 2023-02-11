/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig.SchemaNameAdjustmentMode;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.embedded.AbstractConnectorTest;

public class SqlServerSchemaNameAdjustmentModeIT extends AbstractConnectorTest {

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();

        connection.execute(
                "CREATE TABLE [name-adjustment] (id INT)",
                "INSERT INTO [name-adjustment] (id) VALUES (1);");
        TestHelper.enableTableCdc(connection, "name-adjustment");

        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        stopConnector();

        if (connection != null) {
            connection.close();
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
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SqlServerConnectorConfig.SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo\\.name-adjustment")
                .with(SqlServerConnectorConfig.SCHEMA_NAME_ADJUSTMENT_MODE, adjustmentMode)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> results = records.recordsForTopic("server1.testDB1.dbo.name-adjustment");
        assertThat(results).hasSize(1);

        return (Struct) results.get(0).value();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration test to verify that connector survives contact with non-standard tables like temporal tables
 *
 * @author Jiri Pechanec
 */
public class SpecialTableTypesIT extends AbstractAsyncEngineConnectorTest {

    private SqlServerConnection connection;

    @AfterEach
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("dbz#25")
    public void shouldIgnoreTemporalTable() throws Exception {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.temporal_table")
                .build();

        connection.execute(
                "CREATE TABLE normal_table (id int primary key, cola varchar(30))",
                "INSERT INTO normal_table (id, cola) VALUES (1, 'value1')",
                """
                            CREATE TABLE temporal_table
                            (
                                id int primary key,
                                cola varchar(30),
                                SysStartTime datetime2(7) GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,
                                SysEndTime   datetime2(7) GENERATED ALWAYS AS ROW END HIDDEN NOT NULL,
                                PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime)
                            )
                            WITH
                            (
                                SYSTEM_VERSIONING = ON
                                (
                                    HISTORY_TABLE = dbo.temporal_table_history,
                                    DATA_CONSISTENCY_CHECK = ON
                                )
                            )
                        """,
                // Triggers creation of index with which behaves as tableIndexStatistic
                """
                            CREATE NONCLUSTERED INDEX [IX_temporal_table_history_test] ON [temporal_table_history]
                            (
                                [SysEndTime] ASC
                            )
                            INCLUDE([id], [cola])
                            WITH (
                                PAD_INDEX = OFF,
                                STATISTICS_NORECOMPUTE = OFF,
                                SORT_IN_TEMPDB = OFF,
                                DROP_EXISTING = OFF,
                                ONLINE = OFF,
                                ALLOW_ROW_LOCKS = ON,
                                ALLOW_PAGE_LOCKS = ON,
                                OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
                            ) ON [PRIMARY]
                        """,
                "INSERT INTO temporal_table (id, cola) VALUES (1, 'value2')");

        TestHelper.enableTableCdc(connection, "normal_table");
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final var actualRecords = consumeRecordsByTopic(1, false);
        assertEquals(1, actualRecords.recordsForTopic("server1.testDB1.dbo.temporal_table").size());
    }
}

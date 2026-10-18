/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

/**
 * Unit tests for {@link SqlServerConnection}.
 *
 * @author Chris Cranford
 */
public class SqlServerConnectionTest {

    @Test
    @FixFor("dbz#1327")
    void shouldOrderFunctionModeChangesByCommandId() throws Exception {
        try (SqlServerConnection connection = testConnection(SqlServerConnectorConfig.DataQueryMode.FUNCTION)) {
            SqlServerChangeTable changeTable = new SqlServerChangeTable(
                    new TableId("testDB", "dbo", "orders"), "dbo_orders", 1, Lsn.NULL, List.of("id", "name"));

            String query = connection.buildGetAllChangesForTableQuery(changeTable, 0);

            assertThat(query).contains("cdc.[fn_cdc_get_all_changes_dbo_orders](?, ?, N'all update old') AS cdc_data");
            assertThat(query).contains("LEFT JOIN [testDB].cdc.lsn_time_mapping ltm WITH (NOLOCK) ON ltm.start_lsn = cdc_data.[__$start_lsn]");
            assertThat(query).contains("LEFT JOIN [testDB].cdc.[dbo_orders_CT] AS ct ON "
                    + "ct.[__$start_lsn] = cdc_data.[__$start_lsn] AND ct.[__$seqval] = cdc_data.[__$seqval] "
                    + "AND ct.[__$operation] = cdc_data.[__$operation] AND ct.[__$start_lsn] >= ? AND ct.[__$start_lsn] <= ?");
            assertThat(query).contains("ct.[__$command_id]");
            assertThat(query).doesNotContain("fn_cdc_map_lsn_to_time");
            assertThat(query).endsWith("ORDER BY cdc_data.[__$start_lsn] ASC, ct.[__$command_id] ASC, cdc_data.[__$seqval] ASC, cdc_data.[__$operation] ASC");
            assertThat(query).startsWith("SELECT cdc_data.[__$start_lsn], cdc_data.[__$seqval], cdc_data.[__$operation], "
                    + "cdc_data.[__$update_mask], ct.[__$command_id], [cdc_data].[id], [cdc_data].[name], ");
        }
    }

    @Test
    @FixFor("debezium/dbz#2670")
    void shouldCountRowsWithCountBig() throws Exception {
        // COUNT is a 32-bit int in T-SQL and overflows on tables with more than Integer.MAX_VALUE rows,
        // so the chunked snapshot has to count with COUNT_BIG instead.
        try (SqlServerConnection connection = testConnection()) {
            assertEquals("SELECT COUNT_BIG(1) FROM [testDB].[dbo].[orders]",
                    connection.buildSelectRowCount(new TableId("testDB", "dbo", "orders")));
        }
    }

    @Test
    @FixFor("debezium/dbz#2693")
    void functionModeQueryShouldJoinLsnTimeMappingInsteadOfCallingScalarFunctionPerRow() throws Exception {
        try (SqlServerConnection connection = testConnection()) {
            String query = connection.buildGetAllChangesForTableQuery(SqlServerConnectorConfig.DataQueryMode.FUNCTION, Collections.emptySet());

            assertFalse(query.contains("fn_cdc_map_lsn_to_time"), "FUNCTION mode must no longer call the per-row scalar function");
            assertTrue(query.contains("FROM #db.cdc.#function(?, ?, N'all update old') AS cdc_data"),
                    "the table-valued function's result set must be aliased as cdc_data");
            assertTrue(query.contains("LEFT JOIN #db.cdc.lsn_time_mapping ltm WITH (NOLOCK) ON ltm.start_lsn = cdc_data.[__$start_lsn]"),
                    "must join lsn_time_mapping the same way DIRECT mode does");
            assertTrue(query.contains("TODATETIMEOFFSET(ltm.tran_end_time, DATEPART(TZOFFSET, SYSDATETIMEOFFSET()))"),
                    "commit timestamp must come from the join, not the scalar function");
        }
    }

    @Test
    @FixFor("debezium/dbz#2693")
    void functionModeQueryShouldQualifyMetadataColumnsWithCdcDataAlias() throws Exception {
        try (SqlServerConnection connection = testConnection()) {
            String query = connection.buildGetAllChangesForTableQuery(SqlServerConnectorConfig.DataQueryMode.FUNCTION, Collections.emptySet());

            // Now that the function's result set is joined against lsn_time_mapping, its columns must be
            // qualified with the cdc_data alias to avoid the "Ambiguous column name" failures fixed for
            // DIRECT mode by debezium/dbz#2511 (debezium/debezium#7907) - the same risk now applies here.
            assertTrue(query.contains("SELECT cdc_data.[__$start_lsn], cdc_data.[__$seqval], cdc_data.[__$operation], cdc_data.[__$update_mask], ct.[__$command_id]"));
            assertTrue(query.contains("WHERE (([cdc_data].[__$start_lsn] = ? AND ct.[__$command_id] = ?"));
            assertTrue(query.endsWith("ORDER BY cdc_data.[__$start_lsn] ASC, ct.[__$command_id] ASC, cdc_data.[__$seqval] ASC, cdc_data.[__$operation] ASC"));
        }
    }

    @Test
    @FixFor("debezium/dbz#2693")
    void functionModeQueryShouldFilterSkippedOperationsWithCdcDataAlias() throws Exception {
        try (SqlServerConnection connection = testConnection()) {
            String query = connection.buildGetAllChangesForTableQuery(SqlServerConnectorConfig.DataQueryMode.FUNCTION, Collections.singleton(Envelope.Operation.DELETE));

            assertTrue(query.contains("[cdc_data].[__$operation] NOT IN (1)"));
        }
    }

    @Test
    @FixFor("debezium/dbz#2693")
    void directModeQueryShouldRemainUnchanged() throws Exception {
        try (SqlServerConnection connection = testConnection()) {
            String query = connection.buildGetAllChangesForTableQuery(SqlServerConnectorConfig.DataQueryMode.DIRECT, Collections.emptySet());

            assertTrue(query.contains("WITH (NOLOCK) LEFT JOIN #db.cdc.lsn_time_mapping"), "DIRECT mode must not be affected by the FUNCTION-mode rewrite");
            assertTrue(query.contains("[cdc_data].[__$command_id]"), "DIRECT mode must not be affected by the FUNCTION-mode rewrite");
        }
    }

    private SqlServerConnection testConnection() {
        return testConnection(SqlServerConnectorConfig.DataQueryMode.DIRECT);
    }

    private SqlServerConnection testConnection(SqlServerConnectorConfig.DataQueryMode dataQueryMode) {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "server")
                .with(SqlServerConnectorConfig.HOSTNAME, "localhost")
                .with(SqlServerConnectorConfig.USER, "debezium")
                .with(SqlServerConnectorConfig.DATABASE_NAMES, "testDB")
                .with(SqlServerConnectorConfig.DATA_QUERY_MODE, dataQueryMode.getValue())
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                .with(KafkaSchemaHistory.TOPIC, "history")
                .build();

        return new SqlServerConnection(new SqlServerConnectorConfig(config), null, Collections.emptySet(), true);
    }
}

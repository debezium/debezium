/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;

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
            assertTrue(query.contains("LEFT JOIN #db.cdc.lsn_time_mapping ltm ON ltm.start_lsn = cdc_data.[__$start_lsn]"),
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
            assertTrue(query.contains("SELECT cdc_data.[__$start_lsn], cdc_data.[__$seqval], cdc_data.[__$operation], cdc_data.[__$update_mask]"));
            assertTrue(query.contains("WHERE (([cdc_data].[__$start_lsn] = ?"));
            assertTrue(query.endsWith("ORDER BY cdc_data.[__$start_lsn] ASC, cdc_data.[__$seqval] ASC, cdc_data.[__$operation] ASC"));
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
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "server")
                .with(SqlServerConnectorConfig.HOSTNAME, "localhost")
                .with(SqlServerConnectorConfig.USER, "debezium")
                .with(SqlServerConnectorConfig.DATABASE_NAMES, "testDB")
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                .with(KafkaSchemaHistory.TOPIC, "history")
                .build();

        return new SqlServerConnection(new SqlServerConnectorConfig(config), null, Collections.emptySet(), true);
    }
}

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    @FixFor("debezium/dbz#2692")
    void directModeQueryShouldBeUnionAllOfSeekableBranchesRatherThanOrPredicate() throws Exception {
        // A single OR'd keyset predicate over the raw change table is a well known "OR prevents seek"
        // pattern that SQL Server's optimizer typically turns into a table scan instead of an index seek.
        // DIRECT mode must instead express the keyset as a UNION ALL of purely conjunctive branches.
        try (SqlServerConnection connection = testConnection(SqlServerConnectorConfig.DataQueryMode.DIRECT)) {
            String query = connection.buildGetAllChangesForTableQuery(SqlServerConnectorConfig.DataQueryMode.DIRECT, Collections.emptySet());

            assertEquals(4, countOccurrences(query, "UNION ALL") + 1, "expected exactly 4 branches joined by 3 UNION ALL");
            assertFalse(query.contains(" OR "), "DIRECT mode query must not contain an OR'd keyset predicate");
            assertTrue(query.trim().startsWith("SELECT * FROM ("), "DIRECT mode query must wrap the UNION ALL in an outer query");
            assertTrue(query.endsWith("ORDER BY [__$start_lsn] ASC, [__$command_id] ASC, [__$seqval] ASC, [__$operation] ASC"),
                    "ORDER BY must apply to the outer query, without a cdc_data prefix");
            // Each branch must carry the common upper/lower LSN bounds so every branch is independently seekable.
            assertEquals(4, countOccurrences(query, "[cdc_data].[__$start_lsn] <= ?"));
            assertEquals(4, countOccurrences(query, "[cdc_data].[__$start_lsn] >= ?"));
            // 4 + 3 + 2 + 1 branch-specific placeholders, plus 2 common bounds repeated per branch.
            assertEquals((4 + 3 + 2 + 1) + 4 * 2, countOccurrences(query, "?"));
            // The captured-columns and table-name placeholders are resolved later, per table, in getChangesForTable;
            // one copy per UNION branch is expected here.
            assertEquals(4, countOccurrences(query, "#cols#"));
            assertEquals(4, countOccurrences(query, "#table"));
        }
    }

    @Test
    @FixFor("debezium/dbz#2692")
    void directModeQueryShouldFilterSkippedOperationsOnEveryBranch() throws Exception {
        try (SqlServerConnection connection = testConnection(SqlServerConnectorConfig.DataQueryMode.DIRECT)) {
            String query = connection.buildGetAllChangesForTableQuery(SqlServerConnectorConfig.DataQueryMode.DIRECT, Collections.singleton(Envelope.Operation.DELETE));

            assertEquals(4, countOccurrences(query, "[cdc_data].[__$operation] NOT IN (1)"));
        }
    }

    @Test
    @FixFor("debezium/dbz#2692")
    void functionModeQueryShouldRemainUnchanged() throws Exception {
        try (SqlServerConnection connection = testConnection(SqlServerConnectorConfig.DataQueryMode.FUNCTION)) {
            String query = connection.buildGetAllChangesForTableQuery(SqlServerConnectorConfig.DataQueryMode.FUNCTION, Collections.emptySet());

            assertFalse(query.contains("UNION ALL"), "FUNCTION mode must not be affected by the DIRECT-mode rewrite");
            assertFalse(query.contains("cdc_data"), "FUNCTION mode must not be affected by the DIRECT-mode rewrite");
            assertTrue(query.contains("fn_cdc_map_lsn_to_time"), "FUNCTION mode must keep using the MS-published scalar function");
        }
    }

    private static int countOccurrences(String text, String needle) {
        int count = 0;
        Matcher matcher = Pattern.compile(Pattern.quote(needle)).matcher(text);
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    private SqlServerConnection testConnection() {
        return testConnection(null);
    }

    private SqlServerConnection testConnection(SqlServerConnectorConfig.DataQueryMode dataQueryMode) {
        Configuration.Builder builder = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "server")
                .with(SqlServerConnectorConfig.HOSTNAME, "localhost")
                .with(SqlServerConnectorConfig.USER, "debezium")
                .with(SqlServerConnectorConfig.DATABASE_NAMES, "testDB")
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                .with(KafkaSchemaHistory.TOPIC, "history");
        if (dataQueryMode != null) {
            builder = builder.with(SqlServerConnectorConfig.DATA_QUERY_MODE, dataQueryMode.getValue());
        }

        return new SqlServerConnection(new SqlServerConnectorConfig(builder.build()), null, Collections.emptySet(), true);
    }
}

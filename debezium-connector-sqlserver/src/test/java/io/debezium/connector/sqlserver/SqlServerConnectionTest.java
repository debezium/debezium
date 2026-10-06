/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
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
    @FixFor("debezium/dbz#2800")
    void shouldKeepNewestCaptureInstancePerStartLsnAfterFiltering() {
        final Lsn sharedStartLsn = Lsn.valueOf("00c853f3:00603f58:0001");
        final SqlServerChangeTable older = changeTable("dbo_knvsp", 1, sharedStartLsn);
        final SqlServerChangeTable newer = changeTable("vsp_tracking", 2, sharedStartLsn);
        final SqlServerChangeTable other = changeTable("dbo_other", 3, Lsn.valueOf("00c853f3:00603f58:0002"));

        // Two capture instances for the same source table and start LSN: only the newest one survives
        assertEquals(List.of(newer, other), SqlServerConnection.newestCaptureInstancePerStartLsn(List.of(
                candidate(100, "2025-01-01 10:00:00", older),
                candidate(100, "2025-06-01 10:00:00", newer),
                candidate(200, "2025-01-01 10:00:00", other))));

        // ... but when the newer one was removed by the capture instance filter, the older one must be kept
        // instead of leaving the source table without any capture instance
        assertEquals(List.of(older, other), SqlServerConnection.newestCaptureInstancePerStartLsn(List.of(
                candidate(100, "2025-01-01 10:00:00", older),
                candidate(200, "2025-01-01 10:00:00", other))));
    }

    private static SqlServerChangeTable changeTable(String captureInstance, int changeTableObjectId, Lsn startLsn) {
        return new SqlServerChangeTable(new TableId("testDB", "dbo", "table"), captureInstance, changeTableObjectId, startLsn,
                Collections.emptyList());
    }

    private static SqlServerConnection.CaptureInstanceCandidate candidate(int sourceObjectId, String createDate,
                                                                          SqlServerChangeTable changeTable) {
        return new SqlServerConnection.CaptureInstanceCandidate(sourceObjectId, Timestamp.valueOf(createDate), changeTable);
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

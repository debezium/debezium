/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogSessionRowImageIT;
import io.debezium.connector.binlog.BinlogStreamingChangeEventSource;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

public class MariaDbSessionRowImageIT extends BinlogSessionRowImageIT<MariaDbConnector> implements MariaDbCommon {

    private static final String JSON_TABLE = "json_noblob";

    @Test
    @FixFor("debezium/dbz#962")
    void shouldFailOnUpdateAfterImageThatOmitsColumnsOfFullNodupSession() throws Exception {
        // With FULL_NODUP, the before image of an update contains all columns, while the after image contains only the changed ones
        assertFailsOnRowImageThatOmitsColumnsOfSession("FULL_NODUP", true, "UPDATE", "UPDATE after");
    }

    @Test
    @FixFor("debezium/dbz#962")
    void shouldStreamRowImageThatOmitsJsonOfNoblobServer() throws Exception {
        final String serverRowImage = getServerRowImage();
        // MariaDB stores JSON as LONGTEXT, so a server that uses NOBLOB omits an unchanged JSON column like TEXT
        executeStatements(database.getDatabaseName(), "SET GLOBAL binlog_row_image='NOBLOB'");
        try {
            final LogInterceptor logInterceptor = new LogInterceptor(BinlogStreamingChangeEventSource.class);
            start(getConnectorClass(), database.defaultConfig()
                    .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                    .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName(JSON_TABLE))
                    .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                    .build());
            waitForStreamingRunning(getConnectorName(), database.getServerName());

            // Created while streaming, so that the DDL parser reads the type name JSON rather than LONGTEXT
            executeStatements(database.getDatabaseName(),
                    "CREATE TABLE " + JSON_TABLE + " (id INT PRIMARY KEY, a INT, j JSON)",
                    "INSERT INTO " + JSON_TABLE + " VALUES (1, 1, '{\"k\": 1}')",
                    "UPDATE " + JSON_TABLE + " SET a = 2 WHERE id = 1");

            final List<SourceRecord> records = consumeRecordsByTopic(2).recordsForTopic(database.topicForTable(JSON_TABLE));
            assertThat(records).hasSize(2);
            final Struct after = ((Struct) records.get(1).value()).getStruct("after");
            assertThat(after.getInt32("a")).isEqualTo(2);
            assertThat(after.getString("j")).isEqualTo(RelationalDatabaseConnectorConfig.DEFAULT_UNAVAILABLE_VALUE_PLACEHOLDER);
            assertThat(logInterceptor.containsMessage("Incomplete row image")).isFalse();
        }
        finally {
            executeStatements(database.getDatabaseName(), "SET GLOBAL binlog_row_image='" + serverRowImage + "'");
        }
    }

    private String getServerRowImage() throws SQLException {
        try (BinlogTestConnection db = getTestDatabaseConnection(database.getDatabaseName());
                JdbcConnection connection = db.connect()) {
            return connection.queryAndMap("SELECT @@GLOBAL.binlog_row_image", rs -> {
                if (rs.next()) {
                    return rs.getString(1);
                }
                throw new IllegalStateException("Could not get binlog_row_image");
            });
        }
    }
}

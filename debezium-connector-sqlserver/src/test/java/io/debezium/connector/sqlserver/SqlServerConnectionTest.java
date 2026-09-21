/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;

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

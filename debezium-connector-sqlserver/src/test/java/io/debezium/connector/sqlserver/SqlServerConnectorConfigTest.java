/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

public class SqlServerConnectorConfigTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnectorConfigTest.class);

    @Test
    public void emptyDatabaseNames() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig().build());
        assertFalse(connectorConfig.validateAndRecord(SqlServerConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void nonEmptyDatabaseNames() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.DATABASE_NAMES, "testDB1")
                        .build());
        assertTrue(connectorConfig.validateAndRecord(SqlServerConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    private Configuration.Builder defaultConfig() {
        return Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "server")
                .with(SqlServerConnectorConfig.HOSTNAME, "localhost")
                .with(SqlServerConnectorConfig.USER, "debezium")
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                .with(KafkaSchemaHistory.TOPIC, "history");
    }
}

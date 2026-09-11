/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.transforms.timescaledb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startables;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.TestHelper;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.testing.testcontainers.ImageNames;

class TimescaleDb229DatabaseIT extends AbstractAsyncEngineConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimescaleDb229DatabaseIT.class);

    private final PostgreSQLContainer<?> timescaleDbContainer = new PostgreSQLContainer<>(ImageNames.TIMESCALE_DB_PG18_IMAGE_NAME)
            .withNetworkAliases("postgres")
            .withUsername("postgres")
            .withPassword("postgres")
            .withDatabaseName("postgres")
            .withCopyToContainer(Transferable.of("#!/bin/bash\n"
                    + "\n"
                    + "echo \"wal_level=logical\" >> ${POSTGRESQL_CONF_DIR}/postgresql.conf"),
                    "docker-entrypoint-initdb.d/002_enable_replication.sh");

    private PostgresConnection connection;
    private Configuration config;

    @BeforeEach
    void prepareDatabase() throws Exception {
        Startables.deepStart(timescaleDbContainer).join();
        final JdbcConfiguration.Builder jdbcConfig = TestHelper.defaultJdbcConfigBuilder()
                .with(JdbcConfiguration.HOSTNAME, timescaleDbContainer.getHost())
                .with(JdbcConfiguration.PORT, timescaleDbContainer.getMappedPort(5432));

        connection = new PostgresConnection(jdbcConfig.build(), TestHelper.CONNECTION_TEST);
        dropPublication(connection);
        connection.execute(
                "DROP TABLE IF EXISTS conditions",
                "CREATE TABLE conditions (time TIMESTAMPTZ NOT NULL, location TEXT NOT NULL);",
                "SELECT create_hypertable('conditions', 'time');",
                "CREATE PUBLICATION dbz_publication FOR ALL TABLES WITH (publish = 'insert,update')");

        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.HOSTNAME, timescaleDbContainer.getHost())
                .with(PostgresConnectorConfig.PORT, timescaleDbContainer.getMappedPort(5432))
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "_timescaledb_internal")
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with("transforms", "timescaledb")
                .with("transforms.timescaledb.type", TimescaleDb.class.getName())
                .with("transforms.timescaledb.database.hostname", timescaleDbContainer.getHost())
                .with("transforms.timescaledb.database.port", timescaleDbContainer.getMappedPort(5432))
                .with("transforms.timescaledb.database.user", "postgres")
                .with("transforms.timescaledb.database.password", "postgres")
                .with("transforms.timescaledb.database.dbname", "postgres")
                .build();
    }

    @AfterEach
    void dropDatabase() {
        timescaleDbContainer.stop();
    }

    @Test
    void shouldTransformChunksOnTimescaleDb229() throws Exception {
        final var version = connection.queryAndMap(
                "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'",
                rs -> rs.next() ? rs.getString(1) : null);
        assertThat(version).isNotNull();
        assertThat(QueryInformationSchemaMetadata.isTimescaleDbVersionAtLeast229(version)).isTrue();

        start(PostgresConnector.class, config);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        connection.execute("INSERT INTO conditions VALUES (now(), 'Loc 1')");

        final var records = consumeRecordsByTopic(1);
        assertConnectorIsRunning();
        assertThat(records.recordsForTopic("timescaledb.public.conditions")).hasSize(1);

        stopConnector();
    }

    private void dropPublication(PostgresConnection connection) {
        try {
            connection.execute("DROP PUBLICATION IF EXISTS dbz_publication");
        }
        catch (Exception e) {
            LOGGER.debug("Error while dropping publication", e);
        }
    }
}

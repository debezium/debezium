/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.transforms.timescaledb;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
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
import io.debezium.util.Testing;

public class TimescaleDbDatabaseIT extends AbstractAsyncEngineConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimescaleDbDatabaseIT.class);

    private static final org.testcontainers.containers.Network network = org.testcontainers.containers.Network.newNetwork();

    public static final PostgreSQLContainer<?> timescaleDbContainer = new PostgreSQLContainer<>(ImageNames.TIMESCALE_DB_IMAGE_NAME)
            .withNetwork(network)
            .withNetworkAliases("postgres")
            .withUsername("postgres")
            .withPassword("postgres")
            .withDatabaseName("postgres")
            .withReuse(false)
            .withCopyToContainer(Transferable.of("#!/bin/bash\n"
                    + "\n"
                    + "echo \"wal_level=logical\" >> ${POSTGRESQL_CONF_DIR}/postgresql.conf"),
                    "docker-entrypoint-initdb.d/002_enable_replication.sh");

    private PostgresConnection connection;
    private Configuration config;

    @Before
    public void prepareDatabase() throws Exception {

        Startables.deepStart(timescaleDbContainer).join();
        JdbcConfiguration.Builder jdbcConfig = TestHelper.defaultJdbcConfigBuilder();
        jdbcConfig.with(JdbcConfiguration.HOSTNAME, timescaleDbContainer.getHost());
        jdbcConfig.with(JdbcConfiguration.PORT, timescaleDbContainer.getMappedPort(5432));

        connection = new PostgresConnection(
                jdbcConfig.build(), TestHelper.CONNECTION_TEST);
        dropPublication(connection);
        connection.execute(
                "DROP TABLE IF EXISTS conditions",
                "CREATE TABLE conditions (time TIMESTAMPTZ NOT NULL, location TEXT NOT NULL, temperature DOUBLE PRECISION NULL, humidity DOUBLE PRECISION NULL);",
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

    @After
    public void dropDatabase() {
        timescaleDbContainer.stop();
    }

    protected void insertData() throws SQLException {
        connection.execute(
                "INSERT INTO conditions VALUES (now(), 'Loc 1', 30, 50)",
                "INSERT INTO conditions VALUES (now(), 'Loc 1', 35, 55)",
                "INSERT INTO conditions VALUES (now(), 'Loc 1', 40, 60)");
    }

    @Test
    public void shouldTransformChunks() throws Exception {
        // Testing.Print.enable();

        start(PostgresConnector.class, config);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        insertData();

        var records = consumeRecordsByTopic(3);
        assertConnectorIsRunning();

        assertThat(records.topics()).hasSize(1);
        assertThat(records.recordsForTopic("timescaledb.public.conditions")).hasSize(3);

        stopConnector();
    }

    @Test
    public void shouldTransformAggregates() throws Exception {
        // Testing.Print.enable();

        start(PostgresConnector.class, config);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        insertData();
        // The aggregates are created after the data are available
        // so they are primed
        connection.execute(
                "  CREATE MATERIALIZED VIEW conditions_summary\n"
                        + "        WITH (timescaledb.continuous) AS\n"
                        + "        SELECT location,\n"
                        + "           time_bucket(INTERVAL '1 hour', time) AS bucket,\n"
                        + "           AVG(temperature),\n"
                        + "           MAX(temperature),\n"
                        + "           MIN(temperature)\n"
                        + "        FROM conditions\n"
                        + "        GROUP BY location, bucket;\n");

        var records = consumeRecordsByTopic(4);
        assertConnectorIsRunning();

        assertThat(records.topics()).hasSize(2);
        assertThat(records.recordsForTopic("timescaledb.public.conditions_summary")).hasSize(1);

        stopConnector();
    }

    @Test
    public void shouldTransformCompressedChunks() throws Exception {
        Testing.Print.enable();

        start(PostgresConnector.class, config);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        insertData();
        connection.execute(
                "ALTER TABLE conditions SET (timescaledb.compress, timescaledb.compress_orderby = 'time DESC', timescaledb.compress_segmentby = 'location')",
                "SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk')");

        // 3 data messages, 1 raw compressed chunk message, 2 WAL notification messages about compression in progress
        var records = consumeRecordsByTopic(6);
        assertConnectorIsRunning();

        assertThat(records.recordsForTopic("timescaledb.public.conditions")).hasSize(3);
        assertThat(records.recordsForTopic("timescaledb._timescaledb_internal._compressed_hypertable_2")).hasSize(1);

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

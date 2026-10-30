/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.ExposedPort;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.postgresql.PostgresConnectorConfig.AutoCreateMode;
import io.debezium.connector.postgresql.PostgresConnectorConfig.LogicalDecoder;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.junit.logging.LogInterceptor;

/**
 * Integration tests for draining the replication stream after PostgreSQL terminates the connector's regular JDBC connection.
 * Requires PostgreSQL 17 or later, because only then does the shutdown checkpoint persist the confirmed flush position of logical slots.
 */
public class PostgresShutdownDrainIT extends AbstractAsyncEngineConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresShutdownDrainIT.class);

    private static final String SLOT = "drain_slot";
    private static final String PUBLICATION = "drain_pub";
    private static final String TOPIC = TestHelper.topicName("s1.a");
    private static final int BACKLOG = 1_000;
    private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE; " +
            "CREATE SCHEMA s1; " +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));";

    private final PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>(
            DockerImageName.parse("quay.io/debezium/postgres:17").asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("postgres")
            .withUsername("postgres")
            .withPassword("postgres")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER));

    private String oldContainerPort;
    private final AtomicInteger inserted = new AtomicInteger();
    private ScheduledExecutorService inserter;

    @BeforeEach
    void setUp() throws SQLException {
        postgresContainer.start();
        oldContainerPort = System.getProperty("database.port", "5432");
        System.setProperty("database.port", String.valueOf(postgresContainer.getMappedPort(5432)));
        TestHelper.dropAllSchemas();
        TestHelper.execute(CREATE_TABLES_STMT);
        initializeConnectorTestFramework();
    }

    @AfterEach
    void tearDown() {
        if (inserter != null) {
            inserter.shutdownNow();
        }
        stopConnector();
        postgresContainer.stop();
        System.setProperty("database.port", oldContainerPort);
    }

    @Test
    @FixFor("debezium/dbz#2951")
    public void shouldConfirmBacklogBeforeFastShutdownCompletes() throws Exception {
        start(PostgresConnector.class, drainConfig(waitFor(30)).build());
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO s1.a (aa) SELECT generate_series(1, " + BACKLOG + ");");

        // The bounded test queue fills, so the connector stops reading until the test consumes again.
        Awaitility.await().atMost(waitFor(30)).until(() -> consumedLines.remainingCapacity() == 0);

        final String dataDirectory = queryString("SHOW data_directory");
        postgresContainer.execInContainer("su", "-", "postgres", "-c", "/usr/lib/postgresql/*/bin/pg_ctl -m fast -W -D " + dataDirectory + " stop");

        // New connections fail once the fast shutdown has terminated the regular backends.
        Awaitility.await().atMost(waitFor(15)).until(() -> !canConnect());

        final AtomicInteger streamed = new AtomicInteger();
        Awaitility.await().atMost(waitFor(60)).until(() -> {
            consumeAvailableRecords(r -> {
                if (TOPIC.equals(r.topic())) {
                    streamed.incrementAndGet();
                }
            });
            return !postgresContainer.isRunning();
        });
        Awaitility.await().atMost(waitFor(15)).until(() -> getLastEngineFailure().isPresent());

        restartStoppedContainer();
        final long pending = pendingDecodableChanges();
        LOGGER.info("Streamed {} of {} backlog changes; {} decodable changes remain in slot '{}'", streamed.get(), BACKLOG, pending, SLOT);

        assertThat(streamed.get()).isEqualTo(BACKLOG);
        assertThat(pending).isZero();
        assertThat(hasAdminShutdownCause(getLastEngineFailure().get())).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2951")
    public void shouldStopDrainingWhenTimeoutExpires() throws Exception {
        final LogInterceptor drainLog = new LogInterceptor(PostgresStreamingChangeEventSource.class);
        start(PostgresConnector.class, drainConfig(waitFor(1)).build());
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        // Inserts arrive every 100 ms, so the stream is never quiet for the 1 s status interval and the drain can only end by timeout.
        startInserter();
        terminateRegularConnection();
        final int lastInsertBeforeTermination = inserted.get();

        final AtomicInteger streamedAfterTermination = new AtomicInteger();
        consumeUntilEngineFailure(r -> {
            if (TOPIC.equals(r.topic())
                    && ((Struct) ((Struct) r.value()).get(Envelope.FieldName.AFTER)).getInt32("aa") > lastInsertBeforeTermination) {
                streamedAfterTermination.incrementAndGet();
            }
        });

        assertThat(streamedAfterTermination.get()).isPositive();
        assertThat(drainLog.containsWarnMessage("did not complete within")).isTrue();
        assertThat(hasAdminShutdownCause(getLastEngineFailure().get())).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2951")
    public void shouldCloseReplicationConnectionWhenConsumerStallsDuringDrain() throws Exception {
        final LogInterceptor drainLog = new LogInterceptor(PostgresStreamingChangeEventSource.class);
        // NO_DATA leaves the streaming phase without a stored offset, as for a connector that starts on an empty offset store.
        start(PostgresConnector.class, drainConfig(Duration.ofMillis(TestHelper.waitTimeForRecords() * 1_500L)).build());
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        // Continuous inserts keep the stream busy, so the drain cannot end because the stream is quiet.
        startInserter();
        terminateRegularConnection();

        // The connector notices the terminated connection within one status update interval. The test consumes meanwhile, so the queue is empty.
        Awaitility.await().atMost(waitFor(5)).until(() -> {
            consumeAvailableRecords(null);
            return drainLog.containsMessage("Continuing to read the replication stream");
        });
        assertThat(getLastEngineFailure()).isEmpty();

        // The test stops consuming, so the bounded queues fill and the streaming thread blocks while it dispatches.
        TestHelper.execute("INSERT INTO s1.a (aa) SELECT generate_series(1, " + BACKLOG + ");");
        Awaitility.await().atMost(waitFor(30)).until(() -> consumedLines.remainingCapacity() == 0);

        // The engine cannot report the failure while the test does not consume. The released slot proves that the replication connection is closed.
        Awaitility.await().atMost(waitFor(15))
                .until(() -> "false".equals(queryString("SELECT active::text FROM pg_replication_slots WHERE slot_name = '" + SLOT + "'")));

        consumeUntilEngineFailure(null);
        assertThat(hasAdminShutdownCause(getLastEngineFailure().get())).isTrue();
        assertThat(drainLog.containsWarnMessage("did not complete within")).isTrue();
    }

    /**
     * Scales a wait with the {@code records.waittime} test property, as the other connector tests do, so that slow hardware can extend it.
     */
    private static Duration waitFor(int multiplier) {
        return Duration.ofSeconds((long) TestHelper.waitTimeForRecords() * multiplier);
    }

    /**
     * Inserts a row every 100 ms and returns after the first change arrives, so that the table schema is loaded and the stream is busy before the test terminates the connection.
     */
    private void startInserter() {
        inserter = Executors.newSingleThreadScheduledExecutor();
        inserter.scheduleAtFixedRate(() -> {
            try {
                TestHelper.execute("INSERT INTO s1.a (aa) VALUES (" + inserted.incrementAndGet() + ");");
            }
            catch (Exception e) {
                LOGGER.warn("Insert failed", e);
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
        assertThat(waitForAvailableRecords(waitFor(15).toSeconds(), TimeUnit.SECONDS)).isTrue();
    }

    private static void terminateRegularConnection() {
        TestHelper.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = '" + PostgresConnection.CONNECTION_GENERAL + "'");
    }

    private void consumeUntilEngineFailure(Consumer<SourceRecord> recordConsumer) {
        Awaitility.await().atMost(waitFor(15)).until(() -> {
            consumeAvailableRecords(recordConsumer);
            return getLastEngineFailure().isPresent();
        });
    }

    private Configuration.Builder drainConfig(Duration drainTimeout) {
        return TestHelper.defaultConfig()
                .with(ConfigurationNames.DATABASE_CONFIG_PREFIX + JdbcConfiguration.PORT, postgresContainer.getMappedPort(5432))
                .with(PostgresConnectorConfig.PLUGIN_NAME, LogicalDecoder.PGOUTPUT.getValue())
                .with(PostgresConnectorConfig.SLOT_NAME, SLOT)
                .with(PostgresConnectorConfig.PUBLICATION_NAME, PUBLICATION)
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, AutoCreateMode.ALL_TABLES.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(PostgresConnectorConfig.STATUS_UPDATE_INTERVAL_MS, 1_000)
                .with(PostgresConnectorConfig.DATABASE_SHUTDOWN_DRAIN_TIMEOUT_MS, drainTimeout.toMillis())
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, 20)
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, 10)
                .with(CommonConnectorConfig.MAX_RETRIES_ON_ERROR, 0);
    }

    private static boolean canConnect() {
        try (PostgresConnection connection = TestHelper.create()) {
            connection.execute("SELECT 1");
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    private static String queryString(String sql) throws SQLException {
        try (PostgresConnection connection = TestHelper.create()) {
            return connection.queryAndMap(sql, connection.singleResultMapper(rs -> rs.getString(1), "No result for " + sql));
        }
    }

    private static long pendingDecodableChanges() throws SQLException {
        try (PostgresConnection connection = TestHelper.create()) {
            return connection.queryAndMap("SELECT count(*) FROM pg_logical_slot_peek_binary_changes('" + SLOT + "', NULL, NULL, "
                    + "'proto_version', '1', 'publication_names', '" + PUBLICATION + "')",
                    connection.singleResultMapper(rs -> rs.getLong(1), "No result from slot peek"));
        }
    }

    private static boolean hasAdminShutdownCause(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException && "57P01".equals(((SQLException) cause).getSQLState())) {
                return true;
            }
        }
        return false;
    }

    private void restartStoppedContainer() {
        final DockerClient client = postgresContainer.getDockerClient();
        final String containerId = postgresContainer.getContainerId();
        client.startContainerCmd(containerId).exec();
        final String hostPort = client.inspectContainerCmd(containerId).exec()
                .getNetworkSettings().getPorts().getBindings().get(ExposedPort.tcp(5432))[0].getHostPortSpec();
        System.setProperty("database.port", hostPort);
        Awaitility.await().atMost(waitFor(30)).until(PostgresShutdownDrainIT::canConnect);
    }
}

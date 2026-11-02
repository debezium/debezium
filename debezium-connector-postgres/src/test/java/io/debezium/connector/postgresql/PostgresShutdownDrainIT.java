/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
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
import io.debezium.connector.postgresql.junit.PostgresDatabaseVersionResolver;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.logging.LogInterceptor;

import ch.qos.logback.classic.Level;

/**
 * Integration tests for draining the replication stream after PostgreSQL terminates the connector's regular JDBC connection.
 * The tests that terminate the regular connection run on the shared database of every matrix version.
 * The fast-shutdown test stops a server, so it runs in a private container and requires PostgreSQL 17 or later,
 * because only then does the shutdown checkpoint persist the confirmed flush position of logical slots.
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

    private final AtomicInteger inserted = new AtomicInteger();
    private ScheduledExecutorService inserter;

    @BeforeEach
    void setUp() throws SQLException {
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
        // Slots and publications outlive the connector, and the database is shared with other tests.
        slotInactiveWithin(waitFor(5));
        try (PostgresConnection connection = TestHelper.create()) {
            connection.dropReplicationSlot(SLOT);
            connection.dropPublication(PUBLICATION);
        }
    }

    @Test
    @FixFor("debezium/dbz#2951")
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 17, reason = "Shutdown checkpoint persists logical slot confirmation only on PostgreSQL 17+")
    public void shouldConfirmBacklogBeforeFastShutdownCompletes() throws Exception {
        // The test stops the server, so it needs a private container. It uses the major version of the shared matrix database.
        final int major = new PostgresDatabaseVersionResolver().getVersion().getMajor();
        final PostgreSQLContainer<?> container = new PostgreSQLContainer<>(
                DockerImageName.parse("quay.io/debezium/postgres:" + major).asCompatibleSubstituteFor("postgres"))
                .withDatabaseName("postgres")
                .withUsername("postgres")
                .withPassword("postgres")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER));
        final String oldPort = System.getProperty("database.port", "5432");
        try {
            container.start();
            System.setProperty("database.port", String.valueOf(container.getMappedPort(5432)));
            TestHelper.dropAllSchemas();
            TestHelper.execute(CREATE_TABLES_STMT);

            start(PostgresConnector.class, drainConfig(waitFor(30))
                    .with(ConfigurationNames.DATABASE_CONFIG_PREFIX + JdbcConfiguration.PORT, container.getMappedPort(5432))
                    .with(PostgresConnectorConfig.PLUGIN_NAME, LogicalDecoder.PGOUTPUT.getValue())
                    .build());
            assertConnectorIsRunning();
            waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

            TestHelper.execute("INSERT INTO s1.a (aa) SELECT generate_series(1, " + BACKLOG + ");");

            // The bounded test queue fills, so the connector stops reading until the test consumes again.
            Awaitility.await().atMost(waitFor(30)).until(() -> consumedLines.remainingCapacity() == 0);

            final String dataDirectory = queryString("SHOW data_directory");
            container.execInContainer("su", "-", "postgres", "-c", "/usr/lib/postgresql/*/bin/pg_ctl -m fast -W -D " + dataDirectory + " stop");

            // New connections fail once the fast shutdown has terminated the regular backends.
            Awaitility.await().atMost(waitFor(15)).until(() -> !canConnect());

            final AtomicInteger streamed = new AtomicInteger();
            Awaitility.await().atMost(waitFor(60)).until(() -> {
                consumeAvailableRecords(r -> {
                    if (TOPIC.equals(r.topic())) {
                        streamed.incrementAndGet();
                    }
                });
                return !container.isRunning();
            });
            Awaitility.await().atMost(waitFor(15)).until(() -> getLastEngineFailure().isPresent());

            restartStoppedContainer(container);
            final long pending = pendingDecodableChanges();
            LOGGER.info("Streamed {} of {} backlog changes; {} decodable changes remain in slot '{}'", streamed.get(), BACKLOG, pending, SLOT);

            assertThat(streamed.get()).isEqualTo(BACKLOG);
            assertThat(pending).isZero();
            assertThat(hasAdminShutdownCause(getLastEngineFailure().get())).isTrue();
        }
        finally {
            try {
                stopConnector();
            }
            finally {
                try {
                    container.stop();
                }
                finally {
                    System.setProperty("database.port", oldPort);
                }
            }
        }
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

    private void restartStoppedContainer(PostgreSQLContainer<?> container) {
        final DockerClient client = container.getDockerClient();
        final String containerId = container.getContainerId();
        client.startContainerCmd(containerId).exec();
        final String hostPort = client.inspectContainerCmd(containerId).exec()
                .getNetworkSettings().getPorts().getBindings().get(ExposedPort.tcp(5432))[0].getHostPortSpec();
        System.setProperty("database.port", hostPort);
        Awaitility.await().atMost(waitFor(30)).until(PostgresShutdownDrainIT::canConnect);
    }

    private static long activeSlotCount() {
        try {
            return Long.parseLong(queryString("SELECT count(*) FROM pg_replication_slots WHERE slot_name = '" + SLOT + "' AND active"));
        }
        catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static boolean slotInactiveWithin(Duration timeout) {
        try {
            Awaitility.await().atMost(timeout).until(() -> activeSlotCount() == 0);
            return true;
        }
        catch (ConditionTimeoutException e) {
            return false;
        }
    }

    /**
     * Records whether the slot was already released when the task begins its own shutdown, before {@link PostgresConnectorTask#doStop()} closes the replication connection.
     */
    static class CleanupObservingTask extends PostgresConnectorTask {
        volatile boolean slotReleasedBeforeDoStop;

        @Override
        protected void doStop() {
            slotReleasedBeforeDoStop = slotInactiveWithin(waitFor(5));
            super.doStop();
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static SourceTaskContext taskContext(Map<String, String> props, Map<String, ?> partition, Map<String, Object> offset) {
        final OffsetStorageReader reader = mock(OffsetStorageReader.class);
        if (partition != null) {
            when((Map) reader.offsets(any())).thenReturn(Map.of(partition, offset));
        }
        final SourceTaskContext context = mock(SourceTaskContext.class);
        when(context.configs()).thenReturn(props);
        when(context.offsetStorageReader()).thenReturn(reader);
        return context;
    }

    // The engine sets connector.class for the connectors it starts. The tests that drive the task directly must set it.
    private Configuration.Builder cleanupConfig() {
        return drainConfig(Duration.ZERO).with("connector.class", PostgresConnector.class.getName());
    }

    @Test
    @FixFor("debezium/dbz#2951")
    public void shouldReleaseSlotOnStopWithoutStoredOffset() throws Exception {
        final Configuration config = cleanupConfig().build();
        final CleanupObservingTask task = new CleanupObservingTask();
        try {
            task.initialize(taskContext(config.asMap(), null, null));
            task.start(config.asMap());
            waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
            Awaitility.await().atMost(waitFor(5)).until(() -> activeSlotCount() == 1);

            task.stop();

            assertThat(task.slotReleasedBeforeDoStop).isTrue();
        }
        finally {
            task.stop();
        }
    }

    @Test
    @FixFor("debezium/dbz#2951")
    public void shouldReleaseSlotWhenCleanupCommitFails() throws Exception {
        final String user = "drain_cleanup_user";
        TestHelper.execute(
                "DROP ROLE IF EXISTS " + user + ";",
                "CREATE ROLE " + user + " WITH REPLICATION LOGIN PASSWORD '" + user + "';",
                "GRANT ALL PRIVILEGES ON DATABASE postgres TO " + user + ";",
                "GRANT USAGE ON SCHEMA s1 TO " + user + ";",
                "GRANT SELECT ON ALL TABLES IN SCHEMA s1 TO " + user + ";",
                "CREATE PUBLICATION " + PUBLICATION + " FOR TABLE s1.a;");
        final LogInterceptor log = new LogInterceptor(PostgresStreamingChangeEventSource.class);
        CleanupObservingTask task = null;
        try {
            final Configuration config = cleanupConfig()
                    .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, AutoCreateMode.DISABLED.getValue())
                    .with(PostgresConnectorConfig.DATABASE_CONFIG_PREFIX + JdbcConfiguration.USER, user)
                    .with(PostgresConnectorConfig.DATABASE_CONFIG_PREFIX + JdbcConfiguration.PASSWORD, user)
                    .build();

            // Seed a stored offset, so that the task under test starts from a non-null offset.
            start(PostgresConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
            TestHelper.execute("INSERT INTO s1.a (aa) VALUES (1);");
            final SourceRecord record = consumeRecordsByTopic(1).recordsForTopic(TOPIC).get(0);
            stopConnector();
            final Map<String, Object> offset = readLastCommittedOffset(config, record.sourcePartition());
            assertThat(offset).isNotNull();
            assertThat(slotInactiveWithin(waitFor(5))).isTrue();

            log.setLoggerLevel(PostgresStreamingChangeEventSource.class, Level.DEBUG);
            task = new CleanupObservingTask();
            task.initialize(taskContext(config.asMap(), record.sourcePartition(), offset));
            task.start(config.asMap());
            waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
            Awaitility.await().atMost(waitFor(5)).until(() -> activeSlotCount() == 1);

            // JdbcConnection.commit() reconnects after a termination, so the role must also lose its login right.
            TestHelper.execute(
                    "ALTER ROLE " + user + " NOLOGIN;",
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '" + user + "' AND application_name = '"
                            + PostgresConnection.CONNECTION_GENERAL + "';");

            // The task is neither polled nor stopped, so only the streaming cleanup can release the slot.
            assertThat(slotInactiveWithin(waitFor(5))).isTrue();
            assertThat(log.containsMessage("Exception while committing the regular connection")).isTrue();
        }
        finally {
            try {
                if (task != null) {
                    task.stop();
                }
            }
            finally {
                log.setLoggerLevel(PostgresStreamingChangeEventSource.class, null);
                TestHelper.execute(
                        "ALTER ROLE " + user + " LOGIN;",
                        "REVOKE ALL PRIVILEGES ON DATABASE postgres FROM " + user + ";",
                        "DROP OWNED BY " + user + " CASCADE;",
                        "DROP ROLE IF EXISTS " + user + ";");
            }
        }
    }
}

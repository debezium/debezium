/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceConnector;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.DatabaseTcpProxy;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.junit.logging.LogInterceptor;

/**
 * Verifies that the connector fails instead of staying in a running state without emitting events when the
 * binlog connection is lost and cannot be restored.
 *
 * @author Chris Cranford
 */
public abstract class BinlogStaleConnectionIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-stale-connection.txt")
            .toAbsolutePath();

    private static final int KEEP_ALIVE_INTERVAL_MS = 1_000;
    private static final int MAX_RECONNECT_ATTEMPTS = 2;

    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("staleconn", "empty")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("debezium/dbz#1474")
    void shouldFailWhenBinlogConnectionCannotBeRestored() throws SQLException, IOException, InterruptedException {
        final LogInterceptor logInterceptor = new LogInterceptor(BinlogStreamingChangeEventSource.class);

        // Route the connector through a proxy so the connection can be broken at the network level ...
        try (DatabaseTcpProxy proxy = DatabaseTcpProxy.forward(
                System.getProperty("database.hostname", "localhost"),
                Integer.parseInt(System.getProperty("database.port", "3306")))) {

            config = DATABASE.defaultConfig()
                    .with(BinlogConnectorConfig.HOSTNAME, proxy.getHostname())
                    .with(BinlogConnectorConfig.PORT, proxy.getPort())
                    .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                    .with(BinlogConnectorConfig.KEEP_ALIVE_INTERVAL_MS, KEEP_ALIVE_INTERVAL_MS)
                    .with(BinlogConnectorConfig.KEEP_ALIVE_MAX_RECONNECT_ATTEMPTS, MAX_RECONNECT_ATTEMPTS)
                    .build();

            final AtomicReference<Throwable> engineFailure = new AtomicReference<>();
            start(getConnectorClass(), config, (success, message, error) -> engineFailure.set(error));
            assertConnectorIsRunning();
            waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());

            // Prove that change events really are flowing through the proxy before it is broken. The
            // statements are executed directly against the database, not through the proxy ...
            executeStatements(DATABASE.getDatabaseName(),
                    "CREATE TABLE dbz9755 (id INT PRIMARY KEY, name VARCHAR(64))",
                    "INSERT INTO dbz9755 VALUES (1, 'before')");
            assertThat(consumeRecordsByTopic(1).allRecordsInOrder()).hasSize(1);

            // The TCP connection stays ESTABLISHED but nothing flows over it any more, and no new
            // connection can be established either ...
            proxy.blackhole();

            // Detection takes one keep alive interval, then every reconnect attempt takes another, so the
            // task must fail shortly after (MAX_RECONNECT_ATTEMPTS + 1) intervals ...
            Awaitility.await()
                    .alias("connector to fail after the binlog connection could not be restored")
                    .atMost(Duration.ofSeconds(60))
                    .pollInterval(Duration.ofMillis(200))
                    .until(() -> engineFailure.get() != null);

            assertConnectorNotRunning();
            assertThat(logInterceptor.containsErrorMessage("Binlog client gave up restoring the connection")).isTrue();
        }
    }
}

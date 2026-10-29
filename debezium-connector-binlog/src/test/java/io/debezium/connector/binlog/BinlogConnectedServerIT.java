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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.DatabaseTcpProxy;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.util.MetricsHelper;
import io.debezium.junit.logging.LogInterceptor;

/**
 * Verifies that the streaming metrics report the server that provides the binlog stream, when the connector reaches
 * the database through an address that can move to another server, as a DNS name or a VIP does on a failover.
 */
public abstract class BinlogConnectedServerIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-connected-server.txt")
            .toAbsolutePath();

    private static final int KEEP_ALIVE_INTERVAL_MS = 1_000;

    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("connectedserver", "empty")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private final String primaryHost = System.getProperty("database.hostname", "localhost");
    private final int primaryPort = Integer.parseInt(System.getProperty("database.port", "3306"));
    private final String replicaHost = System.getProperty("database.replica.hostname", primaryHost);
    private final int replicaPort = Integer.parseInt(System.getProperty("database.replica.port", String.valueOf(primaryPort)));

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
    @FixFor("debezium/dbz#2969")
    void shouldReportTheSameServerAfterReconnectingToIt() throws IOException, SQLException {
        final LogInterceptor logInterceptor = new LogInterceptor(BinlogStreamingChangeEventSource.class);

        try (DatabaseTcpProxy proxy = DatabaseTcpProxy.forward(primaryHost, primaryPort)) {
            startThrough(proxy);
            final Map<String, String> primary = serverOf(getTestDatabaseConnection(DATABASE.getDatabaseName()));
            awaitConnectedServer(primary);
            assertThat(numberOfConnectedServerChanges()).isZero();

            proxy.dropConnections();
            Awaitility.await()
                    .alias("connector to reconnect to the binlog")
                    .atMost(Duration.ofSeconds(60))
                    .until(() -> logInterceptor.countOccurrences("Connected to binlog at") >= 2);

            awaitConnectedServer(primary);
            assertThat(numberOfConnectedServerChanges()).isZero();
        }
    }

    @Test
    @FixFor("debezium/dbz#2969")
    void shouldReportTheServerThatProvidesTheStreamAfterAFailover() throws IOException, SQLException, InterruptedException {
        Assumptions.assumeFalse(replicaHost.equals(primaryHost) && replicaPort == primaryPort, "The test requires a replica server");

        try (DatabaseTcpProxy proxy = DatabaseTcpProxy.forward(primaryHost, primaryPort)) {
            startThrough(proxy);
            final Map<String, String> primary = serverOf(getTestDatabaseConnection(DATABASE.getDatabaseName()));
            final Map<String, String> replica = serverOf(getTestReplicaDatabaseConnection(DATABASE.getDatabaseName()));
            assertThat(replica).isNotEqualTo(primary);
            awaitConnectedServer(primary);

            // The address now leads to the replica, as after a failover ...
            proxy.retarget(replicaHost, replicaPort);
            proxy.dropConnections();
            awaitConnectedServer(replica);
            assertThat(numberOfConnectedServerChanges()).isEqualTo(1);

            // ... which streams the rows written on the primary with the server id of the primary
            executeStatements(DATABASE.getDatabaseName(),
                    "CREATE TABLE connected_server (id INT PRIMARY KEY)",
                    "INSERT INTO connected_server VALUES (1)");
            final List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(DATABASE.topicForTable("connected_server"));
            assertThat(records).hasSize(1);
            final Struct source = ((Struct) records.get(0).value()).getStruct(Envelope.FieldName.SOURCE);
            assertThat(String.valueOf(source.getInt64(BinlogSourceInfo.SERVER_ID_KEY))).isEqualTo(primary.get("server_id"));
            assertThat(connectedServer()).isEqualTo(replica);

            proxy.retarget(primaryHost, primaryPort);
            proxy.dropConnections();
            awaitConnectedServer(primary);
            assertThat(numberOfConnectedServerChanges()).isEqualTo(2);
        }
    }

    private void startThrough(DatabaseTcpProxy proxy) {
        // A user that exists on the primary and on the replica
        final Configuration config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.HOSTNAME, proxy.getHostname())
                .with(BinlogConnectorConfig.PORT, proxy.getPort())
                .with(BinlogConnectorConfig.USER, "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.KEEP_ALIVE_INTERVAL_MS, KEEP_ALIVE_INTERVAL_MS)
                .build();
        start(getConnectorClass(), config);
        assertConnectorIsRunning();
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());
    }

    private Map<String, String> serverOf(BinlogTestConnection connection) throws SQLException {
        try (connection) {
            return Map.of("server_id", connection.queryAndMap("SELECT @@server_id", rs -> rs.next() ? rs.getString(1) : null));
        }
    }

    private void awaitConnectedServer(Map<String, String> server) {
        Awaitility.await()
                .alias("streaming metrics to report server " + server)
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(200))
                // The metrics are registered again when the task restarts after a retriable error
                .ignoreExceptions()
                .until(() -> server.equals(connectedServer()));
    }

    private Map<String, String> connectedServer() {
        final TabularData data = MetricsHelper.getStreamingMetric(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace(),
                "ConnectedServer");
        final Map<String, String> server = new HashMap<>();
        for (Object row : data.values()) {
            final CompositeData entry = (CompositeData) row;
            server.put((String) entry.get("key"), (String) entry.get("value"));
        }
        return server;
    }

    private long numberOfConnectedServerChanges() {
        return MetricsHelper.getStreamingMetric(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace(),
                "NumberOfConnectedServerChanges");
    }
}

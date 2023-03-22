/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter.e2e.source;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import com.github.dockerjava.api.command.LogContainerCmd;

import io.debezium.connector.jdbc.util.RandomTableNameGenerator;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;

/**
 * A test parameter object that represents the source database and containers in a JDBC end-to-end test pipeline.
 *
 * @author Chris Cranford
 */
public class Source implements AutoCloseable {

    private static final AtomicInteger sourceId = new AtomicInteger();

    // SQL Server
    private static final String ENABLE_TABLE_CDC = "IF EXISTS(select 1 from sys.tables where name = '#' AND is_tracked_by_cdc=0)\n"
            + "EXEC sys.sp_cdc_enable_table @source_schema = N'%', @source_name = N'#', @role_name = NULL, @supports_net_changes = 0";

    private final Integer id;
    private final SourceType type;
    private final JdbcDatabaseContainer<?> database;
    private final KafkaContainer kafka;
    private final DebeziumContainer connect;
    private final SourceConnectorOptions options;
    private final RandomTableNameGenerator tableNameGenerator;

    // Lazily opened and closed
    private Connection connection;

    public Source(SourceType type, JdbcDatabaseContainer<?> database, KafkaContainer kafka, DebeziumContainer connect,
                  SourceConnectorOptions options, RandomTableNameGenerator tableGenerator) {
        this.type = type;
        this.id = sourceId.getAndIncrement();
        this.database = database;
        this.kafka = kafka;
        this.connect = connect;
        this.options = options;
        this.tableNameGenerator = tableGenerator;
    }

    public SourceType getType() {
        return type;
    }

    public KafkaContainer getKafka() {
        return kafka;
    }

    public String getUsername() {
        return database.getUsername();
    }

    public String getPassword() {
        return database.getPassword();
    }

    @SuppressWarnings("unused")
    public int getPort() {
        return database.getFirstMappedPort();
    }

    public SourceConnectorOptions getOptions() {
        return options;
    }

    public String getSourceConnectorName() {
        return "jdbc-source-" + id;
    }

    public String randomTableName() {
        return randomObjectName();
    }

    public String randomObjectName() {
        return tableNameGenerator.randomName(12);
    }

    public void registerSourceConnector(ConnectorConfiguration config) {
        waitUntilStreamingStarted(() -> connect.registerConnector(getSourceConnectorName(), config));
    }

    protected void waitUntilStreamingStarted(Runnable callback) {
        waitUntil("Starting streaming", callback);
    }

    public void waitUntilDeleted() {
        Awaitility.await("Source connector deleted")
                .atMost(60, TimeUnit.SECONDS)
                .until(() -> {
                    try {
                        return connect.getConnectorState(getSourceConnectorName()).equals(Connector.State.UNASSIGNED);
                    }
                    catch (IllegalStateException e) {
                        if (e.getMessage().contains("No status found for connector jdbc-source")) {
                            return true;
                        }
                        throw e;
                    }
                });
    }

    @SuppressWarnings("SameParameterValue")
    private void waitUntil(String message, Runnable doBeforeWait) {
        final WaitingConsumer wait = new WaitingConsumer();

        FrameConsumerResultCallback callback = new FrameConsumerResultCallback();
        callback.addConsumer(OutputFrame.OutputType.STDOUT, wait);

        try (LogContainerCmd command = connect.getDockerClient().logContainerCmd(connect.getContainerId())) {
            command.withFollowStream(true).withTail(0).withStdOut(true).exec(callback);
            if (doBeforeWait != null) {
                try {
                    doBeforeWait.run();
                }
                catch (Exception e) {
                    throw new IllegalStateException("WaitUntil callback failed", e);
                }
            }
            try {
                wait.waitUntil(f -> f.getUtf8String().contains(message), 20, TimeUnit.SECONDS);
            }
            catch (TimeoutException e) {
                throw new IllegalStateException("Failed to wait for '" + message + "'", e);
            }
        }
    }

    public void execute(String statement) throws Exception {
        try (Statement st = getConnection().createStatement()) {
            st.execute(statement);
        }
        if (!getConnection().getAutoCommit()) {
            getConnection().commit();
        }
    }

    public void streamTable(String tableName) throws Exception {
        if (SourceType.SQLSERVER == type) {
            execute(ENABLE_TABLE_CDC.replace("#", tableName).replace("%", "dbo"));
        }
        else if (SourceType.ORACLE == type) {
            execute("ALTER TABLE " + tableName + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        }
    }

    private Connection getConnection() throws SQLException {
        if (connection == null) {
            connection = database.createConnection("");
            if (SourceType.SQLSERVER == type) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("USE testDB");
                }
            }
        }
        return connection;
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }
}

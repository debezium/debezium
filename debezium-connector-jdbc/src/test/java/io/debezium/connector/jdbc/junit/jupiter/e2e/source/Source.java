/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter.e2e.source;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
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

import io.debezium.connector.jdbc.junit.jupiter.JdbcConnectionProvider;
import io.debezium.connector.jdbc.util.RandomTableNameGenerator;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;

/**
 * A test parameter object that represents the source database and containers in a JDBC end-to-end test pipeline.
 *
 * @author Chris Cranford
 */
public class Source extends JdbcConnectionProvider {

    private static final AtomicInteger sourceId = new AtomicInteger();

    // SQL Server
    private static final String ENABLE_TABLE_CDC = "IF EXISTS(select 1 from sys.tables where name = '#' AND is_tracked_by_cdc=0)\n"
            + "EXEC sys.sp_cdc_enable_table @source_schema = N'%', @source_name = N'#', @role_name = NULL, @supports_net_changes = 0";

    private final Integer id;
    private final SourceType type;
    private final KafkaContainer kafka;
    private final DebeziumContainer connect;
    private final SourceConnectorOptions options;
    private final RandomTableNameGenerator tableNameGenerator;

    public Source(SourceType type, JdbcDatabaseContainer<?> database, KafkaContainer kafka, DebeziumContainer connect,
                  SourceConnectorOptions options, RandomTableNameGenerator tableGenerator) {
        super(database, new SourceConnectionInitializer(type));
        this.type = type;
        this.id = sourceId.getAndIncrement();
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

    @SuppressWarnings("unused")
    public int getPort() {
        return getContainer().getFirstMappedPort();
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

        try (FrameConsumerResultCallback callback = new FrameConsumerResultCallback()) {
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
        catch (IOException e) {
            throw new RuntimeException("Wait failed for message '" + message + "'", e);
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

    public void queryContainerTable(String tableName) throws Exception {
        List<String> commands = new ArrayList<>();
        commands.add("docker");
        commands.add("exec");
        commands.add("-i");
        commands.add("--tty=false");
        commands.add(getContainerName());

        switch (getType()) {
            case MYSQL:
                commands.add("mysql");
                commands.add("--user=" + getUsername());
                commands.add("--password=" + getPassword());
                commands.add("test");
                commands.add("-e");
                commands.add("SELECT * FROM " + tableName);
                break;
            case POSTGRES:
                commands.add("psql");
                commands.add("-U");
                commands.add(getUsername());
                commands.add("-w");
                commands.add("test");
                commands.add("-c");
                commands.add("show time zone; select * from public." + tableName);
                break;
            case ORACLE:
                commands.add("bash");
                commands.add("-c");
                commands.add("echo \"select * from " + tableName + ";\" | sqlplus debezium/dbz@ORCLPDB1");
                break;
            case SQLSERVER:
                commands.add("/opt/mssql-tools/bin/sqlcmd");
                commands.add("-U");
                commands.add(getUsername());
                commands.add("-P");
                commands.add(getPassword());
                commands.add("-d");
                commands.add("testDB");
                commands.add("-Q");
                commands.add("select * from " + tableName + ";");
                break;
        }
        queryContainer("Source", commands);
    }

    private static class SourceConnectionInitializer implements ConnectionInitializer {

        private final SourceType type;

        SourceConnectionInitializer(SourceType type) {
            this.type = type;
        }

        @Override
        public void initialize(Connection connection) throws SQLException {
            if (SourceType.SQLSERVER.is(type)) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("USE testDB");
                }
            }
        }
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.perf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipTestRule;

public abstract class AbstractPerformanceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPerformanceTest.class);

    private static final String TEST_OUTPUT = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vivamus sed ante vitae justo dictum ultrices. Phasellus non enim a ligula fringilla mattis sit amet placerat sem. Nulla pharetra est sem, nec imperdiet sem molestie sed. Pellentesque tempor tempus lacus, sit amet blandit sapien consequat non. Nulla accumsan sit amet felis quis laoreet. Etiam consectetur tempus est, molestie semper lorem eleifend vitae. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Mauris convallis lobortis vestibulum. Etiam convallis purus eget urna accumsan egestas. Integer semper cursus tellus ut venenatis. Nunc euismod, felis ultricies mollis elementum, lectus lacus ultrices ligula, vel eleifend massa lacus id mi. In venenatis, orci vitae luctus venenatis, diam ex suscipit lacus, eu hendrerit lectus elit quis lectus. Proin finibus lacus vitae quam blandit, et placerat sapien suscipit. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Morbi vestibulum ex quis velit eleifend, ac non.";

    private static final String SCENARIO_BASIC = "basic";
    private static final String RECORDING_STREAMING = "streaming";
    private static final String RECORDING_SNAPSHOT = "snapshot";

    @Rule
    public TestRule skipTestRule = new SkipTestRule();

    private Instant capturingStart;
    private TestConfiguration config;
    private DeploymentController deployment;

    protected abstract String connectorType();

    protected abstract JdbcConnection databaseConnection();

    protected abstract void createSchema(final JdbcConnection db, final String schemaName) throws SQLException;

    protected TestConfiguration config() {
        return config;
    }

    @Before
    public void init() throws Exception {
        config = new TestConfiguration(connectorType());
        deployment = new DeploymentController(config, connectorType());
    }

    @After
    public void cleanUp() throws Exception {
        deployment.unregisterConnector();
    }

    @Test
    public void testSnapshot() throws Exception {
        populateSchema();
        populateData();
        deployment.startRecording(RECORDING_SNAPSHOT);
        deployment.registerConnector(SCENARIO_BASIC);
        capturingStart = deployment.waitForSnapshotToStart();
        deployment.waitForCaptureCompletion();
        deployment.stopRecording();
        logResults(SCENARIO_BASIC, RECORDING_SNAPSHOT);
    }

    @Test
    public void testStreaming() throws Exception {
        populateSchema();
        deployment.registerConnector(SCENARIO_BASIC);
        deployment.waitForStreamingToStart();
        deployment.unregisterConnector();
        populateData();
        deployment.startRecording(RECORDING_STREAMING);
        deployment.registerConnector(SCENARIO_BASIC);
        capturingStart = deployment.waitForStreamingToStart();
        deployment.waitForCaptureCompletion();
        deployment.stopRecording();
        logResults(SCENARIO_BASIC, RECORDING_STREAMING);
    }

    private void logResults(String scenario, String recording) throws IOException {
        final Duration runtime = Duration.between(capturingStart, deployment.lastDataMessageTimestamp());
        final long speed = (long) (((double) config().totalMessageCount() / runtime.toMillis()) * 1000);
        LOGGER.info("{} messages processed in {} with speed {} msgs/sec", config().totalMessageCount(), runtime,
                speed);
        writeTestReport(scenario, recording, runtime, speed);
    }

    private String buildColumnList() {
        final List<String> columns = new ArrayList<>();
        final String columnType = " VARCHAR(" + config().columnLength() + ")";
        for (int columnNo = 0; columnNo < config().columnsCount(); columnNo++) {
            final String columnName = "value" + columnNo;
            columns.add(columnName + columnType);
        }
        return columns.stream().collect(Collectors.joining(" ,"));
    }

    private String buildColumnValues() {
        final List<String> values = new ArrayList<>();
        for (int columnNo = 0; columnNo < config().columnsCount(); columnNo++) {
            values.add("'" + TEST_OUTPUT.substring(config().columnLength()) + "'");
        }
        return values.stream().collect(Collectors.joining(" ,"));
    }

    protected void populateSchema() throws SQLException {
        LOGGER.info("Populating database schema");
        final String columnList = buildColumnList();

        try (final JdbcConnection db = databaseConnection()) {
            db.connect();
            for (int schemaNo = 0; schemaNo < config().schemasCount(); schemaNo++) {
                final String schemaName = "noncap" + schemaNo;
                createSchema(db, schemaName);
                for (int tableNo = 0; tableNo < config().tablesCount(); tableNo++) {
                    final String tableName = "ntable" + tableNo;
                    db.execute(String.format("CREATE TABLE %s.%s (id INT PRIMARY KEY, %s)", schemaName, tableName, columnList));
                }
            }
            db.execute(
                    String.format("DROP TABLE IF EXISTS %s.%s", config().capturedSchemaName(), config().capturedTableName()),
                    String.format("DROP TABLE IF EXISTS %s.%s", config().capturedSchemaName(), config().finishTableName()),
                    String.format("CREATE TABLE %s.%s (id INT PRIMARY KEY, %s)", config().capturedSchemaName(), config().capturedTableName(), columnList),
                    String.format("CREATE TABLE %s.%s (id INT PRIMARY KEY)", config().capturedSchemaName(), config().finishTableName()));
        }
        LOGGER.info("Database schema populated");
    }

    protected void populateData() throws SQLException {
        LOGGER.info("Populating database data");
        final String columnValues = buildColumnValues();

        try (final JdbcConnection db = databaseConnection()) {
            db.connect();
            db.setAutoCommit(false);
            for (int txNo = 0; txNo < config().populateTxCount(); txNo++) {
                for (int recordNo = 0; recordNo < config().populateChangesPerTxCount(); recordNo++) {
                    db.executeWithoutCommitting(
                            String.format("INSERT INTO %s.%s VALUES(%s, %s)",
                                    config.capturedSchemaName(),
                                    config.capturedTableName(),
                                    txNo * 1_000 + recordNo,
                                    columnValues));
                }
                db.commit();
                LOGGER.debug("Transaction no {} of {} committed", txNo, config().populateTxCount());
            }
            db.execute(
                    String.format("INSERT INTO %s.%s VALUES(0)",
                            config.capturedSchemaName(),
                            config.finishTableName()));
            db.close();
        }
        LOGGER.info("Database data populated");
    }

    private void writeTestReport(String scenario, String recording, Duration runtime, double speed) throws IOException {
        final String fileHeader = "connector;scenario;recording;schemas;tables;columns;column_size;transactions;changes_per_transaction;messages;runtime;speed"
                + System.lineSeparator();

        final Path reportFile = Path.of(config().testReportFile());
        if (!Files.exists(reportFile)) {
            Files.createFile(reportFile);
            Files.writeString(reportFile, fileHeader);
        }

        final StringBuilder testResults = new StringBuilder();
        testResults
                .append(connectorType()).append(';')
                .append(scenario).append(';')
                .append(recording).append(';')
                .append(config().schemasCount()).append(';')
                .append(config().tablesCount()).append(';')
                .append(config().columnsCount()).append(';')
                .append(config().columnLength()).append(';')
                .append(config().populateTxCount()).append(';')
                .append(config().populateChangesPerTxCount()).append(';')
                .append(config().totalMessageCount()).append(';')
                .append(runtime.getSeconds()).append(';')
                .append(speed).append(System.lineSeparator());
        Files.writeString(reportFile, testResults.toString(), StandardOpenOption.APPEND);
    }
}

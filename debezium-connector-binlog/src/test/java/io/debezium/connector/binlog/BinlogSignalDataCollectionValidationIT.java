/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;

/**
 * Starts the connector with {@code signal.data.collection} validation enabled and checks whether it starts or fails.
 */
public abstract class BinlogSignalDataCollectionValidationIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-signal-validation.txt").toAbsolutePath();

    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("signal_validation", "incremental_snapshot-test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

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
    public void shouldStartWithCanonicalName() throws Exception {
        assertStarts(DATABASE.qualifiedTableName("debezium_signal"), Function.identity());
    }

    @Test
    public void shouldFailForOnePartName() {
        assertFails("debezium_signal", Function.identity(), "not 'debezium_signal'.");
    }

    @Test
    public void shouldFailForWrongDatabase() {
        assertFails("no_such_db.debezium_signal", Function.identity(),
                "Signal data collection 'no_such_db.debezium_signal' does not exist or is not visible to the connector user.");
    }

    @Test
    public void shouldIgnoreLookAlikeTables() throws Exception {
        executeStatements(DATABASE.getDatabaseName(), "CREATE TABLE debeziumXsignal (a INT, b INT)", "CREATE TABLE sigXnal (id VARCHAR(64))");

        assertStarts(DATABASE.qualifiedTableName("debezium_signal"), Function.identity());
        stopConnector();

        String lookAlike = DATABASE.qualifiedTableName("sig_nal");
        assertFails(lookAlike, Function.identity(), "Signal data collection '" + lookAlike + "' does not exist or is not visible to the connector user.");
    }

    @Test
    public void shouldFailForWrongColumnCountUnlessFiltered() throws Exception {
        executeStatements(DATABASE.getDatabaseName(), "CREATE TABLE sig_4cols (id VARCHAR(64), type VARCHAR(32), data VARCHAR(2048), extra INT)");
        String signalDataCollection = DATABASE.qualifiedTableName("sig_4cols");

        assertFails(signalDataCollection, Function.identity(),
                "Signal data collection '" + signalDataCollection + "' has 4 columns; exactly 3 are required.");
        stopConnector();

        assertStarts(signalDataCollection, config -> config.with(BinlogConnectorConfig.COLUMN_EXCLUDE_LIST, signalDataCollection + ".extra"));
    }

    @Test
    @FixFor("debezium/dbz#3041")
    public void shouldFindTableWithNoBackslashEscapes() throws Exception {
        assertStarts(DATABASE.qualifiedTableName("debezium_signal"),
                config -> config.with(BinlogConnectorConfig.ON_CONNECT_STATEMENTS, "SET SESSION sql_mode = CONCAT(@@sql_mode, ',NO_BACKSLASH_ESCAPES')"));
    }

    @Test
    public void shouldFindQuotedNameWithSpace() throws Exception {
        executeStatements(DATABASE.getDatabaseName(), "CREATE TABLE `sig with space` (id VARCHAR(64), type VARCHAR(32), data VARCHAR(2048))");

        assertStarts(DATABASE.getDatabaseName() + ".`sig with space`", Function.identity());
    }

    private Configuration config(String signalDataCollection, Function<Configuration.Builder, Configuration.Builder> custom) {
        return custom.apply(DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION, signalDataCollection)
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION_VALIDATION_ENABLED, true))
                .build();
    }

    private void assertStarts(String signalDataCollection, Function<Configuration.Builder, Configuration.Builder> custom) throws InterruptedException {
        start(getConnectorClass(), config(signalDataCollection, custom));
        assertConnectorIsRunning();
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
    }

    private void assertFails(String signalDataCollection, Function<Configuration.Builder, Configuration.Builder> custom, String message) {
        AtomicReference<Throwable> error = new AtomicReference<>();
        start(getConnectorClass(), config(signalDataCollection, custom), (success, msg, throwable) -> error.set(throwable));
        waitForEngineShutdown();
        assertConnectorNotRunning();
        assertThat(error.get()).hasStackTraceContaining(message);
    }
}

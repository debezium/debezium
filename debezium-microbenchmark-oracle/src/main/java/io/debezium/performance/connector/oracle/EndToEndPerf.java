/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.connector.oracle;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.ConnectorAdapter;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningStrategy;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.embedded.EmbeddedEngineConfig;
import io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.util.IoUtil;

/**
 * An end-to-end performance benchmark of the Oracle connector using LogMiner.
 *
 * This benchmark is designed to generate a given number of events for a specified table using
 * a connector configuration based on the parameterized state {@link EndToEndState}.  The
 * benchmarks test the following:
 *
 * <ul>
 *     <li>A given set of expected DML events</li>
 *     <li>Parser implementation, legacy and fast versions</li>
 *     <li>Mining Strategy, using redo logs or database online dictionary</li>
 * </ul>
 *
 * The benchmark output is a matrix of all these parameterized values and the total time that
 * is spent executing each end-to-end benchmark for the given tuple of parameters.
 *
 * @author Chris Cranford
 */
public class EndToEndPerf {

    @State(Scope.Thread)
    public static class EndToEndState {

        private DebeziumEngine<SourceRecord> engine;
        private ExecutorService executors;
        private BlockingQueue<SourceRecord> consumedLines;

        @Param({ "1000", "5000", "10000" })
        public int dmlEvents;

        @Param({ "redo_log_catalog", "online_catalog" })
        public String miningStrategy;

        @Setup(Level.Iteration)
        public void doSetup() {
            consumedLines = new ArrayBlockingQueue<>(100);

            OracleConnection connection = getTestConnection();
            try {
                connection.execute("DROP TABLE debezium.test");
            }
            catch (SQLException e) {
                // ignored
            }
            try {
                connection.execute("CREATE TABLE debezium.test (id numeric(9,0) primary key, name varchar2(50))");
                connection.execute("ALTER TABLE debezium.test add supplemental log data (all) columns");
            }
            catch (SQLException e) {
                throw new RuntimeException("Failed to create table", e);
            }

            delete("offsets.txt");
            delete("history.txt");

            Configuration connectorConfig = defaultConnectorConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.TEST")
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, LogMiningStrategy.parse(miningStrategy))
                    .build();

            Configuration config = Configuration.copy(connectorConfig)
                    .with(EmbeddedEngineConfig.ENGINE_NAME, "benchmark")
                    .with(EmbeddedEngineConfig.CONNECTOR_CLASS, OracleConnector.class)
                    .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, getPath("offsets.txt").toAbsolutePath())
                    .with(EmbeddedEngineConfig.OFFSET_FLUSH_INTERVAL_MS, 0)
                    .build();

            Consumer<SourceRecord> recordArrivedListener = this::processRecord;
            this.engine = new ConvertingAsyncEngineBuilderFactory()
                    .builder((KeyValueHeaderChangeEventFormat) null)
                    .using(config.asProperties())
                    .notifying((record) -> {
                        if (Thread.currentThread().isInterrupted()) {
                            return;
                        }
                        while (!consumedLines.offer((SourceRecord) record)) {
                            if (Thread.currentThread().isInterrupted()) {
                                return;
                            }
                        }
                        recordArrivedListener.accept((SourceRecord) record);
                    })
                    .using(this.getClass().getClassLoader())
                    .build();

            executors = Executors.newFixedThreadPool(1);
            executors.execute(engine);

            // wait for the connector to transition to streaming
            waitForStreamingToStart();

            // insert records & commit as one transaction
            try {
                for (int i = 0; i < dmlEvents; ++i) {
                    StringBuilder dml = new StringBuilder("INSERT INTO debezium.test (id, name) values (");
                    dml.append(i).append(",").append("'Test").append(i).append("')");
                    connection.executeWithoutCommitting(dml.toString());
                }
                connection.execute("COMMIT");
            }
            catch (SQLException e) {
                throw new RuntimeException("Failed to insert data set", e);
            }
        }

        @TearDown(Level.Iteration)
        public void doCleanup() throws IOException {
            try {
                if (engine != null) {
                    engine.close();
                }
                if (executors != null) {
                    executors.shutdownNow();
                    try {
                        while (!executors.awaitTermination(60, TimeUnit.SECONDS)) {
                            // wait
                        }
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            finally {
                engine = null;
                executors = null;
            }
        }

        private static final String HOST = "localhost";
        private static final String USER = "c##dbzuser";
        private static final String PASSWORD = "dbz";
        private static final String DATABASE_CDB = "ORCLCDB";
        private static final String SERVER_NAME = "server1";
        private static final String SCHEMA_USER = "debezium";
        private static final String SCHEMA_USER_PASSWORD = "dbz";
        private static final String DATABASE = "ORCLPDB1";

        private JdbcConfiguration defaultJdbcConfig() {
            return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                    .withDefault(JdbcConfiguration.HOSTNAME, HOST)
                    .withDefault(JdbcConfiguration.PORT, 1521)
                    .withDefault(JdbcConfiguration.USER, USER)
                    .withDefault(JdbcConfiguration.PASSWORD, PASSWORD)
                    .withDefault(JdbcConfiguration.DATABASE, DATABASE_CDB)
                    .build();
        }

        private JdbcConfiguration testJdbcConfig() {
            return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                    .withDefault(JdbcConfiguration.HOSTNAME, HOST)
                    .withDefault(JdbcConfiguration.PORT, 1521)
                    .withDefault(JdbcConfiguration.USER, SCHEMA_USER)
                    .withDefault(JdbcConfiguration.PASSWORD, SCHEMA_USER_PASSWORD)
                    .withDefault(JdbcConfiguration.DATABASE, DATABASE)
                    .build();
        }

        private Configuration.Builder defaultConnectorConfig() {
            JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();

            Configuration.Builder builder = Configuration.create();
            jdbcConfiguration.forEach((f, v) -> builder.with(ConfigurationNames.DATABASE_CONFIG_PREFIX + f, v));

            return builder.with(CommonConnectorConfig.TOPIC_PREFIX, SERVER_NAME)
                    .with(OracleConnectorConfig.PDB_NAME, "ORCLPDB1")
                    .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                    .with(OracleConnectorConfig.CONNECTOR_ADAPTER, ConnectorAdapter.LOG_MINER)
                    .with(OracleConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                    .with(FileSchemaHistory.FILE_PATH, getPath("history.txt"));
        }

        private Configuration.Builder testConfig() {
            JdbcConfiguration jdbcConfiguration = testJdbcConfig();
            Configuration.Builder builder = Configuration.create();
            jdbcConfiguration.forEach((f, v) -> builder.with(ConfigurationNames.DATABASE_CONFIG_PREFIX + f, v));
            return builder;
        }

        private OracleConnection getTestConnection() {
            OracleConnection connection = new OracleConnection(testJdbcConfig());
            try {
                connection.setAutoCommit(false);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }

            String pdbName = new OracleConnectorConfig(testConfig().build()).getPdbName();
            if (pdbName != null) {
                connection.setSessionToPdb(pdbName);
            }

            return connection;
        }

        private void waitForStreamingToStart() {
            Awaitility.await()
                    .alias("Streaming was not started on time")
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .atMost(30, TimeUnit.SECONDS)
                    .ignoreException(InstanceNotFoundException.class)
                    .until(() -> {
                        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
                        try {
                            return (boolean) server.getAttribute(getName(), "Connected");
                        }
                        catch (JMException ignored) {
                        }
                        return false;
                    });
        }

        private ObjectName getName() throws MalformedObjectNameException {
            return new ObjectName("debezium.oracle:type=connector-metrics,context=streaming,server=" + SERVER_NAME);
        }

        private Path getPath(String relativePath) {
            return Paths.get(resolveDataDir(), relativePath).toAbsolutePath();
        }

        private void delete(String relativePath) {
            Path history = getPath(relativePath).toAbsolutePath();
            if (history != null) {
                history = history.toAbsolutePath();
                if (inTestDataDir(history)) {
                    try {
                        IoUtil.delete(history);
                    }
                    catch (IOException e) {
                        // ignored
                    }
                }
            }
        }

        private boolean inTestDataDir(Path path) {
            Path target = FileSystems.getDefault().getPath(resolveDataDir()).toAbsolutePath();
            return path.toAbsolutePath().startsWith(target);
        }

        private String resolveDataDir() {
            String value = System.getProperty("dbz.test.data.dir");
            if (value != null && (value = value.trim()).length() > 0) {
                return value;
            }

            value = System.getenv("DBZ_TEST_DATA_DIR");
            if (value != null && (value = value.trim()).length() > 0) {
                return value;
            }

            return "target/data";
        }

        private void processRecord(SourceRecord record) {
            try {
                consumedLines.put(record);
            }
            catch (InterruptedException e) {
                throw new RuntimeException("Failed to insert record into queue", e);
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 0)
    @Measurement(iterations = 1, time = 1)
    public void capture(EndToEndState state) {
        List<SourceRecord> records = new ArrayList<>();
        while (records.size() < state.dmlEvents) {
            List<SourceRecord> temp = new ArrayList<>();
            state.consumedLines.drainTo(temp);
            records.addAll(temp);
        }
    }
}

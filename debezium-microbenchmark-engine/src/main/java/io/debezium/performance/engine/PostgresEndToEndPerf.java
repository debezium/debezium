/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.engine;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
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
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.embedded.EmbeddedEngineConfig;
import io.debezium.embedded.async.AsyncEngineConfig;
import io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.KeyValueChangeEventFormat;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.util.IoUtil;

/**
 * Basic end-to-end comparison between {@link io.debezium.embedded.EmbeddedEngine} and {@link io.debezium.embedded.async.AsyncEmbeddedEngine}.
 * Heavily inspired by JMH benchmark {@code EndToEndPerf} for Oracle connector and reusing parts of its code,
 * this test pre-creates specified number of records in the Postgres database and measure how long it takes
 * to the engine to process them and store in an in-memory queue.
 */
public class PostgresEndToEndPerf {

    private static final String HOST = "localhost";
    private static final int PORT = 5432;
    private static final String USER = "postgres";
    private static final String PASSWORD = "postgres";
    private static final String DATABASE = "postgres";
    private static final String SERVER_NAME = "server1";
    private static final String BASE_TABLE_NAME = "inventory.test";
    private static final KeyValueChangeEventFormat KV_EVENT_FORMAT = KeyValueChangeEventFormat.of(Json.class, Json.class);

    @State(Scope.Thread)
    public abstract static class DebeziumEndToEndPerfTest {

        private DebeziumEngine engine;
        private ExecutorService executors;
        protected BlockingQueue<EmbeddedEngineChangeEvent> consumedLines;
        protected AtomicInteger count = new AtomicInteger(0);

        @Param({ "100000", "1000000" })
        public int eventCount;

        public abstract String getBaseTableName();

        public abstract DebeziumEngine createEngine();

        @Setup(Level.Iteration)
        public void doSetup() {
            final String tableName = getBaseTableName() + "_" + eventCount;

            // delete offset and re-create table if it already exists
            delete("offsets.txt");
            recreateTable(tableName);
            consumedLines = new ArrayBlockingQueue<>(eventCount);

            // create engine and start it
            this.engine = createEngine();
            executors = Executors.newFixedThreadPool(1);
            executors.execute(engine);

            // wait for the connector to transition to streaming
            waitForStreamingToStart();

            // insert records & commit as one transaction
            createDmlEvents(tableName, eventCount);
        }

        @TearDown(Level.Iteration)
        public void doCleanup() throws Exception {
            try {
                if (engine != null) {
                    engine.close();
                }
                if (executors != null) {
                    executors.shutdown();
                    try {
                        executors.awaitTermination(CommonConnectorConfig.DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            finally {
                executors.shutdownNow();
                engine = null;
                executors = null;
            }
        }
    }

    private static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.HOSTNAME, HOST)
                .withDefault(JdbcConfiguration.PORT, PORT)
                .withDefault(JdbcConfiguration.USER, USER)
                .withDefault(JdbcConfiguration.PASSWORD, PASSWORD)
                .withDefault(JdbcConfiguration.DATABASE, DATABASE)
                .build();
    }

    private static Configuration.Builder defaultConnectorConfig() {
        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();
        jdbcConfiguration.forEach((f, v) -> builder.with(ConfigurationNames.DATABASE_CONFIG_PREFIX + f, v));

        return builder.with(CommonConnectorConfig.TOPIC_PREFIX, SERVER_NAME)
                .with(PostgresConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, true)
                .with(EmbeddedEngineConfig.ENGINE_NAME, "benchmark")
                .with(EmbeddedEngineConfig.CONNECTOR_CLASS, PostgresConnector.class)
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, getPath("offsets.txt").toAbsolutePath())
                .with(EmbeddedEngineConfig.OFFSET_FLUSH_INTERVAL_MS, 0);
    }

    private static Properties addSmtConfig(Configuration config) {
        final Properties configProps = config.asProperties();
        configProps.setProperty("transforms", "replace");
        configProps.setProperty("transforms.replace.type", "org.apache.kafka.connect.transforms.ReplaceField$Value");
        configProps.setProperty("transforms.replace.renames", "name:transformed_name");
        configProps.setProperty("transforms.replace.exclude", "id");
        return configProps;
    }

    private static Consumer<ChangeEvent<String, String>> getRecordConsumer(BlockingQueue<EmbeddedEngineChangeEvent> consumedLines) {
        return record -> {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            while (!consumedLines.offer((EmbeddedEngineChangeEvent) record)) {
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
            }
        };
    }

    private static void recreateTable(String tableName) {
        PostgresConnection connection = getTestConnection();
        try {
            connection.execute("DROP TABLE IF EXISTS " + tableName);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            connection.execute("CREATE TABLE " + tableName + " (id numeric(9,0) primary key, name varchar(50))");
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to create table", e);
        }
    }

    private static void createDmlEvents(String tableName, int eventCount) {
        PostgresConnection connection = getTestConnection();
        try {
            for (int i = 0; i < eventCount; i++) {
                StringBuilder dml = new StringBuilder("INSERT INTO " + tableName + " (id, name) values (");
                dml.append(i).append(",").append("'Test").append(i).append("')");
                connection.executeWithoutCommitting(dml.toString());
            }
            connection.commit();
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to insert data set", e);
        }
    }

    private static PostgresConnection getTestConnection() {
        PostgresConnection connection = new PostgresConnection(defaultJdbcConfig(), "test_connection");
        try {
            connection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return connection;
    }

    private static void waitForStreamingToStart() {
        Awaitility.await()
                .alias("Streaming was not started on time")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(30, TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> {
                    final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
                    try {
                        return (boolean) server.getAttribute(getMbeanName(), "Connected");
                    }
                    catch (JMException ignored) {
                    }
                    return false;
                });
    }

    private static ObjectName getMbeanName() throws MalformedObjectNameException {
        return new ObjectName("debezium.postgres:type=connector-metrics,context=streaming,server=" + SERVER_NAME);
    }

    private static Path getPath(String relativePath) {
        return Paths.get(resolveDataDir(), relativePath).toAbsolutePath();
    }

    private static void delete(String relativePath) {
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

    private static boolean inTestDataDir(Path path) {
        Path target = FileSystems.getDefault().getPath(resolveDataDir()).toAbsolutePath();
        return path.toAbsolutePath().startsWith(target);
    }

    private static String resolveDataDir() {
        String value = System.getProperty("dbz.test.data.dir");
        if (value != null && (value = value.trim()).length() > 0) {
            return value;
        }

        value = System.getenv("DBZ_TEST_DATA_DIR");
        if (value != null && (value = value.trim()).length() > 0) {
            return value;
        }

        return "/tmp";
    }

    @State(Scope.Thread)
    public static class AsyncEngineEndToEndPerfTest extends DebeziumEndToEndPerfTest {
        @Param({ "1", "2", "4", "8", "16" })
        public int threadCount;

        @Param({ "ORDERED", "UNORDERED" })
        public String processingOrder;

        public String getBaseTableName() {
            return BASE_TABLE_NAME + "_async" + "_" + threadCount + "_" + processingOrder;
        }

        public DebeziumEngine createEngine() {
            Configuration config = defaultConnectorConfig()
                    .with(PostgresConnectorConfig.SLOT_NAME, "async_" + eventCount)
                    // .with(EmbeddedEngineConfig.WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_MS, CommonConnectorConfig.EXECUTOR_SHUTDOWN_TIMEOUT_SEC)
                    .with(AsyncEngineConfig.RECORD_PROCESSING_SHUTDOWN_TIMEOUT_MS, 100)
                    .with(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS, 5000)
                    .with(AsyncEngineConfig.RECORD_PROCESSING_THREADS, threadCount)
                    .with(AsyncEngineConfig.RECORD_PROCESSING_ORDER, processingOrder)
                    .build();
            Properties configProps = addSmtConfig(config);

            return new ConvertingAsyncEngineBuilderFactory()
                    // new ConvertingEngineBuilderFactory()
                    .builder(KV_EVENT_FORMAT)
                    .using(configProps)
                    .notifying(getRecordConsumer(consumedLines))
                    .using(this.getClass().getClassLoader())
                    .build();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Measurement(iterations = 1, time = 1)
    public void processRecordsAsyncEngine(AsyncEngineEndToEndPerfTest state) {
        List<EmbeddedEngineChangeEvent> records = new ArrayList<>();
        while (records.size() < state.eventCount) {
            List<EmbeddedEngineChangeEvent> temp = new ArrayList<>();
            state.consumedLines.drainTo(temp);
            records.addAll(temp);
        }
    }
}

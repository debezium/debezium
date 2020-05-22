/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.cassandraunit.utils.CqlOperations;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

import io.debezium.config.Configuration;

/**
 * Base class used to automatically spin up a single-node embedded Cassandra cluster before tests
 * and handle clean up after tests.
 */
public abstract class EmbeddedCassandraConnectorTestBase {
    public static final String TEST_CONNECTOR_NAME = "cassandra-01";
    public static final String TEST_KEYSPACE = "test_keyspace";
    public static final long STARTUP_TIMEOUT_IN_SECONDS = 10;
    public static final String TEST_CASSANDRA_YAML_CONFIG = "cassandra-unit.yaml";
    public static final String TEST_CASSANDRA_HOSTS = "127.0.0.1";
    public static final int TEST_CASSANDRA_PORT = 9042;
    public static final String TEST_KAFKA_SERVERS = "localhost:9092";
    public static final String TEST_SCHEMA_REGISTRY_URL = "http://localhost:8081";
    public static final String TEST_KAFKA_TOPIC_PREFIX = "test_topic";

    @BeforeClass
    public static void setUpClass() throws Exception {
        startEmbeddedCassandra();
        createTestKeyspace();
    }

    @AfterClass
    public static void tearDownClass() {
        destroyTestKeyspace();
        stopEmbeddedCassandra();
    }

    /**
     * Truncate data in all tables in TEST_KEYSPACE, but do not drop the tables
     */
    protected static void truncateTestKeyspaceTableData() {
        EmbeddedCassandraServerHelper.cleanDataEmbeddedCassandra(TEST_KEYSPACE);
    }

    /**
     * Drop all tables in TEST_KEYSPACE
     */
    protected static void deleteTestKeyspaceTables() {
        Session session = EmbeddedCassandraServerHelper.getSession();
        Cluster cluster = EmbeddedCassandraServerHelper.getCluster();
        for (TableMetadata tm : cluster.getMetadata().getKeyspace(TEST_KEYSPACE).getTables()) {
            session.execute("DROP TABLE IF EXISTS " + keyspaceTable(tm.getName()));
        }
    }

    /**
     * Generate a task context with default test configs
     */
    protected static CassandraConnectorContext generateTaskContext() throws Exception {
        Properties defaults = generateDefaultConfigMap();
        return new CassandraConnectorContext(new CassandraConnectorConfig(Configuration.from(defaults)));
    }

    /**
     * General a task context with default and custom test configs
     */
    protected static CassandraConnectorContext generateTaskContext(Map<String, Object> configs) throws Exception {
        Properties defaults = generateDefaultConfigMap();
        defaults.putAll(configs);
        return new CassandraConnectorContext(new CassandraConnectorConfig(Configuration.from(defaults)));
    }

    /**
     * Delete all files in offset backing store directory
     */
    protected static void deleteTestOffsets(CassandraConnectorContext context) throws IOException {
        String offsetDirPath = context.getCassandraConnectorConfig().offsetBackingStoreDir();
        File offsetDir = new File(offsetDirPath);
        if (offsetDir.isDirectory()) {
            File[] files = offsetDir.listFiles();
            if (files != null) {
                for (File f : files) {
                    Files.delete(f.toPath());
                }
            }
        }
    }

    /**
     * Return the full name of the test table in the form of <keyspace>.<table>
     */
    protected static String keyspaceTable(String tableName) {
        return TEST_KEYSPACE + "." + tableName;
    }

    /**
     * Generate a commit log file with current timestamp in memory
     */
    protected static File generateCommitLogFile() {
        File cdcDir = new File(DatabaseDescriptor.getCDCLogLocation());
        long ts = System.currentTimeMillis();
        return Paths.get(cdcDir.getAbsolutePath(), "CommitLog-6-" + ts + ".log").toFile();
    }

    /**
     * Generate commit log files in directory
     */
    protected static void populateFakeCommitLogsForDirectory(int numOfFiles, File directory) throws IOException {
        if (directory.exists() && !directory.isDirectory()) {
            throw new IOException(directory + " is not a directory");
        }
        if (!directory.exists() && !directory.mkdir()) {
            throw new IOException("Cannot create directory " + directory);
        }
        clearCommitLogFromDirectory(directory, true);
        long prefix = System.currentTimeMillis();
        for (int i = 0; i < numOfFiles; i++) {
            long ts = prefix + i;
            Path path = Paths.get(directory.getAbsolutePath(), "CommitLog-6-" + ts + ".log");
            boolean success = path.toFile().createNewFile();
            if (!success) {
                throw new IOException("Failed to create new commit log for testing");
            }
        }
    }

    /**
     * Delete all commit log files in directory
     */
    protected static void clearCommitLogFromDirectory(File directory, boolean recursive) throws IOException {
        if (!directory.exists() || !directory.isDirectory()) {
            throw new IOException(directory + " is not a valid directory");
        }

        File[] commitLogs = CommitLogUtil.getCommitLogs(directory);
        for (File commitLog : commitLogs) {
            CommitLogUtil.deleteCommitLog(commitLog);
        }

        if (recursive) {
            File[] directories = directory.listFiles(File::isDirectory);
            if (directories != null) {
                for (File dir : directories) {
                    clearCommitLogFromDirectory(dir, true);
                }
            }
        }
    }

    protected static Properties generateDefaultConfigMap() throws IOException {
        Properties props = new Properties();
        props.put(CassandraConnectorConfig.CONNECTOR_NAME.name(), TEST_CONNECTOR_NAME);
        props.put(CassandraConnectorConfig.CASSANDRA_CONFIG.name(), TEST_CASSANDRA_YAML_CONFIG);
        props.put(CassandraConnectorConfig.KAFKA_TOPIC_PREFIX.name(), TEST_KAFKA_TOPIC_PREFIX);
        props.put(CassandraConnectorConfig.CASSANDRA_HOSTS.name(), TEST_CASSANDRA_HOSTS);
        props.put(CassandraConnectorConfig.CASSANDRA_PORT.name(), String.valueOf(TEST_CASSANDRA_PORT));
        props.put(CassandraConnectorConfig.OFFSET_BACKING_STORE_DIR.name(), Files.createTempDirectory("offset").toString());
        props.put(CassandraConnectorConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TEST_KAFKA_SERVERS);
        props.put(CassandraConnectorConfig.COMMIT_LOG_RELOCATION_DIR.name(), Files.createTempDirectory("cdc_raw_relocation").toString());
        return props;
    }

    private static void startEmbeddedCassandra() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(TEST_CASSANDRA_YAML_CONFIG, Duration.ofSeconds(STARTUP_TIMEOUT_IN_SECONDS).toMillis());
    }

    private static void stopEmbeddedCassandra() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    private static void createTestKeyspace() {
        Session session = EmbeddedCassandraServerHelper.getSession();
        CqlOperations.createKeyspace(session).accept(TEST_KEYSPACE);
    }

    private static void destroyTestKeyspace() {
        Session session = EmbeddedCassandraServerHelper.getSession();
        CqlOperations.dropKeyspace(session).accept(TEST_KEYSPACE);
    }
}

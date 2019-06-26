/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class CassandraConnectorConfigTest {
    @Test
    public void testConfigs() {
        int port = 1234;
        CassandraConnectorConfig config = buildTaskConfig(CassandraConnectorConfig.HTTP_PORT, port);
        assertEquals(port, config.httpPort());

        String cassandraConfig = "cassandra-unit.yaml";
        config = buildTaskConfig(CassandraConnectorConfig.CASSANDRA_CONFIG, cassandraConfig);
        assertEquals(cassandraConfig, config.cassandraConfig());

        String cassandraHosts = "127.0.0.1,127.0.0.2";
        config = buildTaskConfig(CassandraConnectorConfig.CASSANDRA_HOSTS, cassandraHosts);
        assertArrayEquals(cassandraHosts.split(","), config.cassandraHosts());

        int cassandraPort = 9412;
        config = buildTaskConfig(CassandraConnectorConfig.CASSANDRA_PORT, cassandraPort);
        assertEquals(cassandraPort, config.cassandraPort());

        String cassandraUsername = "test_user";
        config = buildTaskConfig(CassandraConnectorConfig.CASSANDRA_USERNAME, cassandraUsername);
        assertEquals(cassandraUsername, config.cassandraUsername());

        String cassandraPassword = "test_pw";
        config = buildTaskConfig(CassandraConnectorConfig.CASSANDRA_PASSWORD, cassandraPassword);
        assertEquals(cassandraPassword, config.cassandraPassword());

        config = buildTaskConfig(CassandraConnectorConfig.CASSANDRA_SSL_ENABLED, true);
        assertTrue(config.cassandraSslEnabled());

        String cassandraSslConfigPath = "/some/path/";
        config = buildTaskConfig(CassandraConnectorConfig.CASSANDRA_SSL_CONFIG_PATH, cassandraSslConfigPath);
        assertEquals(cassandraSslConfigPath, config.cassandraSslConfigPath());

        String kafkaTopicPrefix = "test_prefix";
        config = buildTaskConfig(CassandraConnectorConfig.KAFKA_TOPIC_PREFIX, kafkaTopicPrefix);
        assertEquals(kafkaTopicPrefix, config.kafkaTopicPrefix());

        String kafkaServers = "host1,host2,host3";
        config = buildTaskConfig("kafka.producer.bootstrap.servers", kafkaServers);
        assertEquals(kafkaServers, config.getKafkaConfigs().getProperty("bootstrap.servers"));

        String schemaRegistry = "schema-registry-host";
        config = buildTaskConfig("kafka.producer.schema.registry", schemaRegistry);
        assertEquals(schemaRegistry, config.getKafkaConfigs().getProperty("schema.registry"));

        String offsetBackingStore = "/some/offset/backing/store/";
        config = buildTaskConfig(CassandraConnectorConfig.OFFSET_BACKING_STORE_DIR, offsetBackingStore);
        assertEquals(offsetBackingStore, config.offsetBackingStoreDir());

        int offsetFlushIntervalMs = 1234;
        config = buildTaskConfig(CassandraConnectorConfig.OFFSET_FLUSH_INTERVAL_MS, offsetFlushIntervalMs);
        assertEquals(offsetFlushIntervalMs, config.offsetFlushIntervalMs().toMillis());

        int offsetMaxFlushSize = 200;
        config = buildTaskConfig(CassandraConnectorConfig.MAX_OFFSET_FLUSH_SIZE, offsetMaxFlushSize);
        assertEquals(offsetMaxFlushSize, config.maxOffsetFlushSize());

        int maxQueueSize = 500;
        config = buildTaskConfig(CassandraConnectorConfig.MAX_QUEUE_SIZE, maxQueueSize);
        assertEquals(maxQueueSize, config.maxQueueSize());

        int maxBatchSize = 500;
        config = buildTaskConfig(CassandraConnectorConfig.MAX_BATCH_SIZE, maxBatchSize);
        assertEquals(maxBatchSize, config.maxBatchSize());

        int pollIntervalMs = 500;
        config = buildTaskConfig(CassandraConnectorConfig.POLL_INTERVAL_MS, pollIntervalMs);
        assertEquals(pollIntervalMs, config.pollIntervalMs().toMillis());

        int schemaPollIntervalMs = 500;
        config = buildTaskConfig(CassandraConnectorConfig.SCHEMA_POLL_INTERVAL_MS, schemaPollIntervalMs);
        assertEquals(schemaPollIntervalMs, config.schemaPollIntervalMs().toMillis());

        int cdcDirPollIntervalMs = 500;
        config = buildTaskConfig(CassandraConnectorConfig.CDC_DIR_POLL_INTERVAL_MS, cdcDirPollIntervalMs);
        assertEquals(cdcDirPollIntervalMs, config.cdcDirPollIntervalMs().toMillis());

        int snapshotPollIntervalMs = 500;
        config = buildTaskConfig(CassandraConnectorConfig.SNAPSHOT_POLL_INTERVAL_MS, snapshotPollIntervalMs);
        assertEquals(snapshotPollIntervalMs, config.snapshotPollIntervalMs().toMillis());

        String fieldBlacklist = "keyspace1.table1.column1,keyspace1.table1.column2";
        config = buildTaskConfig(CassandraConnectorConfig.FIELD_BLACKLIST, fieldBlacklist);
        assertArrayEquals(fieldBlacklist.split(","), config.fieldBlacklist());

        config = buildTaskConfig(CassandraConnectorConfig.TOMBSTONES_ON_DELETE, true);
        assertTrue(config.tombstonesOnDelete());

        String snapshotMode = "always";
        config = buildTaskConfig(CassandraConnectorConfig.SNAPSHOT_MODE, snapshotMode);
        assertEquals(CassandraConnectorConfig.SnapshotMode.ALWAYS, config.snapshotMode());

        String commitLogDir = "/foo/bar";
        config = buildTaskConfig(CassandraConnectorConfig.COMMIT_LOG_RELOCATION_DIR, commitLogDir);
        assertEquals(commitLogDir, config.commitLogRelocationDir());

        boolean shouldPostProcess = false;
        config = buildTaskConfig(CassandraConnectorConfig.COMMIT_LOG_POST_PROCESSING_ENABLED, shouldPostProcess);
        assertEquals(shouldPostProcess, config.postProcessEnabled());

        String transferClazz = "io.debezium.connector.cassandra.BlackHoleCommitLogTransfer";
        config = buildTaskConfig(CassandraConnectorConfig.COMMIT_LOG_TRANSFER_CLASS, transferClazz);
        assertEquals(transferClazz, config.getCommitLogTransfer().getClass().getName());
    }

    private CassandraConnectorConfig buildTaskConfig(String key, Object value) {
        Map<String, Object> map = Collections.singletonMap(key, value);
        return new CassandraConnectorConfig(map);
    }

    @Test
    public void testDefaultConfigs() {
        CassandraConnectorConfig config = new CassandraConnectorConfig(Collections.emptyMap());
        Assert.assertEquals(CassandraConnectorConfig.DEFAULT_HTTP_PORT, config.httpPort());
        Assert.assertArrayEquals(CassandraConnectorConfig.DEFAULT_CASSANDRA_HOST.split(","), config.cassandraHosts());
        Assert.assertEquals(CassandraConnectorConfig.DEFAULT_CASSANDRA_PORT, config.cassandraPort());
        Assert.assertEquals(CassandraConnectorConfig.DEFAULT_MAX_QUEUE_SIZE, config.maxQueueSize());
        Assert.assertEquals(CassandraConnectorConfig.DEFAULT_MAX_BATCH_SIZE, config.maxBatchSize());
        Assert.assertEquals(CassandraConnectorConfig.DEFAULT_POLL_INTERVAL_MS, config.pollIntervalMs().toMillis());
        Assert.assertEquals(CassandraConnectorConfig.DEFAULT_MAX_OFFSET_FLUSH_SIZE, config.maxOffsetFlushSize());
        Assert.assertEquals(CassandraConnectorConfig.DEFAULT_OFFSET_FLUSH_INTERVAL_MS, config.offsetFlushIntervalMs().toMillis());
        Assert.assertEquals(CassandraConnectorConfig.DEFAULT_SCHEMA_POLL_INTERVAL_MS, config.schemaPollIntervalMs().toMillis());
        Assert.assertEquals(CassandraConnectorConfig.DEFAULT_CDC_DIR_POLL_INTERVAL_MS, config.cdcDirPollIntervalMs().toMillis());
        Assert.assertEquals(CassandraConnectorConfig.DEFAULT_SNAPSHOT_POLL_INTERVAL_MS, config.snapshotPollIntervalMs().toMillis());
        Assert.assertEquals(CassandraConnectorConfig.DEFAULT_COMMIT_LOG_POST_PROCESSING_ENABLED, config.postProcessEnabled());
        assertEquals(CassandraConnectorConfig.DEFAULT_COMMIT_LOG_TRANSFER_CLASS, config.getCommitLogTransfer().getClass().getName());
        assertFalse(config.cassandraSslEnabled());
        assertFalse(config.tombstonesOnDelete());
        assertEquals(CassandraConnectorConfig.SnapshotMode.INITIAL, config.snapshotMode());
    }

    @Test
    public void testSnapshotMode() {
        String mode = "initial";
        assertEquals(CassandraConnectorConfig.SnapshotMode.INITIAL, CassandraConnectorConfig.SnapshotMode.fromText(mode).get());
        mode = "INITIAL";
        assertEquals(CassandraConnectorConfig.SnapshotMode.INITIAL, CassandraConnectorConfig.SnapshotMode.fromText(mode).get());
        mode = "Initial";
        assertEquals(CassandraConnectorConfig.SnapshotMode.INITIAL, CassandraConnectorConfig.SnapshotMode.fromText(mode).get());
        mode = "always";
        assertEquals(CassandraConnectorConfig.SnapshotMode.ALWAYS, CassandraConnectorConfig.SnapshotMode.fromText(mode).get());
        mode = "never";
        assertEquals(CassandraConnectorConfig.SnapshotMode.NEVER, CassandraConnectorConfig.SnapshotMode.fromText(mode).get());
        mode = null;
        assertFalse(CassandraConnectorConfig.SnapshotMode.fromText(mode).isPresent());
        mode = "";
        assertFalse(CassandraConnectorConfig.SnapshotMode.fromText(mode).isPresent());
        mode = "invalid";
        assertFalse(CassandraConnectorConfig.SnapshotMode.fromText(mode).isPresent());
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import io.debezium.connector.cassandra.exceptions.CassandraConnectorConfigException;
import com.datastax.driver.core.ConsistencyLevel;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.storage.Converter;

/**
 * All configs used by a Cassandra connector agent.
 */
public class CassandraConnectorConfig {

    /**
     * The set of predefined SnapshotMode options.
     */
    public enum SnapshotMode {

        /**
         * Perform a snapshot whenever a new table with cdc enabled is detected. This is detected by periodically
         * scanning tables in Cassandra.
         */
        ALWAYS,

        /**
         * Perform a snapshot for unsnapshotted tables upon initial startup of the cdc agent.
         */
        INITIAL,

        /**
         * Never perform a snapshot, instead change events are only read from commit logs.
         */
        NEVER;

        public static Optional<SnapshotMode> fromText(String text) {
            return Arrays.stream(values())
                    .filter(v -> text != null && v.name().toLowerCase().equals(text.toLowerCase()))
                    .findFirst();
        }
    }

    /**
     * Logical name for the Cassandra connector. This name should uniquely identify the connector from
     * those that reside in other Cassandra nodes.
     */
    public static final String CONNECTOR_NAME = "connector.name";

    /**
     * Logical name for the Cassandra cluster. This name should be identical across all Cassandra connectors
     * in a Cassandra cluster
     */
    public static final String KAFKA_TOPIC_PREFIX ="kafka.topic.prefix";

    /**
     * The prefix prepended to all Kafka producer configurations, including schema registry
     */
    public static final String KAFKA_PRODUCER_CONFIG_PREFIX = "kafka.producer.";

    /**
     * The prefix prepended to all Kafka key converter configurations, including schema registry.
     */
    public static final String KEY_CONVERTER_PREFIX = "key.converter.";

    /**
     * The prefix prepended to all Kafka value converter configurations, including schema registry.
     */
    public static final String VALUE_CONVERTER_PREFIX = "value.converter.";

    /**
     * Required config for Kafka key converter.
     */
    public static final String KEY_CONVERTER_CLASS_CONFIG = "key.converter";

    /**
     * Required config for Kafka value converter.
     */
    public static final String VALUE_CONVERTER_CLASS_CONFIG = "value.converter";

    /**
     * Specifies the criteria for running a snapshot (eg. initial sync) upon startup of the cassandra connector agent.
     * Must be one of 'INITIAL', 'ALWAYS', or 'NEVER'. The default snapshot mode is 'INITIAL'.
     * See {@link SnapshotMode for details}.
     */
    public static final String SNAPSHOT_MODE = "snapshot.mode";
    public static final String DEFAULT_SNAPSHOT_MODE = "INITIAL";

    /**
     * Specify the {@link ConsistencyLevel} used for the snapshot query.
     */
    public static final String SNAPSHOT_CONSISTENCY = "snapshot.consistency";
    public static final String DEFAULT_SNAPSHOT_CONSISTENCY = "QUORUM";

    /**
     * The port used by the HTTP server for ping, health check, and build info
     */
    public static final String HTTP_PORT = "http.port";
    public static final int DEFAULT_HTTP_PORT = 8000;

    /**
     * The absolute path of the YAML config file used by a Cassandra node.
     */
    public static final String CASSANDRA_CONFIG = "cassandra.config";

    /**
     * One or more addresses of Cassandra nodes that driver uses to discover topology, separated by ","
     */
    public static final String CASSANDRA_HOSTS = "cassandra.hosts";
    public static final String DEFAULT_CASSANDRA_HOST = "localhost";

    /**
     * The port used to connect to Cassandra host(s).
     */
    public static final String CASSANDRA_PORT = "cassandra.port";
    public static final int DEFAULT_CASSANDRA_PORT = 9042;

    /**
     * The username used when connecting to Cassandra hosts.
     */
    public static final String CASSANDRA_USERNAME = "cassandra.username";

    /**
     * The password used when connecting to Cassandra hosts.
     */
    public static final String CASSANDRA_PASSWORD = "cassandra.password";

    /**
     * If set to true, Cassandra connector agent will use SSL to connect to Cassandra node.
     */
    public static final String CASSANDRA_SSL_ENABLED = "cassandra.ssl.enabled";
    public static final boolean DEFAULT_CASSANDRA_SSL_ENABLED = false;

    /**
     * The SSL config file path required for storage node.
     */
    public static final String CASSANDRA_SSL_CONFIG_PATH = "cassandra.ssl.config.path";

    /**
     * The local directory which commit logs get relocated to once processed.
     */
    public static final String COMMIT_LOG_RELOCATION_DIR = "commit.log.relocation.dir";

    /**
     * Determines whether or not the CommitLogPostProcessor should run.
     * If disabled, commit logs would not be deleted post-process, and this could lead to disk storage
     */
    public static final String COMMIT_LOG_POST_PROCESSING_ENABLED = "commit.log.post.processing.enabled";
    public static final boolean DEFAULT_COMMIT_LOG_POST_PROCESSING_ENABLED = true;

    /**
     * The fully qualified {@link CommitLogTransfer} class used to transfer commit logs.
     * The default option will delete all commit log files after processing (successful or otherwise).
     * You can extend a custom implementation.
     */
    public static final String COMMIT_LOG_TRANSFER_CLASS = "commit.log.transfer.class";
    public static final String DEFAULT_COMMIT_LOG_TRANSFER_CLASS = "io.debezium.connector.cassandra.BlackHoleCommitLogTransfer";

    /**
     * The prefix for all {@link CommitLogTransfer} configurations.
     */
    public static final String COMMIT_LOG_TRANSFER_CONFIG_PREFIX = "commit.log.transfer.";

    /**
     * The directory to store offset tracking files.
     */
    public static final String OFFSET_BACKING_STORE_DIR = "offset.backing.store.dir";

    /**
     * The minimum amount of time to wait before committing the offset. The default value of 0 implies
     * the offset will be flushed every time.
     */
    public static final String OFFSET_FLUSH_INTERVAL_MS = "offset.flush.interval.ms";
    public static final int DEFAULT_OFFSET_FLUSH_INTERVAL_MS = 0;

    /**
     * The maximum records that are allowed to be processed until it is required to flush offset to disk.
     * This config is effective only if offset_flush_interval_ms != 0
     */
    public static final String MAX_OFFSET_FLUSH_SIZE = "max.offset.flush.size";
    public static final int DEFAULT_MAX_OFFSET_FLUSH_SIZE = 100;

    /**
     * Positive integer value that specifies the maximum size of the blocking queue into which change events read from
     * the commit log are placed before they are written to Kafka. This queue can provide back pressure to the commit log
     * reader when, for example, writes to Kafka are slower or if Kafka is not available. Events that appear in the queue
     * are not included in the offsets periodically recorded by this connector. Defaults to 8192, and should always be
     * larger than the maximum batch size specified in the max.batch.size property.
     * The capacity of the queue to hold deserialized {@link Record}
     * before they are converted to kafka connect Struct Records and emitted to Kafka.
     */
    public static final String MAX_QUEUE_SIZE = "max.queue.size";
    public static final int DEFAULT_MAX_QUEUE_SIZE = 8192;

    /**
     * The maximum number of change events to dequeue each time.
     */
    public static final String MAX_BATCH_SIZE = "max.batch.size";
    public static final int DEFAULT_MAX_BATCH_SIZE = 2048;

    /**
     * Positive integer value that specifies the number of milliseconds the commit log processor should wait during
     * each iteration for new change events to appear in the queue. Defaults to 1000 milliseconds, or 1 second.
     */
    public static final String POLL_INTERVAL_MS = "poll.interval.ms";
    public static final int DEFAULT_POLL_INTERVAL_MS = 1000;

     /**
     * Positive integer value that specifies the number of milliseconds the schema processor should wait before
     * refreshing the cached Cassandra table schemas.
     */
    public static final String SCHEMA_POLL_INTERVAL_MS = "schema.refresh.interval.ms";
    public static final int DEFAULT_SCHEMA_POLL_INTERVAL_MS = 10000;

    /**
     * The maximum amount of time to wait on each poll before reattempt.
     */
    public static final String CDC_DIR_POLL_INTERVAL_MS = "cdc.dir.poll.interval.ms";
    public static final int DEFAULT_CDC_DIR_POLL_INTERVAL_MS = 10000;

    /**
     * Positive integer value that specifies the number of milliseconds the snapshot processor should wait before
     * re-scanning tables to look for new cdc-enabled tables. Defaults to 10000 milliseconds, or 10 seconds.
     */
    public static final String SNAPSHOT_POLL_INTERVAL_MS = "snapshot.scan.interval.ms";
    public static final int DEFAULT_SNAPSHOT_POLL_INTERVAL_MS = 10000;

    /**
     * Whether deletion events should have a subsequent tombstone event (true) or not (false).
     * It's important to note that in Cassandra, two events with the same key may be updating
     * different columns of a given table. So this could potentially result in records being lost
     * during compaction if they haven't been consumed by the consumer yet. In other words, do NOT
     * set this to true if you have kafka compaction turned on.
     */
    public static final String TOMBSTONES_ON_DELETE = "tombstones.on.delete";
    public static final boolean DEFAULT_TOMBSTONES_ON_DELETE = false;

    /**
     * A comma-separated list of fully-qualified names of fields that should be excluded from change event message values.
     * Fully-qualified names for fields are in the form {@code <keyspace_name>.<field_name>.<nested_field_name>}.
     */
    public static final String FIELD_BLACKLIST = "field.blacklist";

    /**
     * Instead of parsing commit logs from CDC directory, this will look for the commit log with the
     * latest modified timestamp in the commit log directory and attempt to process this file only.
     * Only used for Testing!
     */
    public static final String LATEST_COMMIT_LOG_ONLY = "latest.commit.log.only";
    public static final boolean DEFAULT_LATEST_COMMIT_LOG_ONLY = false;

    private final Properties configs;

    public CassandraConnectorConfig(Properties configs) {
        this.configs = configs;
    }

    public String connectorName() {
        return (String) configs.get(CONNECTOR_NAME);
    }

    public String kafkaTopicPrefix() {
        return (String) configs.get(KAFKA_TOPIC_PREFIX);
    }

    public Properties getKafkaConfigs() {
        Properties props = new Properties();

        // default configs
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        configs.entrySet().stream()
                .filter(entry -> entry.getKey().toString().startsWith(KAFKA_PRODUCER_CONFIG_PREFIX))
                .forEach(entry -> {
                    String k = entry.getKey().toString().replace(KAFKA_PRODUCER_CONFIG_PREFIX, "");
                    Object v = entry.getValue();
                    props.put(k, v);
                });

        return props;
    }

    public Properties commitLogTransferConfigs() {
        Properties props = new Properties();
        configs.entrySet().stream()
                .filter(entry -> entry.getKey().toString().startsWith(COMMIT_LOG_TRANSFER_CONFIG_PREFIX))
                .forEach(entry -> props.put(entry.getKey(), entry.getValue()));
        return props;
    }

    public boolean latestCommitLogOnly() {
        return configs.containsKey(LATEST_COMMIT_LOG_ONLY) ?
                Boolean.parseBoolean((String) configs.get(LATEST_COMMIT_LOG_ONLY)) : DEFAULT_LATEST_COMMIT_LOG_ONLY;
    }

    public SnapshotMode snapshotMode() {
        String mode = (String) configs.getOrDefault(SNAPSHOT_MODE, DEFAULT_SNAPSHOT_MODE);
        Optional<SnapshotMode> snapshotModeOpt = SnapshotMode.fromText(mode);
        return snapshotModeOpt.orElseThrow(() -> new CassandraConnectorConfigException(mode + " is not a valid SnapshotMode"));
    }

    public ConsistencyLevel snapshotConsistencyLevel() {
        String cl = (String) configs.getOrDefault(SNAPSHOT_CONSISTENCY, DEFAULT_SNAPSHOT_CONSISTENCY);
        return ConsistencyLevel.valueOf(cl);
    }

    public int httpPort() {
        return configs.containsKey(HTTP_PORT) ? Integer.parseInt((String) configs.get(HTTP_PORT)) : DEFAULT_HTTP_PORT;
    }

    public String cassandraConfig() {
        return (String) configs.get(CASSANDRA_CONFIG);
    }

    public String[] cassandraHosts() {
        String hosts = (String) configs.getOrDefault(CASSANDRA_HOSTS, DEFAULT_CASSANDRA_HOST);
        return hosts.split(",");
    }

    public int cassandraPort() {
        return configs.containsKey(CASSANDRA_PORT) ?
                Integer.parseInt((String) configs.get(CASSANDRA_PORT)) : DEFAULT_CASSANDRA_PORT;
    }

    public boolean cassandraSslEnabled() {
        return configs.containsKey(CASSANDRA_SSL_ENABLED) ?
                Boolean.parseBoolean((String) configs.get(CASSANDRA_SSL_ENABLED)) : DEFAULT_CASSANDRA_SSL_ENABLED;
    }

    public String cassandraSslConfigPath() {
        return (String) configs.get(CASSANDRA_SSL_CONFIG_PATH);
    }

    public String cassandraUsername() {
        return (String) configs.get(CASSANDRA_USERNAME);
    }

    public String cassandraPassword() {
        return (String) configs.get(CASSANDRA_PASSWORD);
    }

    public String commitLogRelocationDir() {
        return (String) configs.get(COMMIT_LOG_RELOCATION_DIR);
    }

    public boolean postProcessEnabled() {
        return configs.containsKey(COMMIT_LOG_POST_PROCESSING_ENABLED) ?
                Boolean.parseBoolean((String) configs.get(COMMIT_LOG_POST_PROCESSING_ENABLED)) :
                DEFAULT_COMMIT_LOG_POST_PROCESSING_ENABLED;
    }

    public CommitLogTransfer getCommitLogTransfer() {
        try {
            String clazz = (String) configs.getOrDefault(COMMIT_LOG_TRANSFER_CLASS, DEFAULT_COMMIT_LOG_TRANSFER_CLASS);
            CommitLogTransfer transfer = (CommitLogTransfer) Class.forName(clazz).newInstance();
            transfer.init(commitLogTransferConfigs());
            return transfer;
        } catch (Exception e) {
            throw new CassandraConnectorConfigException(e);
        }
    }

    public String offsetBackingStoreDir() {
    return (String) configs.get(OFFSET_BACKING_STORE_DIR);
    }

    public Duration offsetFlushIntervalMs() {
        int ms = configs.containsKey(OFFSET_FLUSH_INTERVAL_MS) ?
                Integer.parseInt((String) configs.get(OFFSET_FLUSH_INTERVAL_MS)) : DEFAULT_OFFSET_FLUSH_INTERVAL_MS;
        return Duration.ofMillis(ms);
    }

    public long maxOffsetFlushSize() {
        return configs.containsKey(MAX_OFFSET_FLUSH_SIZE) ?
                Long.parseLong((String) configs.get(MAX_OFFSET_FLUSH_SIZE)) : DEFAULT_MAX_OFFSET_FLUSH_SIZE;
    }

    public int maxQueueSize() {
        return configs.containsKey(MAX_QUEUE_SIZE) ?
                Integer.parseInt((String) configs.get(MAX_QUEUE_SIZE)) : DEFAULT_MAX_QUEUE_SIZE;
    }

    public int maxBatchSize() {
        return configs.containsKey(MAX_BATCH_SIZE) ?
                Integer.parseInt((String) configs.get(MAX_BATCH_SIZE)) : DEFAULT_MAX_BATCH_SIZE;
    }

    public Duration pollIntervalMs() {
        int ms = configs.containsKey(POLL_INTERVAL_MS) ?
                Integer.parseInt((String) configs.get(POLL_INTERVAL_MS)) : DEFAULT_POLL_INTERVAL_MS;
        return Duration.ofMillis(ms);
    }

    public Duration schemaPollIntervalMs() {
        int ms = configs.containsKey(SCHEMA_POLL_INTERVAL_MS) ?
                Integer.parseInt((String) configs.get(SCHEMA_POLL_INTERVAL_MS)) : DEFAULT_SCHEMA_POLL_INTERVAL_MS;
        return Duration.ofMillis(ms);
    }

    public Duration cdcDirPollIntervalMs() {
        int ms = configs.containsKey(CDC_DIR_POLL_INTERVAL_MS) ?
                Integer.parseInt((String) configs.get(CDC_DIR_POLL_INTERVAL_MS)) : DEFAULT_CDC_DIR_POLL_INTERVAL_MS;
        return Duration.ofMillis(ms);
    }

    public Duration snapshotPollIntervalMs() {
        int ms = configs.containsKey(SNAPSHOT_POLL_INTERVAL_MS) ?
                Integer.parseInt((String) configs.get(SNAPSHOT_POLL_INTERVAL_MS)) : DEFAULT_SNAPSHOT_POLL_INTERVAL_MS;
        return Duration.ofMillis(ms);
    }

    public String[] fieldBlacklist() {
        String hosts = (String) configs.get(FIELD_BLACKLIST);
        if (hosts == null) {
            return new String[0];
        }
        return hosts.split(",");
    }

    public boolean tombstonesOnDelete() {
        return configs.containsKey(TOMBSTONES_ON_DELETE) ?
                Boolean.parseBoolean((String) configs.get(TOMBSTONES_ON_DELETE)) : DEFAULT_TOMBSTONES_ON_DELETE;
    }

    public Converter getKeyConverter() throws CassandraConnectorConfigException {
        try {
            Class keyConverterClass = Class.forName((String) configs.get(KEY_CONVERTER_CLASS_CONFIG));
            Converter keyConverter = (Converter) keyConverterClass.newInstance();
            Map<String, Object> keyConverterConfigs = keyValueConverterConfigs(KEY_CONVERTER_PREFIX);
            keyConverter.configure(keyConverterConfigs, true);
            return keyConverter;
        } catch (Exception e) {
            throw new CassandraConnectorConfigException(e);
        }
    }

    public Converter getValueConverter() throws CassandraConnectorConfigException {
        try {
            Class valueConverterClass = Class.forName((String) configs.get(VALUE_CONVERTER_CLASS_CONFIG));
            Converter valueConverter = (Converter) valueConverterClass.newInstance();
            Map<String, Object> valueConverterConfigs = keyValueConverterConfigs(VALUE_CONVERTER_PREFIX);
            valueConverter.configure(valueConverterConfigs, false);
            return valueConverter;
        } catch (Exception e) {
            throw new CassandraConnectorConfigException(e);
        }
    }

    private Map<String, Object> keyValueConverterConfigs(String converterPrefix) {
        return configs.entrySet().stream()
                .filter(entry -> entry.toString().startsWith(converterPrefix))
                .collect(Collectors.toMap(entry -> entry.getKey().toString().replace(converterPrefix, ""), entry -> entry.getValue()));
    }

    @Override
    public String toString() {
        return configs.entrySet().stream()
                .filter(e -> !e.getKey().toString().toLowerCase().contains("username") && !e.getKey().toString().toLowerCase().contains("password"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                .toString();
    }
}

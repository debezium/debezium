/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.storage.Converter;

import com.datastax.driver.core.ConsistencyLevel;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorConfigException;

/**
 * All configs used by a Cassandra connector agent.
 */
public class CassandraConnectorConfig extends CommonConnectorConfig {

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
     * The prefix for all {@link CommitLogTransfer} configurations.
     */
    public static final String COMMIT_LOG_TRANSFER_CONFIG_PREFIX = "commit.log.transfer.";

    /**
     * Logical name for the Cassandra connector. This name should uniquely identify the connector from
     * those that reside in other Cassandra nodes.
     */
    public static final Field CONNECTOR_NAME = Field.create("connector.name").withType(Type.STRING);

    /**
     * Logical name for the Cassandra cluster. This name should be identical across all Cassandra connectors
     * in a Cassandra cluster
     */
    public static final Field KAFKA_TOPIC_PREFIX = Field.create("kafka.topic.prefix").withType(Type.STRING);

    /**
     * Required config for Kafka key converter.
     */
    public static final Field KEY_CONVERTER_CLASS_CONFIG = Field.create("key.converter").withType(Type.STRING);

    /**
     * Required config for Kafka value converter.
     */
    public static final Field VALUE_CONVERTER_CLASS_CONFIG = Field.create("value.converter").withType(Type.STRING);

    /**
     * Specifies the criteria for running a snapshot (eg. initial sync) upon startup of the cassandra connector agent.
     * Must be one of 'INITIAL', 'ALWAYS', or 'NEVER'. The default snapshot mode is 'INITIAL'.
     * See {@link SnapshotMode for details}.
     */
    public static final String DEFAULT_SNAPSHOT_MODE = "INITIAL";
    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withType(Type.STRING).withDefault(DEFAULT_SNAPSHOT_MODE);

    /**
     * Specify the {@link ConsistencyLevel} used for the snapshot query.
     */
    public static final String DEFAULT_SNAPSHOT_CONSISTENCY = "QUORUM";
    public static final Field SNAPSHOT_CONSISTENCY = Field.create("snapshot.consistency")
            .withType(Type.STRING).withDefault(DEFAULT_SNAPSHOT_CONSISTENCY);

    /**
     * The port used by the HTTP server for ping, health check, and build info
     */
    public static final int DEFAULT_HTTP_PORT = 8000;
    public static final Field HTTP_PORT = Field.create("http.port").withType(Type.INT).withDefault(DEFAULT_HTTP_PORT);

    /**
     * The absolute path of the YAML config file used by a Cassandra node.
     */
    public static final Field CASSANDRA_CONFIG = Field.create("cassandra.config").withType(Type.STRING);

    /**
     * One or more addresses of Cassandra nodes that driver uses to discover topology, separated by ","
     */
    public static final String DEFAULT_CASSANDRA_HOST = "localhost";
    public static final Field CASSANDRA_HOSTS = Field.create("cassandra.hosts")
            .withType(Type.STRING).withDefault(DEFAULT_CASSANDRA_HOST);

    /**
     * The port used to connect to Cassandra host(s).
     */
    public static final int DEFAULT_CASSANDRA_PORT = 9042;
    public static final Field CASSANDRA_PORT = Field.create("cassandra.port")
            .withType(Type.INT).withDefault(DEFAULT_CASSANDRA_PORT);

    /**
     * The username used when connecting to Cassandra hosts.
     */
    public static final Field CASSANDRA_USERNAME = Field.create("cassandra.username").withType(Type.STRING);

    /**
     * The password used when connecting to Cassandra hosts.
     */
    public static final Field CASSANDRA_PASSWORD = Field.create("cassandra.password").withType(Type.STRING);

    /**
     * If set to true, Cassandra connector agent will use SSL to connect to Cassandra node.
     */
    public static final boolean DEFAULT_CASSANDRA_SSL_ENABLED = false;
    public static final Field CASSANDRA_SSL_ENABLED = Field.create("cassandra.ssl.enabled")
            .withType(Type.BOOLEAN).withDefault(DEFAULT_CASSANDRA_SSL_ENABLED);

    /**
     * The SSL config file path required for storage node.
     */
    public static final Field CASSANDRA_SSL_CONFIG_PATH = Field.create("cassandra.ssl.config.path").withType(Type.STRING);

    /**
     * The local directory which commit logs get relocated to once processed.
     */
    public static final Field COMMIT_LOG_RELOCATION_DIR = Field.create("commit.log.relocation.dir").withType(Type.STRING);

    /**
     * Determines whether or not the CommitLogPostProcessor should run.
     * If disabled, commit logs would not be deleted post-process, and this could lead to disk storage
     */
    public static final boolean DEFAULT_COMMIT_LOG_POST_PROCESSING_ENABLED = true;
    public static final Field COMMIT_LOG_POST_PROCESSING_ENABLED = Field.create("commit.log.post.processing.enabled")
            .withType(Type.BOOLEAN).withDefault(DEFAULT_COMMIT_LOG_POST_PROCESSING_ENABLED);

    /**
     * Determine if CommitLogProcessor should re-process error commitLogFiles.
     */
    public static final boolean DEFAULT_COMMIT_LOG_ERROR_REPROCESSING_ENABLED = false;
    public static final Field COMMIT_LOG_ERROR_REPROCESSING_ENABLED = Field.create("commit.log.error.reprocessing.enabled")
            .withType(Type.BOOLEAN).withDefault(DEFAULT_COMMIT_LOG_ERROR_REPROCESSING_ENABLED);

    /**
     * The fully qualified {@link CommitLogTransfer} class used to transfer commit logs.
     * The default option will delete all commit log files after processing (successful or otherwise).
     * You can extend a custom implementation.
     */
    public static final String DEFAULT_COMMIT_LOG_TRANSFER_CLASS = "io.debezium.connector.cassandra.BlackHoleCommitLogTransfer";
    public static final Field COMMIT_LOG_TRANSFER_CLASS = Field.create("commit.log.transfer.class")
            .withType(Type.STRING).withDefault(DEFAULT_COMMIT_LOG_TRANSFER_CLASS);

    /**
     * The directory to store offset tracking files.
     */
    public static final Field OFFSET_BACKING_STORE_DIR = Field.create("offset.backing.store.dir").withType(Type.STRING);

    /**
     * The minimum amount of time to wait before committing the offset. The default value of 0 implies
     * the offset will be flushed every time.
     */
    public static final int DEFAULT_OFFSET_FLUSH_INTERVAL_MS = 0;
    public static final Field OFFSET_FLUSH_INTERVAL_MS = Field.create("offset.flush.interval.ms")
            .withType(Type.INT).withDefault(DEFAULT_OFFSET_FLUSH_INTERVAL_MS);

    /**
     * The maximum records that are allowed to be processed until it is required to flush offset to disk.
     * This config is effective only if offset_flush_interval_ms != 0
     */
    public static final int DEFAULT_MAX_OFFSET_FLUSH_SIZE = 100;
    public static final Field MAX_OFFSET_FLUSH_SIZE = Field.create("max.offset.flush.size")
            .withType(Type.INT).withDefault(DEFAULT_MAX_OFFSET_FLUSH_SIZE);

    /**
    * Positive integer value that specifies the number of milliseconds the schema processor should wait before
    * refreshing the cached Cassandra table schemas.
    */
    public static final int DEFAULT_SCHEMA_POLL_INTERVAL_MS = 10000;
    public static final Field SCHEMA_POLL_INTERVAL_MS = Field.create("schema.refresh.interval.ms")
            .withType(Type.INT).withDefault(DEFAULT_SCHEMA_POLL_INTERVAL_MS);

    /**
     * The maximum amount of time to wait on each poll before reattempt.
     */
    public static final int DEFAULT_CDC_DIR_POLL_INTERVAL_MS = 10000;
    public static final Field CDC_DIR_POLL_INTERVAL_MS = Field.create("cdc.dir.poll.interval.ms")
            .withType(Type.INT).withDefault(DEFAULT_CDC_DIR_POLL_INTERVAL_MS);

    /**
     * Positive integer value that specifies the number of milliseconds the snapshot processor should wait before
     * re-scanning tables to look for new cdc-enabled tables. Defaults to 10000 milliseconds, or 10 seconds.
     */
    public static final int DEFAULT_SNAPSHOT_POLL_INTERVAL_MS = 10000;
    public static final Field SNAPSHOT_POLL_INTERVAL_MS = Field.create("snapshot.scan.interval.ms")
            .withType(Type.INT).withDefault(DEFAULT_SNAPSHOT_POLL_INTERVAL_MS);

    /**
     * A comma-separated list of fully-qualified names of fields that should be excluded from change event message values.
     * Fully-qualified names for fields are in the form {@code <keyspace_name>.<field_name>.<nested_field_name>}.
     */
    public static final Field FIELD_EXCLUDE_LIST = Field.create("field.exclude.list")
            .withDisplayName("Exclude Fields")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withInvisibleRecommender()
            .withDescription("Regular expressions matching fields to include in change events");

    /**
     * Old, backwards-compatible "blacklist" property.
     */
    @Deprecated
    public static final Field FIELD_BLACKLIST = Field.create("field.blacklist")
            .withDisplayName("Deprecated: Exclude Fields")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withInvisibleRecommender()
            .withDescription("Regular expressions matching fields to include in change events (deprecated, use \"" + FIELD_EXCLUDE_LIST.name() + "\" instead)");

    /**
     * Instead of parsing commit logs from CDC directory, this will look for the commit log with the
     * latest modified timestamp in the commit log directory and attempt to process this file only.
     * Only used for Testing!
     */
    public static final boolean DEFAULT_LATEST_COMMIT_LOG_ONLY = false;
    public static final Field LATEST_COMMIT_LOG_ONLY = Field.create("latest.commit.log.only")
            .withType(Type.BOOLEAN).withDefault(DEFAULT_LATEST_COMMIT_LOG_ONLY);

    public static final int DEFAULT_POLL_INTERVAL_MS = 1000;

    public static final boolean DEFAULT_TOMBSTONES_ON_DELETE = false;

    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 0;

    public CassandraConnectorConfig(Configuration config) {
        super(config, config.getString(CONNECTOR_NAME), DEFAULT_SNAPSHOT_FETCH_SIZE);
    }

    public String connectorName() {
        return this.getConfig().getString(CONNECTOR_NAME);
    }

    public String kafkaTopicPrefix() {
        return this.getConfig().getString(KAFKA_TOPIC_PREFIX);
    }

    public Properties getKafkaConfigs() {
        Properties props = new Properties();

        // default configs
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        this.getConfig().asMap().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(KAFKA_PRODUCER_CONFIG_PREFIX))
                .forEach(entry -> {
                    String k = entry.getKey().replace(KAFKA_PRODUCER_CONFIG_PREFIX, "");
                    Object v = entry.getValue();
                    props.put(k, v);
                });

        return props;
    }

    public Properties commitLogTransferConfigs() {
        Properties props = new Properties();
        this.getConfig().asMap().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(COMMIT_LOG_TRANSFER_CONFIG_PREFIX))
                .forEach(entry -> props.put(entry.getKey(), entry.getValue()));
        return props;
    }

    public boolean latestCommitLogOnly() {
        return this.getConfig().getBoolean(LATEST_COMMIT_LOG_ONLY);
    }

    public SnapshotMode snapshotMode() {
        String mode = this.getConfig().getString(SNAPSHOT_MODE);
        Optional<SnapshotMode> snapshotModeOpt = SnapshotMode.fromText(mode);
        return snapshotModeOpt.orElseThrow(() -> new CassandraConnectorConfigException(mode + " is not a valid SnapshotMode"));
    }

    public ConsistencyLevel snapshotConsistencyLevel() {
        String cl = this.getConfig().getString(SNAPSHOT_CONSISTENCY);
        return ConsistencyLevel.valueOf(cl);
    }

    public int httpPort() {
        return this.getConfig().getInteger(HTTP_PORT);
    }

    public String cassandraConfig() {
        return this.getConfig().getString(CASSANDRA_CONFIG);
    }

    public String[] cassandraHosts() {
        String hosts = this.getConfig().getString(CASSANDRA_HOSTS);
        return hosts.split(",");
    }

    public int cassandraPort() {
        return this.getConfig().getInteger(CASSANDRA_PORT);
    }

    public boolean cassandraSslEnabled() {
        return this.getConfig().getBoolean(CASSANDRA_SSL_ENABLED);
    }

    public String cassandraSslConfigPath() {
        return this.getConfig().getString(CASSANDRA_SSL_CONFIG_PATH);
    }

    public String cassandraUsername() {
        return this.getConfig().getString(CASSANDRA_USERNAME);
    }

    public String cassandraPassword() {
        return this.getConfig().getString(CASSANDRA_PASSWORD);
    }

    public String commitLogRelocationDir() {
        return this.getConfig().getString(COMMIT_LOG_RELOCATION_DIR);
    }

    public boolean postProcessEnabled() {
        return this.getConfig().getBoolean(COMMIT_LOG_POST_PROCESSING_ENABLED);
    }

    public boolean errorCommitLogReprocessEnabled() {
        return this.getConfig().getBoolean(COMMIT_LOG_ERROR_REPROCESSING_ENABLED);
    }

    public CommitLogTransfer getCommitLogTransfer() {
        try {
            String clazz = this.getConfig().getString(COMMIT_LOG_TRANSFER_CLASS);
            CommitLogTransfer transfer = (CommitLogTransfer) Class.forName(clazz).newInstance();
            transfer.init(commitLogTransferConfigs());
            return transfer;
        }
        catch (Exception e) {
            throw new CassandraConnectorConfigException(e);
        }
    }

    public String offsetBackingStoreDir() {
        return this.getConfig().getString(OFFSET_BACKING_STORE_DIR);
    }

    public Duration offsetFlushIntervalMs() {
        int ms = this.getConfig().getInteger(OFFSET_FLUSH_INTERVAL_MS);
        return Duration.ofMillis(ms);
    }

    public long maxOffsetFlushSize() {
        return this.getConfig().getLong(MAX_OFFSET_FLUSH_SIZE);
    }

    public int maxQueueSize() {
        return this.getConfig().getInteger(MAX_QUEUE_SIZE);
    }

    public int maxBatchSize() {
        return this.getConfig().getInteger(MAX_BATCH_SIZE);
    }

    /**
     * Positive integer value that specifies the number of milliseconds the commit log processor should wait during
     * each iteration for new change events to appear in the queue. Defaults to 1000 milliseconds, or 1 second.
     */
    public Duration pollIntervalMs() {
        int ms = this.getConfig().getInteger(POLL_INTERVAL_MS, DEFAULT_POLL_INTERVAL_MS);
        return Duration.ofMillis(ms);
    }

    public Duration schemaPollIntervalMs() {
        int ms = this.getConfig().getInteger(SCHEMA_POLL_INTERVAL_MS);
        return Duration.ofMillis(ms);
    }

    public Duration cdcDirPollIntervalMs() {
        int ms = this.getConfig().getInteger(CDC_DIR_POLL_INTERVAL_MS);
        return Duration.ofMillis(ms);
    }

    public Duration snapshotPollIntervalMs() {
        int ms = this.getConfig().getInteger(SNAPSHOT_POLL_INTERVAL_MS);
        return Duration.ofMillis(ms);
    }

    public List<String> fieldExcludeList() {
        String fieldExcludeList = this.getConfig().getFallbackStringProperty(FIELD_EXCLUDE_LIST, FIELD_BLACKLIST);
        if (fieldExcludeList == null) {
            return Collections.emptyList();
        }
        return Arrays.asList(fieldExcludeList.split(","));
    }

    /**
     * Whether deletion events should have a subsequent tombstone event (true) or not (false).
     * It's important to note that in Cassandra, two events with the same key may be updating
     * different columns of a given table. So this could potentially result in records being lost
     * during compaction if they haven't been consumed by the consumer yet. In other words, do NOT
     * set this to true if you have kafka compaction turned on.
     */
    public boolean tombstonesOnDelete() {
        return this.getConfig().getBoolean(TOMBSTONES_ON_DELETE, DEFAULT_TOMBSTONES_ON_DELETE);
    }

    public Converter getKeyConverter() throws CassandraConnectorConfigException {
        try {
            Class keyConverterClass = Class.forName(this.getConfig().getString(KEY_CONVERTER_CLASS_CONFIG));
            Converter keyConverter = (Converter) keyConverterClass.newInstance();
            Map<String, Object> keyConverterConfigs = keyValueConverterConfigs(KEY_CONVERTER_PREFIX);
            keyConverter.configure(keyConverterConfigs, true);
            return keyConverter;
        }
        catch (Exception e) {
            throw new CassandraConnectorConfigException(e);
        }
    }

    public Converter getValueConverter() throws CassandraConnectorConfigException {
        try {
            Class valueConverterClass = Class.forName(this.getConfig().getString(VALUE_CONVERTER_CLASS_CONFIG));
            Converter valueConverter = (Converter) valueConverterClass.newInstance();
            Map<String, Object> valueConverterConfigs = keyValueConverterConfigs(VALUE_CONVERTER_PREFIX);
            valueConverter.configure(valueConverterConfigs, false);
            return valueConverter;
        }
        catch (Exception e) {
            throw new CassandraConnectorConfigException(e);
        }
    }

    private Map<String, Object> keyValueConverterConfigs(String converterPrefix) {
        return this.getConfig().asMap().entrySet().stream()
                .filter(entry -> entry.toString().startsWith(converterPrefix))
                .collect(Collectors.toMap(entry -> entry.getKey().replace(converterPrefix, ""), entry -> entry.getValue()));
    }

    @Override
    public String toString() {
        return this.getConfig().asMap().entrySet().stream()
                .filter(e -> !e.getKey().toLowerCase().contains("username") && !e.getKey().toLowerCase().contains("password"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                .toString();
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return new CassandraSourceInfoStructMaker(Module.name(), Module.version(), this);
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }
}

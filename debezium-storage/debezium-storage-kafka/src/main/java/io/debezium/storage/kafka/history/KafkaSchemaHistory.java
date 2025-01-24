/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.kafka.history;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.config.Field.Validator;
import io.debezium.document.DocumentReader;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.util.Collect;
import io.debezium.util.Loggings;
import io.debezium.util.Threads;

/**
 * A {@link SchemaHistory} implementation that records schema changes as normal {@link SourceRecord}s on the specified topic,
 * and that recovers the history by establishing a Kafka Consumer re-processing all messages on that topic.
 *
 * @author Randall Hauch
 */
@NotThreadSafe
public class KafkaSchemaHistory extends AbstractSchemaHistory {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSchemaHistory.class);

    private static final String CLEANUP_POLICY_NAME = "cleanup.policy";
    private static final String CLEANUP_POLICY_VALUE = "delete";
    private static final String RETENTION_MS_NAME = "retention.ms";
    private static final long RETENTION_MS_MAX = Long.MAX_VALUE;
    private static final long RETENTION_MS_MIN = Duration.of(5 * 365, ChronoUnit.DAYS).toMillis(); // 5 years

    private static final String RETENTION_BYTES_NAME = "retention.bytes";
    private static final int UNLIMITED_VALUE = -1;
    private static final int PARTITION_COUNT = 1;

    /**
     * The name of broker property defining default replication factor for topics without the explicit setting.
     *
     * @see kafka.server.KafkaConfig.DefaultReplicationFactorProp
     */
    private static final String DEFAULT_TOPIC_REPLICATION_FACTOR_PROP_NAME = "default.replication.factor";

    /**
     * The default replication factor for the history topic which is used in case
     * the value couldn't be retrieved from the broker.
     */
    private static final short DEFAULT_TOPIC_REPLICATION_FACTOR = 1;

    public static final Field TOPIC = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.topic")
            .withDisplayName("Database schema history topic name")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 32))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("The name of the topic for the database schema history")
            .withValidation(KafkaSchemaHistory.forKafka(Field::isRequired));

    public static final Field BOOTSTRAP_SERVERS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.bootstrap.servers")
            .withDisplayName("Kafka broker addresses")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 31))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("A list of host/port pairs that the connector will use for establishing the initial "
                    + "connection to the Kafka cluster for retrieving database schema history previously stored "
                    + "by the connector. This should point to the same Kafka cluster used by the Kafka Connect "
                    + "process.")
            .withValidation(KafkaSchemaHistory.forKafka(Field::isRequired));

    public static final Field RECOVERY_POLL_INTERVAL_MS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING
            + "kafka.recovery.poll.interval.ms")
            .withDisplayName("Poll interval during database schema history recovery (ms)")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The number of milliseconds to wait while polling for persisted data during recovery.")
            .withDefault(100)
            .withValidation(Field::isNonNegativeInteger);

    public static final Field RECOVERY_POLL_ATTEMPTS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.recovery.attempts")
            .withDisplayName("Max attempts to recovery database schema history")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The number of attempts in a row that no data are returned from Kafka before recover completes. "
                    + "The maximum amount of time to wait after receiving no data is (recovery.attempts) x (recovery.poll.interval.ms).")
            .withDefault(100)
            .withValidation(Field::isInteger);

    public static final Field KAFKA_QUERY_TIMEOUT_MS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.query.timeout.ms")
            .withDisplayName("Kafka admin client query timeout (ms)")
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 33))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The number of milliseconds to wait while fetching cluster information using Kafka admin client.")
            .withDefault(Duration.ofSeconds(3).toMillis())
            .withValidation(Field::isPositiveInteger);

    public static final Field KAFKA_CREATE_TIMEOUT_MS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.create.timeout.ms")
            .withDisplayName("Kafka admin client create timeout (ms)")
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 33))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The number of milliseconds to wait while create kafka history topic using Kafka admin client.")
            .withDefault(Duration.ofSeconds(30).toMillis())
            .withValidation(Field::isPositiveInteger);

    public static Field.Set ALL_FIELDS = Field.setOf(TOPIC, BOOTSTRAP_SERVERS, NAME, RECOVERY_POLL_INTERVAL_MS,
            RECOVERY_POLL_ATTEMPTS, INTERNAL_CONNECTOR_CLASS, INTERNAL_CONNECTOR_ID, KAFKA_QUERY_TIMEOUT_MS);

    private static final String CONSUMER_PREFIX = CONFIGURATION_FIELD_PREFIX_STRING + "consumer.";
    private static final String PRODUCER_PREFIX = CONFIGURATION_FIELD_PREFIX_STRING + "producer.";

    /**
     * The one and only partition of the history topic.
     */
    private static final Integer PARTITION = 0;

    private final DocumentReader reader = DocumentReader.defaultReader();
    private String topicName;
    private Configuration consumerConfig;
    private Configuration producerConfig;
    private volatile KafkaProducer<String, String> producer;
    private int maxRecoveryAttempts;
    private Duration pollInterval;
    private ExecutorService checkTopicSettingsExecutor;
    private Duration kafkaQueryTimeout;
    private Duration kafkaCreateTimeout;

    private static final boolean USE_KAFKA_24_NEW_TOPIC_CONSTRUCTOR = hasNewTopicConstructorWithOptionals();

    private static boolean hasNewTopicConstructorWithOptionals() {
        try {
            NewTopic.class.getConstructor(String.class, Optional.class, Optional.class);
            return true;
        }
        catch (NoSuchMethodException nsme) {
            return false;
        }
    }

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        if (!config.validateAndRecord(ALL_FIELDS, LOGGER::error)) {
            throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }
        this.topicName = config.getString(TOPIC);
        this.pollInterval = Duration.ofMillis(config.getInteger(RECOVERY_POLL_INTERVAL_MS));
        this.maxRecoveryAttempts = config.getInteger(RECOVERY_POLL_ATTEMPTS);
        this.kafkaQueryTimeout = Duration.ofMillis(config.getLong(KAFKA_QUERY_TIMEOUT_MS));
        this.kafkaCreateTimeout = Duration.ofMillis(config.getLong(KAFKA_CREATE_TIMEOUT_MS));

        String bootstrapServers = config.getString(BOOTSTRAP_SERVERS);
        // Copy the relevant portions of the configuration and add useful defaults ...
        String dbHistoryName = config.getString(SchemaHistory.NAME, UUID.randomUUID().toString());
        this.consumerConfig = config.subset(CONSUMER_PREFIX, true).edit()
                .withDefault(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                .withDefault(ConsumerConfig.CLIENT_ID_CONFIG, dbHistoryName)
                .withDefault(ConsumerConfig.GROUP_ID_CONFIG, dbHistoryName)
                .withDefault(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1) // get even the smallest message
                .withDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                .withDefault(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000) // readjusted since 0.10.1.0
                .withDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        OffsetResetStrategy.EARLIEST.toString().toLowerCase()) // consume from the beginning
                .withDefault(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .withDefault(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .build();
        this.producerConfig = config.subset(PRODUCER_PREFIX, true).edit()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                .withDefault(ProducerConfig.CLIENT_ID_CONFIG, dbHistoryName)
                .withDefault(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1) // to prevent message re-ordering
                // in case of failed sends with retries
                .withDefault(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false) // By default, Connect disables idempotent
                // behavior for all producers, even though idempotence became default for Kafka producers. This is
                // to ensure Connect continues to work with many Kafka broker versions, including older brokers that
                // do not support idempotent producers or require explicit steps to enable them (e.g. adding the
                // IDEMPOTENT_WRITE ACL to brokers older than 2.8).

                .withDefault(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 32) // 32KB
                .withDefault(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024 * 1024) // 1MB - reduced buffer memory as
                // schema changes are small and infrequent
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .build();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("KafkaSchemaHistory Consumer config: {}", consumerConfig.withMaskedPasswords());
            LOGGER.info("KafkaSchemaHistory Producer config: {}", producerConfig.withMaskedPasswords());
        }

        try {
            final String connectorClassname = config.getString(INTERNAL_CONNECTOR_CLASS);
            if (connectorClassname != null) {
                checkTopicSettingsExecutor = Threads.newSingleThreadExecutor((Class<? extends SourceConnector>) Class.forName(connectorClassname),
                        config.getString(INTERNAL_CONNECTOR_ID), "db-history-config-check", true);
            }
        }
        catch (ClassNotFoundException e) {
            throw new DebeziumException(e);
        }
    }

    @Override
    public synchronized void start() {
        super.start();
        if (this.producer == null) {
            this.producer = new KafkaProducer<>(this.producerConfig.asProperties());
        }
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
        if (this.producer == null) {
            throw new IllegalStateException("No producer is available. Ensure that 'start()' is called before storing database schema history records.");
        }
        LOGGER.trace("Storing record into database schema history: {}", record);
        try {
            ProducerRecord<String, String> produced = new ProducerRecord<>(topicName, PARTITION, null, record.toString());
            Future<RecordMetadata> future = this.producer.send(produced);
            // Flush and then wait ...
            this.producer.flush();
            RecordMetadata metadata = future.get(); // block forever since we have to be sure this gets recorded
            if (metadata != null) {
                LOGGER.debug("Stored record in topic '{}' partition {} at offset {} ",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        }
        catch (InterruptedException e) {
            LOGGER.trace("Interrupted before record was written into database schema history: {}", record);
            Thread.currentThread().interrupt();
            throw new SchemaHistoryException(e);
        }
        catch (ExecutionException e) {
            throw new SchemaHistoryException(e);
        }
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        try (KafkaConsumer<String, String> historyConsumer = new KafkaConsumer<>(consumerConfig.asProperties())) {
            // Subscribe to the only partition for this topic, and seek to the beginning of that partition ...
            LOGGER.debug("Subscribing to database schema history topic '{}'", topicName);
            historyConsumer.subscribe(Collect.arrayListOf(topicName));

            // Read all messages in the topic ...
            long lastProcessedOffset = UNLIMITED_VALUE;
            Long endOffset = null;
            int recoveryAttempts = 0;

            // read the topic until the end
            do {
                if (recoveryAttempts > maxRecoveryAttempts) {
                    throw new IllegalStateException(
                            "The database schema history couldn't be recovered. Consider to increase the value for " + RECOVERY_POLL_INTERVAL_MS.name());
                }

                endOffset = getEndOffsetOfDbHistoryTopic(endOffset, historyConsumer);
                LOGGER.debug("End offset of database schema history topic is {}", endOffset);

                // DBZ-1361 not using poll(Duration) to keep compatibility with AK 1.x
                ConsumerRecords<String, String> recoveredRecords = historyConsumer.poll(this.pollInterval.toMillis());
                int numRecordsProcessed = 0;

                for (ConsumerRecord<String, String> record : recoveredRecords) {
                    try {
                        if (lastProcessedOffset < record.offset()) {
                            if (record.value() == null) {
                                LOGGER.warn("Skipping null database schema history record. " +
                                        "This is often not an issue, but if it happens repeatedly please check the '{}' topic.", topicName);
                            }
                            else {
                                HistoryRecord recordObj = new HistoryRecord(reader.read(record.value()));
                                LOGGER.trace("Recovering database schema history: {}", recordObj);
                                if (recordObj == null || !recordObj.isValid()) {
                                    LOGGER.warn("Skipping invalid database schema history record '{}'. " +
                                            "This is often not an issue, but if it happens repeatedly please check the '{}' topic.",
                                            recordObj, topicName);
                                }
                                else {
                                    records.accept(recordObj);
                                    LOGGER.trace("Recovered database schema history: {}", recordObj);
                                }
                            }
                            lastProcessedOffset = record.offset();
                            ++numRecordsProcessed;
                        }
                    }
                    catch (final IOException e) {
                        Loggings.logErrorAndTraceRecord(LOGGER, record, "Error while deserializing history record", e);
                    }
                    catch (final Exception e) {
                        Loggings.logErrorAndTraceRecord(LOGGER, record, "Unexpected exception while processing record", e);
                        throw e;
                    }
                }
                if (numRecordsProcessed == 0) {
                    LOGGER.debug("No new records found in the database schema history; will retry");
                    recoveryAttempts++;
                }
                else {
                    LOGGER.debug("Processed {} records from database schema history", numRecordsProcessed);
                    recoveryAttempts = 0;
                }
            } while (lastProcessedOffset < endOffset - 1);
        }
    }

    private Long getEndOffsetOfDbHistoryTopic(Long previousEndOffset, KafkaConsumer<String, String> historyConsumer) {
        Map<TopicPartition, Long> offsets = historyConsumer.endOffsets(Collections.singleton(new TopicPartition(topicName, PARTITION)));
        Long endOffset = offsets.entrySet().iterator().next().getValue();

        // The end offset should never change during recovery; doing this check here just as - a rather weak - attempt
        // to spot other connectors that share the same history topic accidentally
        if (previousEndOffset != null && !previousEndOffset.equals(endOffset)) {
            LOGGER.warn("Detected changed end offset of database schema history topic (previous: "
                    + previousEndOffset + ", current: " + endOffset
                    + "). Make sure that the same history topic isn't shared by multiple connector instances.");
        }

        return endOffset;
    }

    @Override
    public boolean storageExists() {
        // Check if the topic exists in the list of all topics
        try (KafkaConsumer<String, String> checkTopicConsumer = new KafkaConsumer<>(consumerConfig.asProperties())) {
            return checkTopicConsumer.listTopics().containsKey(topicName);
        }
    }

    @Override
    public boolean exists() {
        boolean exists = false;
        if (storageExists()) {
            try (KafkaConsumer<String, String> historyConsumer = new KafkaConsumer<>(consumerConfig.asProperties())) {
                checkTopicSettings(topicName);
                // check if the topic is empty
                Set<TopicPartition> historyTopic = Collections.singleton(new TopicPartition(topicName, PARTITION));

                Map<TopicPartition, Long> beginningOffsets = historyConsumer.beginningOffsets(historyTopic);
                Map<TopicPartition, Long> endOffsets = historyConsumer.endOffsets(historyTopic);

                Long beginOffset = beginningOffsets.entrySet().iterator().next().getValue();
                Long endOffset = endOffsets.entrySet().iterator().next().getValue();

                exists = endOffset > beginOffset;
            }
        }
        return exists;
    }

    private void checkTopicSettings(String topicName) {
        if (checkTopicSettingsExecutor == null || checkTopicSettingsExecutor.isShutdown()) {
            return;
        }
        checkTopicSettingsExecutor.execute(() -> {
            // DBZ-2228 Avoiding id conflict with client used on the main thread
            String clientId = this.producerConfig.getString(AdminClientConfig.CLIENT_ID_CONFIG) + "-topic-check";
            Properties clientConfig = this.producerConfig.asProperties();
            clientConfig.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId);

            try (AdminClient admin = AdminClient.create(clientConfig)) {

                Set<ConfigResource> resources = Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC, topicName));
                final Map<ConfigResource, Config> configs = admin.describeConfigs(resources).all().get(
                        kafkaQueryTimeout.toMillis(), TimeUnit.MILLISECONDS);
                if (configs.size() != 1) {
                    LOGGER.info("Expected one topic '{}' to match the query but got {}", topicName, configs.values().size());
                    return;
                }
                final Config topic = configs.values().iterator().next();
                if (topic == null) {
                    LOGGER.info("Could not get config for topic '{}'", topic);
                    return;
                }

                final String cleanupPolicy = topic.get(CLEANUP_POLICY_NAME).value();
                if (!CLEANUP_POLICY_VALUE.equals(cleanupPolicy)) {
                    LOGGER.warn("Database schema history topic '{}' option '{}' should be '{}' but is '{}'", topicName, CLEANUP_POLICY_NAME, CLEANUP_POLICY_VALUE,
                            cleanupPolicy);
                    return;
                }

                final String retentionBytes = topic.get(RETENTION_BYTES_NAME).value();
                if (retentionBytes != null && Long.parseLong(retentionBytes) != UNLIMITED_VALUE) {
                    LOGGER.warn("Database schema history topic '{}' option '{}' should be '{}' but is '{}'", topicName, RETENTION_BYTES_NAME, UNLIMITED_VALUE,
                            retentionBytes);
                    return;
                }

                final String retentionMs = topic.get(RETENTION_MS_NAME).value();
                if (retentionMs != null && (Long.parseLong(retentionMs) != UNLIMITED_VALUE && Long.parseLong(retentionMs) < RETENTION_MS_MIN)) {
                    LOGGER.warn("Database schema history topic '{}' option '{}' should be '{}' or greater than '{}' (5 years) but is '{}'", topicName, RETENTION_MS_NAME,
                            UNLIMITED_VALUE, RETENTION_MS_MIN, retentionMs);
                    return;
                }

                final DescribeTopicsResult result = admin.describeTopics(Collections.singleton(topicName));
                if (result.values().size() != 1) {
                    LOGGER.info("Expected one topic '{}' to match the query but got {}", topicName, result.values().size());
                    return;
                }
                final TopicDescription topicDesc = result.values().values().iterator().next().get();
                if (topicDesc == null) {
                    LOGGER.info("Could not get description for topic '{}'", topicName);
                    return;
                }

                final int partitions = topicDesc.partitions().size();
                if (partitions != PARTITION_COUNT) {
                    LOGGER.warn("Database schema history topic '{}' should have one partiton but has '{}'", topicName, partitions);
                    return;
                }

                LOGGER.info("Database schema history topic '{}' has correct settings", topicName);
            }
            catch (Throwable e) {
                LOGGER.info("Attempted to validate database schema history topic but failed", e);
            }
            stopCheckTopicSettingsExecutor();
        });
    }

    @Override
    public synchronized void stop() {
        stopCheckTopicSettingsExecutor();
        try {
            if (this.producer != null) {
                try {
                    this.producer.flush();
                }
                finally {
                    this.producer.close(Duration.ofSeconds(30));
                }
            }
        }
        finally {
            this.producer = null;
            super.stop();
        }
    }

    private void stopCheckTopicSettingsExecutor() {
        if (checkTopicSettingsExecutor != null) {
            checkTopicSettingsExecutor.shutdown();
        }
    }

    @Override
    public String toString() {
        if (topicName != null) {
            return "Kafka topic " + topicName + ":" + PARTITION
                    + " using brokers at " + producerConfig.getString(BOOTSTRAP_SERVERS);
        }
        return "Kafka topic";
    }

    public static String consumerConfigPropertyName(String kafkaConsumerPropertyName) {
        return CONSUMER_PREFIX + kafkaConsumerPropertyName;
    }

    @Override
    public void initializeStorage() {
        super.initializeStorage();

        try (AdminClient admin = AdminClient.create(this.producerConfig.asProperties())) {

            NewTopic topic = null;

            // if possible (underlying client and server are Kafka API 2.4+), we create the
            // topic without explicitly specifying the replication factor, relying on the
            // broker default setting
            try {
                if (USE_KAFKA_24_NEW_TOPIC_CONSTRUCTOR) {
                    topic = new NewTopic(topicName, Optional.of(PARTITION_COUNT), Optional.empty());
                }
            }
            catch (Exception ex) {
                if (!(ex.getCause() instanceof UnsupportedVersionException)) {
                    throw ex;
                }
            }

            // falling back to querying and providing the replication factor explicitly;
            // that's not the preferred choice, as querying ("DescribeConfigs") requires an
            // additional ACL/privilege, which we otherwise don't need
            if (topic == null) {
                short replicationFactor = getDefaultTopicReplicationFactor(admin);
                topic = new NewTopic(topicName, PARTITION_COUNT, replicationFactor);
            }

            topic.configs(Collect.hashMapOf(CLEANUP_POLICY_NAME, CLEANUP_POLICY_VALUE, RETENTION_MS_NAME, Long.toString(RETENTION_MS_MAX), RETENTION_BYTES_NAME,
                    Long.toString(UNLIMITED_VALUE)));
            try {
                CreateTopicsResult result = admin.createTopics(Collections.singleton(topic));
                result.all().get(kafkaCreateTimeout.toMillis(), TimeUnit.MILLISECONDS);
                LOGGER.info("Database schema history topic '{}' created", topic);
            }
            catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    LOGGER.info("Database schema history topic '{}' already exist", topic);
                }
                else {
                    throw e;
                }
            }
        }
        catch (Exception e) {
            throw new ConnectException("Creation of database schema history topic failed, please create the topic manually", e);
        }
    }

    private short getDefaultTopicReplicationFactor(AdminClient admin) throws Exception {
        try {
            Config brokerConfig = getKafkaBrokerConfig(admin);
            String defaultReplicationFactorValue = brokerConfig.get(DEFAULT_TOPIC_REPLICATION_FACTOR_PROP_NAME).value();

            // Ensure that the default replication factor property was returned by the Admin Client
            if (defaultReplicationFactorValue != null) {
                return Short.parseShort(defaultReplicationFactorValue);
            }
        }
        catch (ExecutionException ex) {
            // ignore UnsupportedVersionException, e.g. due to older broker version
            if (!(ex.getCause() instanceof UnsupportedVersionException)) {
                throw ex;
            }
        }

        // Otherwise warn that no property was obtained and default it to 1 - users can increase this later if desired
        LOGGER.warn(
                "Unable to obtain the default replication factor from the brokers at {}. Setting value to {} instead.",
                producerConfig.getString(BOOTSTRAP_SERVERS),
                DEFAULT_TOPIC_REPLICATION_FACTOR);

        return DEFAULT_TOPIC_REPLICATION_FACTOR;
    }

    private Config getKafkaBrokerConfig(AdminClient admin) throws Exception {
        final Collection<Node> nodes = admin.describeCluster().nodes().get(kafkaQueryTimeout.toMillis(), TimeUnit.MILLISECONDS);
        if (nodes.isEmpty()) {
            throw new ConnectException("No brokers available to obtain default settings");
        }

        String nodeId = nodes.iterator().next().idString();
        Set<ConfigResource> resources = Collections.singleton(new ConfigResource(ConfigResource.Type.BROKER, nodeId));
        final Map<ConfigResource, Config> configs = admin.describeConfigs(resources).all().get(
                kafkaQueryTimeout.toMillis(), TimeUnit.MILLISECONDS);

        if (configs.isEmpty()) {
            throw new ConnectException("No configs have been received");
        }

        return configs.values().iterator().next();
    }

    private static Validator forKafka(final Validator validator) {
        return (config, field, problems) -> {
            final String history = config.getString(HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY);
            return KafkaSchemaHistory.class.getName().equals(history) ? validator.validate(config, field, problems) : 0;
        };
    }
}

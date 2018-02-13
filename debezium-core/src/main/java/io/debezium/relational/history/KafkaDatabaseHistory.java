/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.util.Collect;

/**
 * A {@link DatabaseHistory} implementation that records schema changes as normal {@link SourceRecord}s on the specified topic,
 * and that recovers the history by establishing a Kafka Consumer re-processing all messages on that topic.
 *
 * @author Randall Hauch
 */
@NotThreadSafe
public class KafkaDatabaseHistory extends AbstractDatabaseHistory {

    /**
     * The name of broker property defining default replication factor for topics without the explicit setting.
     *
     * @see kafka.server.KafkaConfig.DefaultReplicationFactorProp
     */
    private static final String DEFAULT_TOPIC_REPLICATION_FACTOR_PROP_NAME = "default.replication.factor";

    public static final Field TOPIC = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.topic")
                                           .withDisplayName("Database history topic name")
                                           .withType(Type.STRING)
                                           .withWidth(Width.LONG)
                                           .withImportance(Importance.HIGH)
                                           .withDescription("The name of the topic for the database schema history")
                                           .withValidation(Field::isRequired);

    public static final Field BOOTSTRAP_SERVERS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.bootstrap.servers")
                                                       .withDisplayName("Kafka broker addresses")
                                                       .withType(Type.STRING)
                                                       .withWidth(Width.LONG)
                                                       .withImportance(Importance.HIGH)
                                                       .withDescription("A list of host/port pairs that the connector will use for establishing the initial "
                                                               + "connection to the Kafka cluster for retrieving database schema history previously stored "
                                                               + "by the connector. This should point to the same Kafka cluster used by the Kafka Connect "
                                                               + "process.")
                                                       .withValidation(Field::isRequired);

    public static final Field RECOVERY_POLL_INTERVAL_MS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING
            + "kafka.recovery.poll.interval.ms")
                                                               .withDisplayName("Poll interval during database history recovery (ms)")
                                                               .withType(Type.INT)
                                                               .withWidth(Width.SHORT)
                                                               .withImportance(Importance.LOW)
                                                               .withDescription("The number of milliseconds to wait while polling for persisted data during recovery.")
                                                               .withDefault(100)
                                                               .withValidation(Field::isNonNegativeInteger);

    public static final Field RECOVERY_POLL_ATTEMPTS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.recovery.attempts")
                                                            .withDisplayName("Max attempts to recovery database history")
                                                            .withType(Type.INT)
                                                            .withWidth(Width.SHORT)
                                                            .withImportance(Importance.LOW)
                                                            .withDescription("The number of attempts in a row that no data are returned from Kafka before recover completes. "
                                                                    + "The maximum amount of time to wait after receiving no data is (recovery.attempts) x (recovery.poll.interval.ms).")
                                                            .withDefault(100)
                                                            .withValidation(Field::isInteger);

    public static Field.Set ALL_FIELDS = Field.setOf(TOPIC, BOOTSTRAP_SERVERS, DatabaseHistory.NAME,
                                                     RECOVERY_POLL_INTERVAL_MS, RECOVERY_POLL_ATTEMPTS);

    private static final String CONSUMER_PREFIX = CONFIGURATION_FIELD_PREFIX_STRING + "consumer.";
    private static final String PRODUCER_PREFIX = CONFIGURATION_FIELD_PREFIX_STRING + "producer.";

    private static final Duration KAFKA_QUERY_TIMEOUT = Duration.ofSeconds(3);

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
    private int pollIntervalMs = -1;

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator) {
        super.configure(config, comparator);
        if (!config.validateAndRecord(ALL_FIELDS, logger::error)) {
            throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }
        this.topicName = config.getString(TOPIC);
        this.pollIntervalMs = config.getInteger(RECOVERY_POLL_INTERVAL_MS);
        this.maxRecoveryAttempts = config.getInteger(RECOVERY_POLL_ATTEMPTS);

        String bootstrapServers = config.getString(BOOTSTRAP_SERVERS);
        // Copy the relevant portions of the configuration and add useful defaults ...
        String dbHistoryName = config.getString(DatabaseHistory.NAME, UUID.randomUUID().toString());
        this.consumerConfig = config.subset(CONSUMER_PREFIX, true).edit()
                                    .withDefault(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                                    .withDefault(ConsumerConfig.CLIENT_ID_CONFIG, dbHistoryName)
                                    .withDefault(ConsumerConfig.GROUP_ID_CONFIG, dbHistoryName)
                                    .withDefault(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1) // get even smallest message
                                    .withDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                                    .withDefault(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000) //readjusted since 0.10.1.0
                                    .withDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                                 OffsetResetStrategy.EARLIEST.toString().toLowerCase())
                                    .withDefault(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                                    .withDefault(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                                    .build();
        this.producerConfig = config.subset(PRODUCER_PREFIX, true).edit()
                                    .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                                    .withDefault(ProducerConfig.CLIENT_ID_CONFIG, dbHistoryName)
                                    .withDefault(ProducerConfig.ACKS_CONFIG, 1)
                                    .withDefault(ProducerConfig.RETRIES_CONFIG, 1) // may result in duplicate messages, but that's
                                                                                   // okay
                                    .withDefault(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 32) // 32KB
                                    .withDefault(ProducerConfig.LINGER_MS_CONFIG, 0)
                                    .withDefault(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024 * 1024) // 1MB
                                    .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                                    .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                                    .withDefault(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000) // wait at most this if we can't reach Kafka
                                    .build();
        logger.info("KafkaDatabaseHistory Consumer config: " + consumerConfig.withMaskedPasswords());
        logger.info("KafkaDatabaseHistory Producer config: " + producerConfig.withMaskedPasswords());
    }

    @Override
    public synchronized void start() {
        super.start();
        if (this.producer == null) {
            this.producer = new KafkaProducer<>(this.producerConfig.asProperties());
        }
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        if (this.producer == null) {
            throw new IllegalStateException("No producer is available. Ensure that 'start()' is called before storing database history records.");
        }
        logger.trace("Storing record into database history: {}", record);
        try {
            ProducerRecord<String, String> produced = new ProducerRecord<>(topicName, PARTITION, null, record.toString());
            Future<RecordMetadata> future = this.producer.send(produced);
            // Flush and then wait ...
            this.producer.flush();
            RecordMetadata metadata = future.get(); // block forever since we have to be sure this gets recorded
            if (metadata != null) {
                logger.debug("Stored record in topic '{}' partition {} at offset {} ",
                             metadata.topic(), metadata.partition(), metadata.offset());
            }
        } catch( InterruptedException e) {
            logger.trace("Interrupted before record was written into database history: {}", record);
            Thread.interrupted();
            throw new DatabaseHistoryException(e);
        } catch (ExecutionException e) {
            throw new DatabaseHistoryException(e);
        }
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        try (KafkaConsumer<String, String> historyConsumer = new KafkaConsumer<>(consumerConfig.asProperties());) {
            // Subscribe to the only partition for this topic, and seek to the beginning of that partition ...
            logger.debug("Subscribing to database history topic '{}'", topicName);
            historyConsumer.subscribe(Collect.arrayListOf(topicName));

            // Read all messages in the topic ...
            long lastProcessedOffset = -1;
            Long endOffset = null;
            int recoveryAttempts = 0;

            // read the topic until the end
            do {
                if (recoveryAttempts > maxRecoveryAttempts) {
                    throw new IllegalStateException("The database history couldn't be recovered. Consider to increase the value for " + RECOVERY_POLL_INTERVAL_MS.name());
                }

                endOffset = getEndOffsetOfDbHistoryTopic(endOffset, historyConsumer);
                logger.debug("End offset of database history topic is {}", endOffset);

                ConsumerRecords<String, String> recoveredRecords = historyConsumer.poll(this.pollIntervalMs);
                int numRecordsProcessed = 0;

                for (ConsumerRecord<String, String> record : recoveredRecords) {
                    try {
                        if (lastProcessedOffset < record.offset()) {
                            if (record.value() == null) {
                                logger.warn("Skipping null database history record. " +
                                        "This is often not an issue, but if it happens repeatedly please check the '{}' topic.", topicName);
                            } else {
                                HistoryRecord recordObj = new HistoryRecord(reader.read(record.value()));
                                logger.trace("Recovering database history: {}", recordObj);
                                if (recordObj == null || !recordObj.isValid()) {
                                    logger.warn("Skipping invalid database history record '{}'. " +
                                            "This is often not an issue, but if it happens repeatedly please check the '{}' topic.",
                                            recordObj, topicName);
                                } else {
                                    records.accept(recordObj);
                                    logger.trace("Recovered database history: {}", recordObj);
                                }
                            }
                            lastProcessedOffset = record.offset();
                            ++numRecordsProcessed;
                        }
                    } catch (final IOException e) {
                        logger.error("Error while deserializing history record '{}'", record, e);
                    } catch (final Exception e) {
                        logger.error("Unexpected exception while processing record '{}'", record, e);
                        throw e;
                    }
                }
                if (numRecordsProcessed == 0) {
                    logger.debug("No new records found in the database history; will retry");
                } else {
                    logger.debug("Processed {} records from database history", numRecordsProcessed);
                }

                recoveryAttempts++;
            }
            while (lastProcessedOffset < endOffset - 1);
        }
    }

    private Long getEndOffsetOfDbHistoryTopic(Long previousEndOffset, KafkaConsumer<String, String> historyConsumer) {
        Map<TopicPartition, Long> offsets = historyConsumer.endOffsets(Collections.singleton(new TopicPartition(topicName, PARTITION)));
        Long endOffset = offsets.entrySet().iterator().next().getValue();

        // The end offset should never change during recovery; doing this check here just as - a rather weak - attempt
        // to spot other connectors that share the same history topic accidentally
        if(previousEndOffset != null && !previousEndOffset.equals(endOffset)) {
            throw new IllegalStateException("Detected changed end offset of database history topic (previous: "
                    + previousEndOffset + ", current: " + endOffset
                    + "). Make sure that the same history topic isn't shared by multiple connector instances."
            );
        }

        return endOffset;
    }

    @Override
    public boolean exists() {
        boolean exists = false;

        try (KafkaConsumer<String, String> historyConsumer = new KafkaConsumer<>(consumerConfig.asProperties());) {
            // First, check if the topic exists in the list of all topics
            if (historyConsumer.listTopics().keySet().contains(topicName)) {
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

    @Override
    public synchronized void stop() {
        try {
            if (this.producer != null) {
                try {
                    this.producer.flush();
                } finally {
                    this.producer.close();
                }
            }
        } finally {
            this.producer = null;
            super.stop();
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

    protected static String consumerConfigPropertyName(String kafkaConsumerPropertyName) {
        return CONSUMER_PREFIX + kafkaConsumerPropertyName;
    }

    @Override
    public void initializeStorage() {
        super.initializeStorage();
        final AdminClient admin = AdminClient.create(this.producerConfig.asProperties());
        try {
            // Find default replication factor
            Config brokerConfig = getKafkaBrokerConfig(admin);
            final short replicationFactor = Short.parseShort(brokerConfig.get(DEFAULT_TOPIC_REPLICATION_FACTOR_PROP_NAME).value());

            // Create topic
            final NewTopic topic = new NewTopic(topicName, (short)1, replicationFactor);
            topic.configs(Collect.hashMapOf("cleanup.policy", "delete", "retention.ms", Long.toString(Long.MAX_VALUE)));
            admin.createTopics(Collections.singleton(topic));

            logger.info("Database history topic '{}' created", topic);
        }
        catch (Exception e) {
            throw new ConnectException("Creation of database history topic failed, please create the topic manually", e);
        }
    }

    private Config getKafkaBrokerConfig(AdminClient admin) throws Exception {
        final Collection<Node> nodes = admin.describeCluster().nodes().get(KAFKA_QUERY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (nodes.isEmpty()) {
            throw new ConnectException("No brokers available to obtain default settings");
        }

        String nodeId = nodes.iterator().next().idString();
        Set<ConfigResource> resources = Collections.singleton(new ConfigResource(ConfigResource.Type.BROKER, nodeId));
        final Map<ConfigResource, Config> configs = admin.describeConfigs(resources).all().get(
                KAFKA_QUERY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS
        );

        if (configs.isEmpty()) {
            throw new ConnectException("No configs have been received");
        }

        return configs.values().iterator().next();
    }
}

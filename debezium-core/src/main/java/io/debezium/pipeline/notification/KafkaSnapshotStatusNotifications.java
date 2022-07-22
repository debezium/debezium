/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification;

import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.Document;
import io.debezium.pipeline.source.snapshot.incremental.DataCollection;
import io.debezium.spi.schema.DataCollectionId;

public class KafkaSnapshotStatusNotifications<T extends DataCollectionId> implements SnapshotStatusNotifications<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSnapshotStatusNotifications.class);

    public static final Field TOPIC = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.topic")
            .withDisplayName("Snapshot notifications topic name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The name of the topic for the snapshot status notifications")
            .withValidation(forKafka(Field::isRequired));

    public static final Field BOOTSTRAP_SERVERS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.bootstrap.servers")
            .withDisplayName("Kafka broker addresses")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("A list of host/port pairs that the connector will use for establishing the initial "
                    + "connection to the Kafka cluster for retrieving signals to the connector."
                    + "This should point to the same Kafka cluster used by the Kafka Connect process.")
            .withValidation(forKafka(Field::isRequired));

    public static final String STATUS = "status";
    public static final String DATA_COLLECTION = "data-collection";
    public static final String ID = "id";
    public static final String TYPE = "type";
    public static final String INCREMENTAL_SNAPSHOT = "incremental-snapshot";
    public static final String ADDITIONAL_CONDITION = "additional-condition";

    public static Field.Set ALL_FIELDS = Field.setOf(TOPIC, BOOTSTRAP_SERVERS);

    private static final String PRODUCER_PREFIX = CONFIGURATION_FIELD_PREFIX_STRING + "producer.";

    /**
     * The one and only partition of the notifications topic.
     */
    private static final Integer PARTITION = 0;
    private String topicName;
    private volatile KafkaProducer<String, String> producer;

    @Override
    public void configure(Configuration config) {
        if (!config.validateAndRecord(ALL_FIELDS, LOGGER::error)) {
            throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }
        this.topicName = config.getString(TOPIC);
        String bootstrapServers = config.getString(BOOTSTRAP_SERVERS);
        String notificationsName = config.getString("snapshot-status-notifications", UUID.randomUUID().toString());
        Configuration producerConfig = config.subset(PRODUCER_PREFIX, true).edit()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                .withDefault(ProducerConfig.CLIENT_ID_CONFIG, notificationsName)
                .withDefault(ProducerConfig.ACKS_CONFIG, 1)
                .withDefault(ProducerConfig.RETRIES_CONFIG, 1) // may result in duplicate messages, but that's okay
                .withDefault(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 32) // 32KB
                .withDefault(ProducerConfig.LINGER_MS_CONFIG, 0)
                .withDefault(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024 * 1024) // 1MB
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000) // wait at most this if we can't reach Kafka
                .build();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("KafkaSnapshotStatusNotifications Producer config: {}", producerConfig.withMaskedPasswords());
        }
        this.producer = new KafkaProducer<>(producerConfig.asProperties());
    }

    @Override
    public void snapshotCompleted(String connectorName, DataCollection<T> dataCollection, SnapshotCompletionStatus status) {
        Document value = toDocument(dataCollection, status);
        send(connectorName, value);
    }

    private void send(String key, Document value) {
        if (producer == null) {
            throw new IllegalStateException("No producer is available. Ensure that 'configure()' is called configure before 'send()'.");
        }
        LOGGER.trace("Sending snapshot status notification into Kafka: {}-{}", key, value);
        try {
            ProducerRecord<String, String> produced = new ProducerRecord<>(topicName, PARTITION, key, value.toString());
            producer.send(produced);
        }
        catch (Exception e) {
            LOGGER.error("Failed sending snapshot status notification into Kafka: {}-{}", key, value, e);
        }
    }

    private static Field.Validator forKafka(final Field.Validator validator) {
        return (config, field, problems) -> {
            final String notifications = config.getString(SnapshotStatusNotifications.SNAPSHOT_NOTIFICATIONS_CLASS);
            return KafkaSnapshotStatusNotifications.class.getName().equals(notifications) ? validator.validate(config, field, problems) : 0;
        };
    }

    private Document toDocument(DataCollection<T> dataCollection, SnapshotCompletionStatus status) {
        Document document = Document.create();
        document.setString(ID, UUID.randomUUID().toString());
        document.setString(TYPE, INCREMENTAL_SNAPSHOT);
        document.setString(DATA_COLLECTION, dataCollection.getId().identifier());
        dataCollection.getAdditionalCondition().ifPresent(s -> document.setString(ADDITIONAL_CONDITION, s));
        document.setString(STATUS, status.name());
        return document;
    }
}

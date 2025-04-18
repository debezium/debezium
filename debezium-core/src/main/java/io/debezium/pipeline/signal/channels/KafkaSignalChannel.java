/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.channels;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.pipeline.signal.SignalRecord;
import io.debezium.util.Collect;
import io.debezium.util.Loggings;

/**
 * The class responsible for processing of signals delivered to Debezium via a dedicated Kafka topic.
 * The signal message must have the following structure:
 * <ul>
 * <li>{@code id STRING} - the unique identifier of the signal sent, usually UUID, can be used for deduplication</li>
 * <li>{@code type STRING} - the unique logical name of the code executing the signal</li>
 * <li>{@code data STRING} - the data in JSON format that are passed to the signal code
 * </ul>
 */
public class KafkaSignalChannel implements SignalChannelReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSignalChannel.class);
    public static final String CONFIGURATION_FIELD_PREFIX_STRING = "signal.";
    private static final String CONSUMER_PREFIX = CONFIGURATION_FIELD_PREFIX_STRING + "consumer.";
    public static final String CHANNEL_OFFSET = "channelOffset";
    public static final String CHANNEL_NAME = "kafka";

    private String topicName;
    private String connectorName;
    private Duration pollTimeoutMs;
    private KafkaConsumer<String, String> signalsConsumer;

    public static final Field SIGNAL_TOPIC = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.topic")
            .withDisplayName("Signal topic name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The name of the topic for the signals to the connector")
            .withValidation(Field::isRequired);

    public static final Field BOOTSTRAP_SERVERS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.bootstrap.servers")
            .withDisplayName("Kafka broker addresses")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("A list of host/port pairs that the connector will use for establishing the initial "
                    + "connection to the Kafka cluster for retrieving signals to the connector."
                    + "This should point to the same Kafka cluster used by the Kafka Connect process.")
            .withValidation(Field::isRequired);

    public static final Field SIGNAL_POLL_TIMEOUT_MS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING
            + "kafka.poll.timeout.ms")
            .withDisplayName("Poll timeout for kafka signals (ms)")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The number of milliseconds to wait while polling signals.")
            .withDefault(0)
            .withValidation(Field::isNonNegativeInteger);

    public static final Field GROUP_ID = Field.create(CONFIGURATION_FIELD_PREFIX_STRING
            + "kafka.groupId")
            .withDisplayName("Consumer group id for the signal topic")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Consumer group id for the signal topic")
            .withDefault("kafka-signal");

    private Optional<SignalRecord> processSignal(ConsumerRecord<String, String> record) {

        if (!connectorName.equals(record.key())) {
            LOGGER.info("Signal key '{}' doesn't match the connector's name '{}'", record.key(), connectorName);
            return Optional.empty();
        }
        String value = record.value();
        LOGGER.trace("Processing signal: {}", value);
        if (value == null || value.isEmpty()) {
            return Optional.empty();
        }

        final Optional<Document> jsonData = parseJson(value);
        if (jsonData.isEmpty()) {
            return Optional.empty();
        }

        Document document = jsonData.get();
        String id = document.getString("id");
        String type = document.getString("type");
        Document data = document.getDocument("data");

        return Optional.of(new SignalRecord(id, type, data.toString(), Map.of(CHANNEL_OFFSET, record.offset())));
    }

    private static Optional<Document> parseJson(String value) {

        final Document jsonData;
        try {
            jsonData = DocumentReader.defaultReader().read(value);
        }
        catch (IOException e) {
            Loggings.logErrorAndTraceRecord(LOGGER, value, "Skipped signal due to an error", e);
            return Optional.empty();
        }
        return Optional.of(jsonData);
    }

    public void seek(long signalOffset) {
        signalsConsumer.seek(new TopicPartition(topicName, 0), signalOffset + 1);
    }

    @Override
    public String name() {
        return CHANNEL_NAME;
    }

    @Override
    public void init(CommonConnectorConfig connectorConfig) {

        this.connectorName = connectorConfig.getLogicalName();
        Configuration signalConfig = connectorConfig.getConfig().subset(CONFIGURATION_FIELD_PREFIX_STRING, false)
                .edit()
                .withDefault(KafkaSignalChannel.SIGNAL_TOPIC, connectorName + "-signal")
                .build();
        this.topicName = signalConfig.getString(SIGNAL_TOPIC);
        this.pollTimeoutMs = Duration.ofMillis(signalConfig.getInteger(SIGNAL_POLL_TIMEOUT_MS));
        Configuration consumerConfig = buildKafkaConfiguration(signalConfig);
        this.signalsConsumer = new KafkaConsumer<>(consumerConfig.asProperties());
        LOGGER.info("Subscribing to signals topic '{}'", topicName);
        signalsConsumer.assign(Collect.arrayListOf(new TopicPartition(topicName, 0)));
    }

    private static Configuration buildKafkaConfiguration(Configuration signalConfig) {

        Configuration.Builder confBuilder = signalConfig.subset(CONSUMER_PREFIX, true).edit()
                .withDefault(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, signalConfig.getString(BOOTSTRAP_SERVERS))
                .withDefault(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString())
                .withDefault(ConsumerConfig.GROUP_ID_CONFIG, signalConfig.getString(GROUP_ID))
                .withDefault(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1) // get even the smallest message
                .withDefault(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000) // readjusted since 0.10.1.0
                .withDefault(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .withDefault(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .withDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true)
                .withDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return confBuilder
                .build();
    }

    @Override
    public List<SignalRecord> read() {

        LOGGER.debug("Reading signal form kafka");
        ConsumerRecords<String, String> recoveredRecords = signalsConsumer.poll(pollTimeoutMs);

        return StreamSupport.stream(recoveredRecords.spliterator(), false)
                .map(this::processSignal)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    @Override
    public void close() {
        signalsConsumer.commitSync();
        signalsConsumer.close();
    }
}
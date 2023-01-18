/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.signal;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mysql.MySqlReadOnlyIncrementalSnapshotChangeEventSource;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.pipeline.signal.AbstractSnapshotSignal;
import io.debezium.pipeline.signal.ExecuteSnapshot;
import io.debezium.pipeline.signal.PauseIncrementalSnapshot;
import io.debezium.pipeline.signal.ResumeIncrementalSnapshot;
import io.debezium.pipeline.signal.StopSnapshot;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;
import io.debezium.util.Loggings;
import io.debezium.util.Threads;

/**
 * The class responsible for processing of signals delivered to Debezium via a dedicated Kafka topic.
 * The signal message must have the following structure:
 * <ul>
 * <li>{@code id STRING} - the unique identifier of the signal sent, usually UUID, can be used for deduplication</li>
 * <li>{@code type STRING} - the unique logical name of the code executing the signal</li>
 * <li>{@code data STRING} - the data in JSON format that are passed to the signal code
 * </ul>
 */
public class KafkaSignalThread<T extends DataCollectionId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSignalThread.class);

    private final ExecutorService signalTopicListenerExecutor;
    private final String topicName;
    private final String connectorName;
    private final Duration pollTimeoutMs;
    private final MySqlReadOnlyIncrementalSnapshotChangeEventSource<T> eventSource;
    private final KafkaConsumer<String, String> signalsConsumer;

    public static final String CONFIGURATION_FIELD_PREFIX_STRING = "signal.";
    private static final String CONSUMER_PREFIX = CONFIGURATION_FIELD_PREFIX_STRING + "consumer.";

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
            + "signal.kafka.poll.timeout.ms")
            .withDisplayName("Poll timeout for kafka signals (ms)")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The number of milliseconds to wait while polling signals.")
            .withDefault(100)
            .withValidation(Field::isNonNegativeInteger);

    public KafkaSignalThread(Class<? extends SourceConnector> connectorType, CommonConnectorConfig connectorConfig,
                             MySqlReadOnlyIncrementalSnapshotChangeEventSource<T> eventSource) {
        String signalName = "kafka-signal";
        connectorName = connectorConfig.getLogicalName();
        signalTopicListenerExecutor = Threads.newSingleThreadExecutor(connectorType, connectorName, signalName, true);
        Configuration signalConfig = connectorConfig.getConfig().subset(CONFIGURATION_FIELD_PREFIX_STRING, false)
                .edit()
                .withDefault(KafkaSignalThread.SIGNAL_TOPIC, connectorName + "-signal")
                .build();
        this.eventSource = eventSource;
        this.topicName = signalConfig.getString(SIGNAL_TOPIC);
        this.pollTimeoutMs = Duration.ofMillis(signalConfig.getInteger(SIGNAL_POLL_TIMEOUT_MS));
        String bootstrapServers = signalConfig.getString(BOOTSTRAP_SERVERS);
        Configuration consumerConfig = signalConfig.subset(CONSUMER_PREFIX, true).edit()
                .withDefault(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                .withDefault(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString())
                .withDefault(ConsumerConfig.GROUP_ID_CONFIG, signalName)
                .withDefault(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1) // get even smallest message
                .withDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                .withDefault(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000) // readjusted since 0.10.1.0
                .withDefault(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .withDefault(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .build();
        signalsConsumer = new KafkaConsumer<>(consumerConfig.asProperties());
        LOGGER.info("Subscribing to signals topic '{}'", topicName);
        signalsConsumer.assign(Collect.arrayListOf(new TopicPartition(topicName, 0)));
    }

    public void start() {
        signalTopicListenerExecutor.submit(this::monitorSignals);
    }

    private void monitorSignals() {
        while (true) {
            // DBZ-1361 not using poll(Duration) to keep compatibility with AK 1.x
            ConsumerRecords<String, String> recoveredRecords = signalsConsumer.poll(pollTimeoutMs.toMillis());
            for (ConsumerRecord<String, String> record : recoveredRecords) {
                try {
                    processSignal(record);
                }
                catch (final InterruptedException e) {
                    LOGGER.error("Signals processing was interrupted", e);
                    signalsConsumer.close();
                    return;
                }
                catch (final Exception e) {
                    Loggings.logErrorAndTraceRecord(LOGGER, record, "Skipped signal due to an error", e);
                }
            }
        }
    }

    private void processSignal(ConsumerRecord<String, String> record) throws IOException, InterruptedException {
        if (!connectorName.equals(record.key())) {
            LOGGER.info("Signal key '{}' doesn't match the connector's name '{}'", record.key(), connectorName);
            return;
        }
        String value = record.value();
        LOGGER.trace("Processing signal: {}", value);
        final Document jsonData = (value == null || value.isEmpty()) ? Document.create()
                : DocumentReader.defaultReader().read(value);
        String type = jsonData.getString("type");
        Document data = jsonData.getDocument("data");
        switch (type) {
            case ExecuteSnapshot.NAME:
                executeSnapshot(data, record.offset());
                break;
            case StopSnapshot.NAME:
                executeStopSnapshot(data, record.offset());
                break;
            case PauseIncrementalSnapshot.NAME:
                executePause(data);
                break;
            case ResumeIncrementalSnapshot.NAME:
                executeResume(data);
                break;
            default:
                LOGGER.warn("Unknown signal type {}", type);
        }
    }

    private void executeSnapshot(Document data, long signalOffset) {
        final List<String> dataCollections = ExecuteSnapshot.getDataCollections(data);
        if (dataCollections != null) {
            ExecuteSnapshot.SnapshotType snapshotType = ExecuteSnapshot.getSnapshotType(data);
            Optional<String> additionalCondition = ExecuteSnapshot.getAdditionalCondition(data);
            Optional<String> surrogateKey = ExecuteSnapshot.getSurrogateKey(data);
            LOGGER.info("Requested '{}' snapshot of data collections '{}' with additional condition '{}'", snapshotType, dataCollections,
                    additionalCondition.orElse("No condition passed"));
            if (snapshotType == ExecuteSnapshot.SnapshotType.INCREMENTAL) {
                eventSource.enqueueDataCollectionNamesToSnapshot(dataCollections, signalOffset, additionalCondition, surrogateKey);
            }
        }
    }

    private void executeStopSnapshot(Document data, long signalOffset) {
        final List<String> dataCollections = StopSnapshot.getDataCollections(data);
        final AbstractSnapshotSignal.SnapshotType snapshotType = StopSnapshot.getSnapshotType(data);
        if (dataCollections == null || dataCollections.isEmpty()) {
            LOGGER.info("Requested stop of '{}' snapshot", snapshotType);
        }
        else {
            LOGGER.info("Requested stop of '{}' snapshot of data collections '{}'", snapshotType, dataCollections);
        }
        if (snapshotType == AbstractSnapshotSignal.SnapshotType.INCREMENTAL) {
            eventSource.stopSnapshot(dataCollections, signalOffset);
        }
    }

    private void executePause(Document data) {
        PauseIncrementalSnapshot.SnapshotType snapshotType = ExecuteSnapshot.getSnapshotType(data);
        LOGGER.info("Requested snapshot pause");
        if (snapshotType == PauseIncrementalSnapshot.SnapshotType.INCREMENTAL) {
            eventSource.enqueuePauseSnapshot();
        }
    }

    private void executeResume(Document data) {
        ResumeIncrementalSnapshot.SnapshotType snapshotType = ExecuteSnapshot.getSnapshotType(data);
        LOGGER.info("Requested snapshot resume");
        if (snapshotType == ResumeIncrementalSnapshot.SnapshotType.INCREMENTAL) {
            eventSource.enqueueResumeSnapshot();
        }
    }

    public void seek(long signalOffset) {
        signalsConsumer.seek(new TopicPartition(topicName, 0), signalOffset + 1);
    }
}

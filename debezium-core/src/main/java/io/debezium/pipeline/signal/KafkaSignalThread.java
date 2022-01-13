/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

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
import io.debezium.pipeline.source.snapshot.incremental.AbstractReadOnlyIncrementalSnapshotChangeEventSource;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Collect;

/**
 * The class responsible for processing of signals delivered to Debezium via a dedicated Kafka topic.
 * The signal message must have the following structure:
 * <ul>
 * <li>{@code id STRING} - the unique identifier of the signal sent, usually UUID, can be used for deduplication</li>
 * <li>{@code type STRING} - the unique logical name of the code executing the signal</li>
 * <li>{@code data STRING} - the data in JSON format that are passed to the signal code
 * </ul>
 */
public class KafkaSignalThread<T extends DataCollectionId> extends AbstractExternalSignalThread<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSignalThread.class);

    private final String topicName;
    private final Duration pollTimeoutMs;
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

    public KafkaSignalThread(CommonConnectorConfig connectorConfig,
                             AbstractReadOnlyIncrementalSnapshotChangeEventSource<T> eventSource) {
        super(connectorConfig, eventSource);
        Configuration signalConfig = connectorConfig.getConfig().subset(CONFIGURATION_FIELD_PREFIX_STRING, false)
                .edit()
                .withDefault(KafkaSignalThread.SIGNAL_TOPIC, connectorName + "-signal")
                .build();
        this.topicName = signalConfig.getString(SIGNAL_TOPIC);
        this.pollTimeoutMs = Duration.ofMillis(signalConfig.getInteger(SIGNAL_POLL_TIMEOUT_MS));
        String bootstrapServers = signalConfig.getString(BOOTSTRAP_SERVERS);
        Configuration consumerConfig = signalConfig.subset(CONSUMER_PREFIX, true).edit()
                .withDefault(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                .withDefault(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString())
                .withDefault(ConsumerConfig.GROUP_ID_CONFIG, signalName())
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

    protected void monitorSignals() {
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
                    LOGGER.error("Skipped signal due to an error '{}'", record, e);
                }
            }
        }
    }

    /**
     * 
     * @param record An example of the execute-snapshot Kafka record is:
     *               Key = `test_connector`
     *               Value = `{"type":"execute-snapshot","data": {"data-collections": ["schema1.table1", "schema1.table2"], "type": "INCREMENTAL"}}
     * @throws IOException
     * @throws InterruptedException
     */
    protected void processSignal(ConsumerRecord<String, String> record) throws IOException, InterruptedException {
        if (!connectorName.equals(record.key())) {
            LOGGER.info("Signal key '{}' doesn't match the connector's name '{}'", record.key(), connectorName);
            return;
        }
        String value = record.value();
        LOGGER.trace("Processing signal: {}", value);
        final Document jsonData = (value == null || value.isEmpty()) ? Document.create() : reader.read(value);
        processSignal(jsonData, record.offset());
    }

    /**
     * 
     * Overrides the fetch offsets that the consumer will use on the next poll(timeout). 
     * If this API is invoked for the same partition more than once, the latest offset will be used on the next poll(). 
     * 
     * @param signalOffset
     */
    public void seek(long signalOffset) {
        signalsConsumer.seek(new TopicPartition(topicName, 0), signalOffset + 1);
    }
}

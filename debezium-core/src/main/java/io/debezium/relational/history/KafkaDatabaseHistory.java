/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.io.IOException;
import java.util.Collection;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.util.Collect;

/**
 * A {@link DatabaseHistory} implementation that records schema changes as normal {@link SourceRecord}s on the specified topic,
 * and that recovers the history by establishing a Kafka Consumer re-processing all messages on that topic.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public class KafkaDatabaseHistory extends AbstractDatabaseHistory {

    @SuppressWarnings("unchecked")
    public static final Field TOPIC = Field.create("topic")
                                           .withDescription("The name of the topic for the database schema history")
                                           .withValidation(Field::isRequired);

    @SuppressWarnings("unchecked")
    public static final Field BOOTSTRAP_SERVERS = Field.create("bootstrap.servers")
                                                       .withDescription("A list of host/port pairs that the connector will use for establishing the initial "
                                                               + "connection to the Kafka cluster for retrieving database schema history previously stored "
                                                               + "by the connector. This should point to the same Kafka cluster used by the Kafka Connect "
                                                               + "process.")
                                                       .withValidation(Field::isRequired);

    public static Collection<Field> ALL_FIELDS = Collect.arrayListOf(TOPIC, BOOTSTRAP_SERVERS);

    private final DocumentReader reader = DocumentReader.defaultReader();
    private final Integer partition = new Integer(0);
    private String topicName;
    private Configuration consumerConfig;
    private Configuration producerConfig;
    private KafkaProducer<String, String> producer;

    @Override
    public void configure(Configuration config) {
        config.validate(ALL_FIELDS, logger::error);
        super.configure(config);
        this.topicName = config.getString(TOPIC);
        String bootstrapServers = config.getString(BOOTSTRAP_SERVERS);
        // Copy the relevant portions of the configuration and add useful defaults ...
        this.consumerConfig = config.subset("consumer.", true).edit()
                                    .withDefault("bootstrap.servers", bootstrapServers)
                                    .withDefault("group.id", UUID.randomUUID().toString())
                                    .withDefault("enable.auto.commit", false)
                                    .withDefault("session.timeout.ms", 30000)
                                    .withDefault("key.deserializer", StringDeserializer.class.getName())
                                    .withDefault("value.deserializer", StringDeserializer.class.getName())
                                    .build();
        this.producerConfig = config.subset("producer.", true).edit()
                                    .withDefault("bootstrap.servers", bootstrapServers)
                                    .withDefault("acks", "all")
                                    .withDefault("retries", 1) // may result in duplicate messages, but that's okay
                                    .withDefault("batch.size", 1024)    // enough 1024 byte messages per batch
                                    .withDefault("linger.ms", 1)
                                    .withDefault("buffer.memory", 1048576) // 1MB
                                    .withDefault("key.deserializer", StringDeserializer.class.getName())
                                    .withDefault("value.deserializer", StringDeserializer.class.getName())
                                    .build();
        this.producer = new KafkaProducer<>(this.producerConfig.asProperties());
    }

    @Override
    protected void storeRecord(HistoryRecord record) {
        this.producer.send(new ProducerRecord<String, String>(topicName, partition, null, record.toString()));
    }

    @Override
    protected void recoverRecords(Tables schema, DdlParser ddlParser, Consumer<HistoryRecord> records) {
        try (KafkaConsumer<String, String> historyConsumer = new KafkaConsumer<String, String>(consumerConfig.asProperties());) {
            // Subscribe to the only partition for this topic, and seek to the beginning of that partition ...
            TopicPartition topicPartition = new TopicPartition(topicName, partition);
            historyConsumer.assign(Collect.arrayListOf(topicPartition));
            historyConsumer.seekToBeginning(topicPartition);

            // Read all messages in the topic ...
            while (true) {
                ConsumerRecords<String, String> recoveredRecords = historyConsumer.poll(100);
                for (ConsumerRecord<String, String> record : recoveredRecords) {
                    try {
                        records.accept(new HistoryRecord(reader.read(record.value())));
                    } catch (IOException e) {
                        logger.error("Error while deserializing history record", e);
                    }
                }
            }
        }
    }

    @Override
    public void shutdown() {
        try {
            if (this.producer != null) this.producer.close();
        } finally {
            this.producer = null;
            super.shutdown();
        }
    }
}

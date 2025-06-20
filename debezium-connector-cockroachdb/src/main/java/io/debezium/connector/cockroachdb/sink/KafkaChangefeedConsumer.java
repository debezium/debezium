/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb.sink;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka-based consumer for CockroachDB enriched changefeed messages.
 * This is responsible for subscribing to a topic and polling records,
 * which will then be parsed and dispatched by the connector runtime.
 *
 * @author Virag Tripathi
 */
public class KafkaChangefeedConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaChangefeedConsumer.class);

    private final KafkaConsumer<String, String> consumer;

    public KafkaChangefeedConsumer(String topic, String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "debezium-connector-cockroachdb");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));
        LOGGER.info("Subscribed to Kafka topic: {}", topic);
    }

    public ConsumerRecords<String, String> poll(Duration timeout) {
        return consumer.poll(timeout);
    }

    public void close() {
        consumer.close();
        LOGGER.info("Kafka consumer closed.");
    }

    public void commitSync() {
        consumer.commitSync();
    }

    public boolean isClosed() {
        return !consumer.subscription().isEmpty() && !consumer.listTopics().isEmpty();
    }
}

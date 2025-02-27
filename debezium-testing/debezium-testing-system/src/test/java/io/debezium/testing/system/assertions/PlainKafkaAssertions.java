/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class PlainKafkaAssertions implements KafkaAssertions<String, String> {

    private final Properties kafkaConsumerProps;

    public PlainKafkaAssertions(Properties kafkaConsumerProps) {
        this.kafkaConsumerProps = kafkaConsumerProps;
        kafkaConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @Override
    public Consumer<String, String> getConsumer() {
        return new KafkaConsumer<>(kafkaConsumerProps);
    }

    public void assertRecordsContain(String topic, String content) {
        try (Consumer<String, String> consumer = getConsumer()) {
            consumer.subscribe(Collections.singleton(topic));
            consumer.seekToBeginning(consumer.assignment());
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            long matchingCount = StreamSupport.stream(records.records(topic).spliterator(), false).filter(r -> r.value() != null && r.value().contains(content)).count();
            assertThat(matchingCount).withFailMessage("Topic '%s' doesn't have message containing <%s>.", topic, content).isGreaterThan(0);
        }
    }

    public void assertRecordIsUnwrapped(String topic, int amount) {
        try (Consumer<String, String> consumer = getConsumer()) {
            consumer.subscribe(Collections.singleton(topic));
            consumer.seekToBeginning(consumer.assignment());

            ConsumerRecords<String, String> records = consumer.poll(Duration.of(15, ChronoUnit.SECONDS));
            long matchingCount = StreamSupport.stream(records.records(topic).spliterator(), false)
                    .filter(r -> r.value() != null && !r.value().contains("source") && r.value().contains("__table")).count();
            assertThat(matchingCount).withFailMessage("Topic '%s' does not contain transformed messages.", topic).isEqualTo(amount);
        }
    }

    public void assertDocumentIsUnwrapped(String topic, int amount) {
        try (Consumer<String, String> consumer = getConsumer()) {
            consumer.subscribe(Collections.singleton(topic));
            consumer.seekToBeginning(consumer.assignment());

            ConsumerRecords<String, String> records = consumer.poll(Duration.of(15, ChronoUnit.SECONDS));
            long matchingCount = StreamSupport.stream(records.records(topic).spliterator(), false)
                    .filter(r -> r.value() != null && !r.value().contains("source") && r.value().contains("__collection")).count();
            assertThat(matchingCount).withFailMessage("Topic '%s' does not contain transformed messages.", topic).isEqualTo(amount);
        }
    }
}

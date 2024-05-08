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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class AvroKafkaAssertions implements KafkaAssertions<byte[], byte[]> {

    private final Properties kafkaConsumerProps;

    public AvroKafkaAssertions(Properties kafkaConsumerProps) {
        this.kafkaConsumerProps = kafkaConsumerProps;
        kafkaConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        kafkaConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    }

    @Override
    public Consumer<byte[], byte[]> getConsumer() {
        return new KafkaConsumer<>(kafkaConsumerProps);
    }

    @Override
    public void assertRecordsContain(String topic, String content) {
        try (Consumer<byte[], byte[]> consumer = getConsumer()) {
            consumer.subscribe(Collections.singleton(topic));
            consumer.seekToBeginning(consumer.assignment());
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));

            // For now verify magic byte of Avro messages in all records
            StreamSupport.stream(records.records(topic).spliterator(), false).forEach(r -> {
                assertThat(r.key()[0]).isZero();
                assertThat(r.value()[0]).isZero();
            });
        }
    }

    @Override
    public void assertRecordIsUnwrapped(String topic, int amount) {
        try (Consumer<byte[], byte[]> consumer = getConsumer()) {
            consumer.subscribe(Collections.singleton(topic));
            consumer.seekToBeginning(consumer.assignment());
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.of(15, ChronoUnit.SECONDS));
            long matchingCount = StreamSupport.stream(records.records(topic).spliterator(), false).filter(r -> r.value() != null && r.value().length < 70).count();
            assertThat(matchingCount).withFailMessage("Topic '%s' does not contain enough transformed messages.", topic).isEqualTo(amount);
        }
    }

    @Override
    public void assertDocumentIsUnwrapped(String topic, int amount) {
        try (Consumer<byte[], byte[]> consumer = getConsumer()) {
            consumer.subscribe(Collections.singleton(topic));
            consumer.seekToBeginning(consumer.assignment());
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.of(15, ChronoUnit.SECONDS));
            long matchingCount = StreamSupport.stream(records.records(topic).spliterator(), false).filter(r -> r.value() != null && r.value().length < 80).count();
            assertThat(matchingCount).withFailMessage("Topic '%s' does not contain enough transformed messages.", topic).isEqualTo(amount);
        }
    }
}

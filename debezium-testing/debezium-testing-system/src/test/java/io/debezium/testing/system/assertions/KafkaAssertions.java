/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.assertions;

import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.awaitility.core.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface KafkaAssertions<K, V> {

    Logger LOGGER = LoggerFactory.getLogger(KafkaAssertions.class);

    static void awaitAssert(long timeout, TimeUnit unit, ThrowingRunnable assertion) {
        await()
                .pollDelay(2, TimeUnit.SECONDS)
                .atMost(timeout, unit)
                .untilAsserted(assertion);
    }

    static void awaitAssert(ThrowingRunnable assertion) {
        awaitAssert(scaled(1), TimeUnit.MINUTES, assertion);
    }

    default void assertTopicsExist(String... names) {
        LOGGER.info("Awaiting topics " + Arrays.toString(names));
        try (Consumer<K, V> consumer = getConsumer()) {
            await().atMost(scaled(2), TimeUnit.MINUTES).untilAsserted(() -> {
                Set<String> topics = consumer.listTopics().keySet();
                assertThat(topics).contains(names);
            });
        }
    }

    default void assertRecordsCount(String topic, int count) {
        try (Consumer<K, V> consumer = getConsumer()) {
            consumer.subscribe(Collections.singleton(topic));
            ConsumerRecords<K, V> records = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            consumer.seekToBeginning(consumer.assignment());
            assertThat(records.count()).withFailMessage("Expecting topic '%s' to have <%d> messages but it had <%d>.", topic, count, records.count()).isEqualTo(count);
        }
    }

    default void assertMinimalRecordsCount(String topic, int count) {
        try (Consumer<K, V> consumer = getConsumer()) {
            consumer.subscribe(Collections.singleton(topic));
            ConsumerRecords<K, V> records = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            consumer.seekToBeginning(consumer.assignment());
            assertThat(
                    records.count()).withFailMessage("Expecting topic '%s' to have  at least <%d> messages but it had <%d>.", topic, count, records.count())
                            .isGreaterThanOrEqualTo(count);
        }
    }

    void assertRecordsContain(String topic, String content);

    Consumer<K, V> getConsumer();
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.naming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.sink.DebeziumSinkRecord;

class PassthroughCollectionNamingStrategyTest {

    private PassthroughCollectionNamingStrategy strategy;

    @BeforeEach
    void setUp() {
        strategy = new PassthroughCollectionNamingStrategy();
    }

    @Test
    void shouldPreserveDotsInTopicName() {
        DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
        when(record.topicName()).thenReturn("server1.schema.table");

        String result = strategy.resolveCollectionName(record, "${topic}");

        assertThat(result).isEqualTo("server1.schema.table");
    }

    @Test
    void shouldSubstituteTopicInFormat() {
        DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
        when(record.topicName()).thenReturn("my_topic");

        String result = strategy.resolveCollectionName(record, "prefix.${topic}");

        assertThat(result).isEqualTo("prefix.my_topic");
    }

    @Test
    void shouldReturnTopicNameWhenFormatIsNull() {
        DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
        when(record.topicName()).thenReturn("my_topic");

        String result = strategy.resolveCollectionName(record, null);

        assertThat(result).isEqualTo("my_topic");
    }

    @Test
    void shouldReturnTopicNameWhenFormatIsBlank() {
        DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
        when(record.topicName()).thenReturn("my_topic");

        String result = strategy.resolveCollectionName(record, "  ");

        assertThat(result).isEqualTo("my_topic");
    }
}

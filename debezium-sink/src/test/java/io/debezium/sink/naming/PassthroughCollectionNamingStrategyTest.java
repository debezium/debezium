/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.naming;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.TestSinkRecords;

class PassthroughCollectionNamingStrategyTest {

    private PassthroughCollectionNamingStrategy strategy;

    @BeforeEach
    void setUp() {
        strategy = new PassthroughCollectionNamingStrategy();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldPreserveDotsInTopicName() {
        DebeziumSinkRecord record = TestSinkRecords.flat("server1.schema.table", (byte) 1, "test");

        String result = strategy.resolveCollectionName(record, "${topic}");

        assertThat(result).isEqualTo("server1.schema.table");
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldSubstituteTopicInFormat() {
        DebeziumSinkRecord record = TestSinkRecords.flat("my_topic", (byte) 1, "test");

        String result = strategy.resolveCollectionName(record, "prefix.${topic}");

        assertThat(result).isEqualTo("prefix.my_topic");
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnTopicNameWhenFormatIsNull() {
        DebeziumSinkRecord record = TestSinkRecords.flat("my_topic", (byte) 1, "test");

        String result = strategy.resolveCollectionName(record, null);

        assertThat(result).isEqualTo("my_topic");
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnTopicNameWhenFormatIsBlank() {
        DebeziumSinkRecord record = TestSinkRecords.flat("my_topic", (byte) 1, "test");

        String result = strategy.resolveCollectionName(record, "  ");

        assertThat(result).isEqualTo("my_topic");
    }
}

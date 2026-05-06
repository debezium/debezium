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

class DefaultCollectionNamingStrategyTest {

    private DefaultCollectionNamingStrategy strategy;

    @BeforeEach
    void setUp() {
        strategy = new DefaultCollectionNamingStrategy();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReplaceDotsWithUnderscoresInTopicName() {
        DebeziumSinkRecord record = TestSinkRecords.flat("server1.schema.table", (byte) 1, "test");

        String result = strategy.resolveCollectionName(record, "${topic}");

        assertThat(result).isEqualTo("server1_schema_table");
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldSubstituteTopicInFormat() {
        DebeziumSinkRecord record = TestSinkRecords.flat("server1.schema.table", (byte) 1, "test");

        String result = strategy.resolveCollectionName(record, "prefix_${topic}");

        assertThat(result).isEqualTo("prefix_server1_schema_table");
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldResolveSourceFieldsFromDebeziumMessage() {
        DebeziumSinkRecord record = TestSinkRecords.debeziumCreate("server1.schema.table", (byte) 1, "John", "mydb", "mytable");

        String result = strategy.resolveCollectionName(record, "${source.db}.${source.table}");

        assertThat(result).isEqualTo("mydb.mytable");
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnNullWhenSourceFieldRequestedForNonDebeziumMessage() {
        DebeziumSinkRecord record = TestSinkRecords.flat("topic1", (byte) 1, "test");

        String result = strategy.resolveCollectionName(record, "${source.db}.${source.table}");

        assertThat(result).isNull();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldHandleTopicNameWithoutDots() {
        DebeziumSinkRecord record = TestSinkRecords.flat("simple_topic", (byte) 1, "test");

        String result = strategy.resolveCollectionName(record, "${topic}");

        assertThat(result).isEqualTo("simple_topic");
    }
}

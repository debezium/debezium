/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.naming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.sink.DebeziumSinkRecord;

class DefaultCollectionNamingStrategyTest {

    private DefaultCollectionNamingStrategy strategy;

    @BeforeEach
    void setUp() {
        strategy = new DefaultCollectionNamingStrategy();
    }

    @Test
    void shouldReplaceDotsWithUnderscoresInTopicName() {
        DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
        when(record.topicName()).thenReturn("server1.schema.table");

        String result = strategy.resolveCollectionName(record, "${topic}");

        assertThat(result).isEqualTo("server1_schema_table");
    }

    @Test
    void shouldSubstituteTopicInFormat() {
        DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
        when(record.topicName()).thenReturn("server1.schema.table");

        String result = strategy.resolveCollectionName(record, "prefix_${topic}");

        assertThat(result).isEqualTo("prefix_server1_schema_table");
    }

    @Test
    void shouldResolveSourceFieldsFromDebeziumMessage() {
        var sourceSchema = SchemaBuilder.struct()
                .field("db", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                .field("table", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                .build();
        var valueSchema = SchemaBuilder.struct()
                .field("source", sourceSchema)
                .build();
        Struct source = new Struct(sourceSchema).put("db", "mydb").put("table", "mytable");
        Struct value = new Struct(valueSchema).put("source", source);

        DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
        when(record.topicName()).thenReturn("server1.schema.table");
        when(record.isDebeziumMessage()).thenReturn(true);
        when(record.value()).thenReturn(value);

        String result = strategy.resolveCollectionName(record, "${source.db}.${source.table}");

        assertThat(result).isEqualTo("mydb.mytable");
    }

    @Test
    void shouldReturnNullWhenSourceFieldRequestedForNonDebeziumMessage() {
        DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
        when(record.topicName()).thenReturn("topic1");
        when(record.isDebeziumMessage()).thenReturn(false);

        String result = strategy.resolveCollectionName(record, "${source.db}.${source.table}");

        assertThat(result).isNull();
    }

    @Test
    void shouldHandleTopicNameWithoutDots() {
        DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
        when(record.topicName()).thenReturn("simple_topic");

        String result = strategy.resolveCollectionName(record, "${topic}");

        assertThat(result).isEqualTo("simple_topic");
    }
}

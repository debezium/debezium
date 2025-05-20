/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.jdbc;

import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.util.SinkRecordFactory;

@Tag("UnitTests")
class RecordBufferTest extends AbstractRecordBufferTest {

    @BeforeEach
    void setUp() {
        dialect = mock(DatabaseDialect.class);
        Type type = mock(Type.class);
        when(type.getTypeName(any(), anyBoolean())).thenReturn("");
        when(dialect.getSchemaType(any())).thenReturn(type);
    }

    private static @NotNull JdbcSinkConnectorConfig getJdbcSinkConnectorConfig() {
        return new JdbcSinkConnectorConfig(Map.of("batch.size", "5"));
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @DisplayName("When 10 sink records arrives and buffer size is 5 then the buffer will be flushed 2 times")
    void correctlyBuffer(SinkRecordFactory factory) {
        JdbcSinkConnectorConfig config = getJdbcSinkConnectorConfig();

        RecordBuffer recordBuffer = new RecordBuffer(config);
        List<JdbcSinkRecord> sinkRecords = IntStream.range(0, 10)
                .mapToObj(i -> createRecordNoPkFields(factory, (byte) i, config))
                .collect(Collectors.toList());

        List<List<JdbcSinkRecord>> batches = sinkRecords.stream().map(recordBuffer::add)
                .filter(not(List::isEmpty))
                .toList();

        assertThat(batches.size()).isEqualTo(2);

    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @DisplayName("When key schema changes then the buffer will be flushed")
    void keySchemaChange(SinkRecordFactory factory) {

        JdbcSinkConnectorConfig config = getJdbcSinkConnectorConfig();

        RecordBuffer recordBuffer = new RecordBuffer(config);

        List<JdbcSinkRecord> sinkRecords = IntStream.range(0, 3)
                .mapToObj(i -> createRecordNoPkFields(factory, (byte) i, config))
                .collect(Collectors.toList());

        KafkaDebeziumSinkRecord sinkRecordWithDifferentKeySchema = factory.updateBuilder()
                .name("prefix")
                .topic("topic")
                .keySchema(factory.keySchema(UnaryOperator.identity(), Schema.INT16_SCHEMA))
                .recordSchema(SchemaBuilder.struct().field("id", Schema.INT8_SCHEMA))
                .sourceSchema(factory.basicSourceSchema())
                .key("id", (short) 1)
                .before("id", (byte) 1)
                .after("id", (byte) 1)
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .build();

        sinkRecords.add(createRecord(sinkRecordWithDifferentKeySchema, config));

        List<List<JdbcSinkRecord>> batches = sinkRecords.stream().map(recordBuffer::add)
                .filter(not(List::isEmpty))
                .toList();

        assertThat(batches.size()).isEqualTo(1);

    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @DisplayName("When value schema changes then the buffer will be flushed")
    void valueSchemaChange(SinkRecordFactory factory) {

        JdbcSinkConnectorConfig config = getJdbcSinkConnectorConfig();

        RecordBuffer recordBuffer = new RecordBuffer(config);

        List<JdbcSinkRecord> sinkRecords = IntStream.range(0, 3)
                .mapToObj(i -> createRecordPkFieldId(factory, (byte) i, config))
                .collect(Collectors.toList());

        KafkaDebeziumSinkRecord sinkRecordWithDifferentValueSchema = factory.updateBuilder()
                .name("prefix")
                .topic("topic")
                .keySchema(factory.basicKeySchema())
                .recordSchema(SchemaBuilder.struct().field("id", Schema.INT16_SCHEMA))
                .sourceSchema(factory.basicSourceSchema())
                .key("id", (byte) 1)
                .before("id", (short) 1)
                .after("id", (short) 1)
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .build();

        sinkRecords.add(createRecord(sinkRecordWithDifferentValueSchema, config));

        List<List<JdbcSinkRecord>> batches = sinkRecords.stream().map(recordBuffer::add)
                .filter(not(List::isEmpty))
                .toList();

        assertThat(batches.size()).isEqualTo(1);

    }
}

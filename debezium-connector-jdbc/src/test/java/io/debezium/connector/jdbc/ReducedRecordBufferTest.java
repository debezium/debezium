/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.jdbc;

import static io.debezium.connector.jdbc.JdbcSinkConnectorConfig.PrimaryKeyMode.NONE;
import static io.debezium.connector.jdbc.JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY;
import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.util.SinkRecordFactory;

/**
 * Unit tests for the {@link ReducedRecordBuffer} class.
 *
 * @author Gaurav Miglani
 */
@Tag("UnitTests")
class ReducedRecordBufferTest {

    private DatabaseDialect dialect;

    @BeforeEach
    void setUp() {
        dialect = mock(DatabaseDialect.class);
        Type type = mock(Type.class);
        when(type.getTypeName(eq(dialect), any(), anyBoolean())).thenReturn("");
        when(dialect.getSchemaType(any())).thenReturn(type);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @DisplayName("When 10 sink records arrives and buffer size is 5 then the buffer will be flushed 2 times")
    void correctlyBuffer(SinkRecordFactory factory) {

        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of("batch.size", "5", "primary.key.mode", "record_key","primary.key.fields", "id"));

        ReducedRecordBuffer reducedRecordBuffer = new ReducedRecordBuffer(config);

        List<SinkRecordDescriptor> sinkRecords = IntStream.range(0, 10)
                .mapToObj(i -> SinkRecordDescriptor.builder()
                        .withSinkRecord(factory.createRecord("topic", (byte) i))
                        .withDialect(dialect)
                        .withPrimaryKeyFields(Set.of("id"))
                        .withPrimaryKeyMode(RECORD_KEY)
                        .build())
                .collect(Collectors.toList());

        List<List<SinkRecordDescriptor>> batches = sinkRecords.stream().map(reducedRecordBuffer::add)
                .filter(not(List::isEmpty))
                .collect(Collectors.toList());

        assertThat(batches.size()).isEqualTo(2);

    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @DisplayName("When key schema changes then the buffer will be flushed")
    void keySchemaChange(SinkRecordFactory factory) {

        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of("batch.size", "5", "primary.key.mode", "record_key","primary.key.fields", "id"));

        ReducedRecordBuffer reducedRecordBuffer = new ReducedRecordBuffer(config);

        List<SinkRecordDescriptor> sinkRecords = IntStream.range(0, 3)
                .mapToObj(i -> SinkRecordDescriptor.builder()
                        .withSinkRecord(factory.createRecord("topic", (byte) i))
                        .withDialect(dialect)
                        .withPrimaryKeyFields(Set.of("id"))
                        .withPrimaryKeyMode(RECORD_KEY)
                        .build())
                .collect(Collectors.toList());

        SinkRecord sinkRecordWithDifferentKeySchema = factory.updateBuilder()
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

        sinkRecords.add(SinkRecordDescriptor.builder()
                .withSinkRecord(sinkRecordWithDifferentKeySchema)
                .withDialect(dialect)
                .withPrimaryKeyFields(Set.of("id"))
                .withPrimaryKeyMode(RECORD_KEY)
                .build());

        List<List<SinkRecordDescriptor>> batches = sinkRecords.stream().map(reducedRecordBuffer::add)
                .filter(not(List::isEmpty))
                .collect(Collectors.toList());

        assertThat(batches.size()).isEqualTo(1);

    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @DisplayName("When value schema changes then the buffer will be flushed")
    void valueSchemaChange(SinkRecordFactory factory) {

        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of("batch.size", "5", "primary.key.mode", "record_key","primary.key.fields", "id"));

        ReducedRecordBuffer reducedRecordBuffer = new ReducedRecordBuffer(config);

        List<SinkRecordDescriptor> sinkRecords = IntStream.range(0, 3)
                .mapToObj(i -> SinkRecordDescriptor.builder()
                        .withSinkRecord(factory.createRecord("topic", (byte) i))
                        .withDialect(dialect)
                        .withPrimaryKeyFields(Set.of("id"))
                        .withPrimaryKeyMode(RECORD_KEY)
                        .build())
                .collect(Collectors.toList());

        SinkRecord sinkRecordWithDifferentValueSchema = factory.updateBuilder()
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

        sinkRecords.add(SinkRecordDescriptor.builder()
                .withSinkRecord(sinkRecordWithDifferentValueSchema)
                .withDialect(dialect)
                .withPrimaryKeyFields(Set.of("id"))
                .withPrimaryKeyMode(RECORD_KEY)
                .build());

        List<List<SinkRecordDescriptor>> batches = sinkRecords.stream().map(reducedRecordBuffer::add)
                .filter(not(List::isEmpty))
                .collect(Collectors.toList());

        assertThat(batches.size()).isEqualTo(1);

    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @DisplayName("When 10 sink records arrives and buffer size is 5 with every alternate duplicate sink record then the buffer will be flushed 1 time")
    void correctlyBufferWithDuplicate(SinkRecordFactory factory) {

        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of("batch.size", "5", "primary.key.mode", "record_key","primary.key.fields", "id"));

        ReducedRecordBuffer reducedRecordBuffer = new ReducedRecordBuffer(config);

        List<SinkRecordDescriptor> sinkRecords = IntStream.range(0, 10)
                .mapToObj(i -> SinkRecordDescriptor.builder()
                        .withSinkRecord(factory.createRecord("topic", (byte) (i % 2 == 0 ? i : i - 1)))
                        .withDialect(dialect)
                        .withPrimaryKeyFields(Set.of("id"))
                        .withPrimaryKeyMode(RECORD_KEY)
                        .build())
                .collect(Collectors.toList());

        List<List<SinkRecordDescriptor>> batches = sinkRecords.stream().map(reducedRecordBuffer::add)
                .filter(not(List::isEmpty))
                .collect(Collectors.toList());

        assertThat(batches.size()).isEqualTo(1);
        assertThat(batches.get(0).size()).isEqualTo(5);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @DisplayName("When primary key mode is none then reduced buffer should raise exception")
    void raiseExceptionWithoutPrimaryKey(SinkRecordFactory factory) {

        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of("batch.size", "5"));

        ReducedRecordBuffer reducedRecordBuffer = new ReducedRecordBuffer(config);

        List<SinkRecordDescriptor> sinkRecords = IntStream.range(0, 10)
                .mapToObj(i -> SinkRecordDescriptor.builder()
                        .withSinkRecord(factory.createRecord("topic", (byte) i))
                        .withDialect(dialect)
                        .withPrimaryKeyFields(Set.of())
                        .withPrimaryKeyMode(NONE)
                        .build())
                .collect(Collectors.toList());

        Stream<List<SinkRecordDescriptor>> batchesFilter = sinkRecords.stream().map(reducedRecordBuffer::add)
                .filter(not(List::isEmpty));

        Exception thrown = Assertions.assertThrows(ConnectException.class, () -> batchesFilter.collect(Collectors.toList()));
        assertThat(thrown.getMessage()).isEqualTo("No struct-based primary key defined for record key/value, reduction buffer require struct based primary key");

    }
}

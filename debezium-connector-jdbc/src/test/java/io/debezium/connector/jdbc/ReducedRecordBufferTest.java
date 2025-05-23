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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

/**
 * Unit tests for the {@link ReducedRecordBuffer} class.
 *
 * @author Gaurav Miglani
 */
@Tag("UnitTests")
class ReducedRecordBufferTest extends AbstractRecordBufferTest {

    @BeforeEach
    void setUp() {
        dialect = mock(DatabaseDialect.class);
        JdbcType type = mock(JdbcType.class);
        when(type.getTypeName(any(), anyBoolean())).thenReturn("");
        when(dialect.getSchemaType(any())).thenReturn(type);
    }

    protected JdbcSinkConnectorConfig getJdbcConnectorConfig(PrimaryKeyMode primaryKeyMode, String primaryKeyFields) {
        if (null == primaryKeyFields) {
            return new JdbcSinkConnectorConfig(
                    Map.of(
                            SinkConnectorConfig.BATCH_SIZE, "5",
                            SinkConnectorConfig.PRIMARY_KEY_MODE, primaryKeyMode.getValue()));

        }
        return new JdbcSinkConnectorConfig(
                Map.of(
                        SinkConnectorConfig.BATCH_SIZE, "5",
                        SinkConnectorConfig.PRIMARY_KEY_MODE, primaryKeyMode.getValue(),
                        JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, primaryKeyFields));
    }

    protected JdbcSinkConnectorConfig getJdbcConnectorConfig() {
        return getJdbcConnectorConfig(PrimaryKeyMode.RECORD_KEY, "id");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @DisplayName("When 10 sink records arrives and buffer size is 5 then the buffer will be flushed 2 times")
    void correctlyBuffer(SinkRecordFactory factory) {

        JdbcSinkConnectorConfig config = getJdbcConnectorConfig();

        ReducedRecordBuffer reducedRecordBuffer = new ReducedRecordBuffer(config);

        List<JdbcSinkRecord> sinkRecords = IntStream.range(0, 10)
                .mapToObj(i -> createRecordPkFieldId(factory, (byte) i, config))
                .collect(Collectors.toList());

        List<List<JdbcSinkRecord>> batches = sinkRecords.stream().map(reducedRecordBuffer::add)
                .filter(not(List::isEmpty))
                .toList();

        assertThat(batches.size()).isEqualTo(2);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @DisplayName("When 4 sink records {0,1,2,3} arrives then 2 of them {0,2}" +
            " removed final flush will have {1,3,4,5,6} since buffer size is 5")
    void correctlyBufferOnRemoving(SinkRecordFactory factory) {
        JdbcSinkConnectorConfig config = getJdbcConnectorConfig();

        ReducedRecordBuffer reducedRecordBuffer = new ReducedRecordBuffer(config);

        List<JdbcKafkaSinkRecord> records = IntStream.range(0, 7)
                .mapToObj(i -> createRecordPkFieldId(factory, (byte) i, config))
                .toList();

        IntStream.range(0, 4)
                .forEach(i -> reducedRecordBuffer.add(records.get(i)));

        reducedRecordBuffer.remove(records.get(0));
        reducedRecordBuffer.remove(records.get(2));

        List<List<JdbcSinkRecord>> batches = IntStream.range(4, records.size())
                .mapToObj(i -> reducedRecordBuffer.add(records.get(i)))
                .filter(not(List::isEmpty))
                .toList();
        assertThat(batches.size()).isEqualTo(1);
        List<JdbcSinkRecord> batch = batches.get(0);
        batch.sort(Comparator.comparing(
                record -> (byte) ((Struct) record.key()).get("id")));
        assertThat(batch.size()).isEqualTo(5);
        assertThat(batch.get(0)).isEqualTo(records.get(1));
        assertThat(batch.get(1)).isEqualTo(records.get(3));
        assertThat(batch.get(2)).isEqualTo(records.get(4));
        assertThat(batch.get(3)).isEqualTo(records.get(5));
        assertThat(batch.get(4)).isEqualTo(records.get(6));
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @DisplayName("When key schema changes then the buffer will be flushed")
    void keySchemaChange(SinkRecordFactory factory) {

        JdbcSinkConnectorConfig config = getJdbcConnectorConfig();

        ReducedRecordBuffer reducedRecordBuffer = new ReducedRecordBuffer(config);

        List<JdbcSinkRecord> sinkRecords = IntStream.range(0, 3)
                .mapToObj(i -> createRecordPkFieldId(factory, (byte) i, config))
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

        List<List<JdbcSinkRecord>> batches = sinkRecords.stream().map(reducedRecordBuffer::add)
                .filter(not(List::isEmpty))
                .toList();

        assertThat(batches.size()).isEqualTo(1);

    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @DisplayName("When value schema changes then the buffer will be flushed")
    void valueSchemaChange(SinkRecordFactory factory) {

        JdbcSinkConnectorConfig config = getJdbcConnectorConfig();

        ReducedRecordBuffer reducedRecordBuffer = new ReducedRecordBuffer(config);

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

        List<List<JdbcSinkRecord>> batches = sinkRecords.stream().map(reducedRecordBuffer::add)
                .filter(not(List::isEmpty))
                .toList();

        assertThat(batches.size()).isEqualTo(1);

    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @DisplayName("When 10 sink records arrives and buffer size is 5 with every alternate duplicate sink record then the buffer will be flushed 1 time")
    void correctlyBufferWithDuplicate(SinkRecordFactory factory) {

        JdbcSinkConnectorConfig config = getJdbcConnectorConfig();

        ReducedRecordBuffer reducedRecordBuffer = new ReducedRecordBuffer(config);

        List<JdbcSinkRecord> sinkRecords = IntStream.range(0, 10)
                .mapToObj(i -> createRecordPkFieldId(factory, (byte) (i % 2 == 0 ? i : i - 1), config))
                .collect(Collectors.toList());

        List<List<JdbcSinkRecord>> batches = sinkRecords.stream().map(reducedRecordBuffer::add)
                .filter(not(List::isEmpty))
                .toList();

        assertThat(batches.size()).isEqualTo(1);
        assertThat(batches.get(0).size()).isEqualTo(5);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @DisplayName("When primary key mode is none then reduced buffer should raise exception")
    void raiseExceptionWithoutPrimaryKeyOnAdding(SinkRecordFactory factory) {
        JdbcSinkConnectorConfig config = getJdbcConnectorConfig(PrimaryKeyMode.NONE, null);

        ReducedRecordBuffer reducedRecordBuffer = new ReducedRecordBuffer(config);

        List<JdbcSinkRecord> sinkRecords = IntStream.range(0, 10)
                .mapToObj(i -> createRecordNoPkFields(factory, (byte) i, config))
                .collect(Collectors.toList());

        Stream<List<JdbcSinkRecord>> batchesFilter = sinkRecords.stream().map(reducedRecordBuffer::add)
                .filter(not(List::isEmpty));

        Exception thrown = Assertions.assertThrows(ConnectException.class, batchesFilter::toList);
        assertThat(thrown.getMessage()).isEqualTo("No struct-based primary key defined for record key/value, reduction buffer require struct based primary key");

    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @DisplayName("When primary key columns are in record value then reduced buffer should work as expected")
    void primaryKeyInValue(SinkRecordFactory factory) {
        JdbcSinkConnectorConfig config = getJdbcConnectorConfig(PrimaryKeyMode.RECORD_VALUE, "value_id");

        ReducedRecordBuffer reducedRecordBuffer = new ReducedRecordBuffer(config);

        List<JdbcSinkRecord> sinkRecords = IntStream.range(0, 10)
                .mapToObj(i -> {
                    KafkaDebeziumSinkRecord record = factory.createRecordWithSchemaValue(
                            "topic",
                            (byte) (1),
                            List.of("value_id", "name"),
                            List.of(SchemaBuilder.type(Schema.INT8_SCHEMA.type()).optional().build(),
                                    SchemaBuilder.type(Schema.STRING_SCHEMA.type()).optional().build()),
                            Arrays.asList((byte) (i % 2 == 0 ? i : i - 1), "John Doe " + i));
                    return new JdbcKafkaSinkRecord(
                            record.getOriginalKafkaRecord(),
                            config.getPrimaryKeyMode(),
                            config.getPrimaryKeyFields(),
                            config.getFieldFilter(),
                            config.cloudEventsSchemaNamePattern(),
                            dialect);
                })
                .collect(Collectors.toList());

        List<List<JdbcSinkRecord>> batches = sinkRecords.stream().map(reducedRecordBuffer::add)
                .filter(not(List::isEmpty))
                .toList();

        assertThat(batches.size()).isEqualTo(1);
        assertThat(batches.get(0).size()).isEqualTo(5);

        batches.get(0).forEach(record -> {
            Struct keyStruct = record.filteredKey();
            assertThat(keyStruct).isNotNull();
            assertThat(keyStruct.schema().fields()).hasSize(1);
            assertThat(keyStruct.schema().field("value_id")).isNotNull();

            // Verify the value_id matches what we expect (even numbers 0,2,4,6,8)
            byte expectedValue = record.getPayload().getInt8("value_id");
            assertThat(keyStruct.get("value_id")).isEqualTo(expectedValue);

            // Verify the name matches what we expect (odd numbers in the last 1, 3, 5, 7)
            // 9 is not included because the buffer is flushed after 5 records, it will be in the next batch
            if (expectedValue < 8) {
                String expectedName = "John Doe " + (expectedValue + 1);
                assertThat(record.getPayload().getString("name")).isEqualTo(expectedName);
            }
        });
    }
}

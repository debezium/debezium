/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.jdbc.util.DebeziumSinkRecordFactory;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.converters.spi.SerializerType;
import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link ConvertCloudEventToSaveableForm}
 *
 * @author Roman Kudryashov
 */
class ConvertCloudEventToSaveableFormTest {

    @Test
    @FixFor({ "DBZ-7065", "DBZ-7130" })
    void testConvertCloudEventRecordWithEmptyConfig() {
        try (ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm()) {
            final Map<String, String> config = new HashMap<>();

            Exception exception = assertThrows(ConfigException.class, () -> transform.configure(config));

            assertThat(exception.getMessage())
                    .isEqualTo("Invalid value null for configuration serializer.type: Serialization/deserialization type of CloudEvents converter is required");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "json", "avro" })
    @FixFor({ "DBZ-7065", "DBZ-7130" })
    void testConvertNotCloudEventRecord(String serializerType) {
        try (ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm()) {
            final Map<String, String> config = new HashMap<>();
            config.put("serializer.type", serializerType);
            transform.configure(config);

            final SinkRecordFactory factory = new DebeziumSinkRecordFactory();

            final SinkRecord createRecord = factory.createRecord("test.topic");
            assertThat(createRecord.valueSchema().name()).doesNotEndWith(".CloudEvents.Envelope");

            final SinkRecord convertedRecord = transform.apply(createRecord);
            assertThat(convertedRecord).isEqualTo(createRecord);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "json", "avro" })
    @FixFor({ "DBZ-7065", "DBZ-7130" })
    void testConvertCloudEventRecordWithEmptyMapping(String serializerType) {
        try (ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm()) {
            final Map<String, String> config = new HashMap<>();
            config.put("serializer.type", serializerType);
            transform.configure(config);

            final SinkRecordFactory factory = new DebeziumSinkRecordFactory();

            final SinkRecord cloudEventRecord = factory.cloudEventRecord("test.topic", SerializerType.withName(serializerType), null);
            if (serializerType.equals("avro")) {
                assertThat(cloudEventRecord.valueSchema().name()).endsWith(".CloudEvents.Envelope");
                assertThat(cloudEventRecord.valueSchema().fields().size()).isEqualTo(7);
                assertThat(cloudEventRecord.valueSchema().field("id").schema()).isEqualTo(Schema.STRING_SCHEMA);
            }

            final SinkRecord convertedRecord = transform.apply(cloudEventRecord);
            assertThat(convertedRecord).isEqualTo(cloudEventRecord);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "json", "avro" })
    @FixFor({ "DBZ-7065", "DBZ-7130" })
    void testConvertCloudEventRecordWithMappingOfIdField(String serializerType) {
        try (ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm()) {
            final Map<String, String> config = new HashMap<>();
            config.put("fields.mapping", "id");
            config.put("serializer.type", serializerType);
            transform.configure(config);

            final SinkRecordFactory factory = new DebeziumSinkRecordFactory();

            final SinkRecord cloudEventRecord = factory.cloudEventRecord("test.topic", SerializerType.withName(serializerType), null);
            if (serializerType.equals("avro")) {
                assertThat(cloudEventRecord.valueSchema().name()).endsWith(".CloudEvents.Envelope");
                assertThat(cloudEventRecord.valueSchema().fields().size()).isEqualTo(7);
                assertThat(cloudEventRecord.valueSchema().field("id").schema()).isEqualTo(Schema.STRING_SCHEMA);
            }

            final SinkRecord convertedRecord = transform.apply(cloudEventRecord);
            assertThat(convertedRecord).isNotNull();
            assertThat(convertedRecord).isNotEqualTo(cloudEventRecord);
            assertThat(convertedRecord.valueSchema().type()).isEqualTo(Schema.Type.STRUCT);
            assertThat(convertedRecord.valueSchema().name()).isNull();
            assertThat(convertedRecord.valueSchema().fields().size()).isEqualTo(1);
            assertThat(convertedRecord.valueSchema().field("id").schema()).isEqualTo(Schema.STRING_SCHEMA);
            assertThat(convertedRecord.value()).isInstanceOf(Struct.class);
            assertThat(((Struct) convertedRecord.value()).getString("id")).isNotBlank();
            checkParamsOfOriginalAndConvertedRecordsAreEqual(cloudEventRecord, convertedRecord);
        }
    }

    @Test
    @FixFor("DBZ-7235")
    void testConvertCloudEventRecordWithNotConfiguredCloudEventsSchemaCustomNameAndMappingOfIdField() {
        try (ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm()) {
            final Map<String, String> config = new HashMap<>();
            config.put("fields.mapping", "id");
            // the test is not applicable to `json` because in that case the schema name is not checked by CloudEventsValidator
            config.put("serializer.type", "avro");
            transform.configure(config);

            final SinkRecordFactory factory = new DebeziumSinkRecordFactory();

            final SinkRecord cloudEventRecord = factory.cloudEventRecord("test.topic", SerializerType.withName("avro"), "TestCESchemaCustomName");
            assertThat(cloudEventRecord.valueSchema().name()).isEqualTo("TestCESchemaCustomName");
            assertThat(cloudEventRecord.valueSchema().fields().size()).isEqualTo(7);
            assertThat(cloudEventRecord.valueSchema().field("id").schema()).isEqualTo(Schema.STRING_SCHEMA);

            final SinkRecord convertedRecord = transform.apply(cloudEventRecord);
            assertThat(convertedRecord).isNotNull();
            // main check: the record was not converted. This is because the transform was not configured with a custom CloudEvents schema name
            // but the incoming record had a custom name so CloudEventsValidator decided it is not a valid CloudEvent record
            assertThat(convertedRecord).isEqualTo(cloudEventRecord);
        }
    }

    @Test
    @FixFor("DBZ-7235")
    void testConvertCloudEventRecordWithMisconfiguredCloudEventsSchemaCustomNameAndMappingOfIdField() {
        try (ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm()) {
            final Map<String, String> config = new HashMap<>();
            config.put("fields.mapping", "id");
            // the test is not applicable to `json` because in that case the schema name is not checked by CloudEventsValidator
            config.put("serializer.type", "avro");
            config.put("schema.cloudevents.name", "TestCESchemaCustomName");
            transform.configure(config);

            final SinkRecordFactory factory = new DebeziumSinkRecordFactory();

            final SinkRecord cloudEventRecord = factory.cloudEventRecord("test.topic", SerializerType.withName("avro"), null);
            assertThat(cloudEventRecord.valueSchema().name()).isEqualTo("test.test.CloudEvents.Envelope");
            assertThat(cloudEventRecord.valueSchema().fields().size()).isEqualTo(7);
            assertThat(cloudEventRecord.valueSchema().field("id").schema()).isEqualTo(Schema.STRING_SCHEMA);

            final SinkRecord convertedRecord = transform.apply(cloudEventRecord);
            assertThat(convertedRecord).isNotNull();
            // main check: the record was not converted. This is because the transform was configured with a custom CloudEvents schema name
            // but the incoming record had a generated by default name so CloudEventsValidator decided it is not a valid CloudEvent record
            assertThat(convertedRecord).isEqualTo(cloudEventRecord);
        }
    }

    @Test
    @FixFor("DBZ-7235")
    void testConvertCloudEventRecordWithConfiguredCloudEventsSchemaCustomNameAndMappingOfIdField() {
        try (ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm()) {
            final Map<String, String> config = new HashMap<>();
            config.put("fields.mapping", "id");
            // the test is not applicable to `json` because in that case the schema name is not checked by CloudEventsValidator
            config.put("serializer.type", "avro");
            config.put("schema.cloudevents.name", "TestCESchemaCustomName");
            transform.configure(config);

            final SinkRecordFactory factory = new DebeziumSinkRecordFactory();

            final SinkRecord cloudEventRecord = factory.cloudEventRecord("test.topic", SerializerType.withName("avro"), "TestCESchemaCustomName");
            assertThat(cloudEventRecord.valueSchema().name()).isEqualTo("TestCESchemaCustomName");
            assertThat(cloudEventRecord.valueSchema().fields().size()).isEqualTo(7);
            assertThat(cloudEventRecord.valueSchema().field("id").schema()).isEqualTo(Schema.STRING_SCHEMA);

            final SinkRecord convertedRecord = transform.apply(cloudEventRecord);
            assertThat(convertedRecord).isNotNull();
            // main check: the record was converted. This is because the transform was configured with a custom CloudEvents schema name
            // and the incoming record had the same custom name so CloudEventsValidator decided it is a valid CloudEvent record
            assertThat(convertedRecord).isNotEqualTo(cloudEventRecord);
            assertThat(convertedRecord.valueSchema().type()).isEqualTo(Schema.Type.STRUCT);
            assertThat(convertedRecord.valueSchema().name()).isNull();
            checkParamsOfOriginalAndConvertedRecordsAreEqual(cloudEventRecord, convertedRecord);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "json", "avro" })
    @FixFor({ "DBZ-7065", "DBZ-7130" })
    void testConvertCloudEventRecordWithMappingOfDataField(String serializerType) {
        try (ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm()) {
            final Map<String, String> config = new HashMap<>();
            config.put("fields.mapping", "data");
            config.put("serializer.type", serializerType);
            transform.configure(config);

            final SinkRecordFactory factory = new DebeziumSinkRecordFactory();

            final SinkRecord cloudEventRecord = factory.cloudEventRecord("test.topic", SerializerType.withName(serializerType), null);
            if (serializerType.equals("avro")) {
                assertThat(cloudEventRecord.valueSchema().name()).endsWith(".CloudEvents.Envelope");
                assertThat(cloudEventRecord.valueSchema().fields().size()).isEqualTo(7);
                assertThat(cloudEventRecord.valueSchema().field("data").schema().type()).isEqualTo(Schema.Type.STRUCT);
            }

            final SinkRecord convertedRecord = transform.apply(cloudEventRecord);
            assertThat(convertedRecord).isNotNull();
            assertThat(convertedRecord).isNotEqualTo(cloudEventRecord);
            assertThat(convertedRecord.valueSchema().type()).isEqualTo(Schema.Type.STRUCT);
            assertThat(convertedRecord.valueSchema().name()).isNull();
            assertThat(convertedRecord.valueSchema().fields().size()).isEqualTo(1);
            assertThat(convertedRecord.valueSchema().field("data").schema()).isEqualTo(Schema.STRING_SCHEMA);
            assertThat(convertedRecord.value()).isInstanceOf(Struct.class);
            assertThat(((Struct) convertedRecord.value()).getString("data")).isNotBlank();
            checkParamsOfOriginalAndConvertedRecordsAreEqual(cloudEventRecord, convertedRecord);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "json", "avro" })
    @FixFor({ "DBZ-7065", "DBZ-7130" })
    void testConvertCloudEventRecordWithMappingOfAllFieldsWithCustomNames(String serializerType) {
        try (ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm()) {
            final Map<String, String> config = new HashMap<>();
            config.put("fields.mapping", "id,source:created_by,specversion:ce_spec_number,type,time:created_at,datacontenttype:payload_format,data:payload");
            config.put("serializer.type", serializerType);
            transform.configure(config);

            final SinkRecordFactory factory = new DebeziumSinkRecordFactory();

            final SinkRecord cloudEventRecord = factory.cloudEventRecord("test.topic", SerializerType.withName(serializerType), null);
            if (serializerType.equals("avro")) {
                assertThat(cloudEventRecord.valueSchema().name()).endsWith(".CloudEvents.Envelope");
                assertThat(cloudEventRecord.valueSchema().fields().size()).isEqualTo(7);
                assertThat(cloudEventRecord.valueSchema().field("data").schema().type()).isEqualTo(Schema.Type.STRUCT);
            }

            final SinkRecord convertedRecord = transform.apply(cloudEventRecord);
            assertThat(convertedRecord).isNotNull();
            assertThat(convertedRecord).isNotEqualTo(cloudEventRecord);
            assertThat(convertedRecord.valueSchema().type()).isEqualTo(Schema.Type.STRUCT);
            assertThat(convertedRecord.valueSchema().name()).isNull();
            assertThat(convertedRecord.valueSchema().fields().size()).isEqualTo(7);
            assertThat(convertedRecord.value()).isInstanceOf(Struct.class);
            Struct convertedRecordValue = (Struct) convertedRecord.value();
            assertThat(convertedRecordValue.getString("id")).isNotBlank();
            assertThat(convertedRecordValue.getString("created_by")).isNotBlank();
            assertThat(convertedRecordValue.getString("ce_spec_number")).isNotBlank();
            assertThat(convertedRecordValue.getString("type")).isNotBlank();
            assertThat(convertedRecordValue.getString("created_at")).isNotBlank();
            assertThat(convertedRecordValue.getString("payload_format")).isNotBlank();
            assertThat(convertedRecordValue.getString("payload")).isNotBlank();
            checkParamsOfOriginalAndConvertedRecordsAreEqual(cloudEventRecord, convertedRecord);
        }
    }

    private void checkParamsOfOriginalAndConvertedRecordsAreEqual(SinkRecord original, SinkRecord converted) {
        assertThat(converted.topic()).isEqualTo(original.topic());
        assertThat(converted.kafkaPartition()).isEqualTo(original.originalKafkaPartition());
        assertThat(converted.kafkaOffset()).isEqualTo(original.originalKafkaOffset());
        assertThat(converted.keySchema()).isEqualTo(original.keySchema());
        assertThat(converted.key()).isEqualTo(original.key());
        assertThat(converted.headers()).isEqualTo(original.headers());
        assertThat(converted.timestamp()).isEqualTo(original.timestamp());
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.transform;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.util.DebeziumSinkRecordFactory;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link ConvertCloudEventToSaveableFormTest}
 *
 * @author Roman Kudryashov
 */
class ConvertCloudEventToSaveableFormTest {

    @Test
    @FixFor("DBZ-7065")
    void testConvertNotCloudEventRecord() {
        try (ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm()) {
            final Map<String, String> config = new HashMap<>();
            transform.configure(config);

            final SinkRecordFactory factory = new DebeziumSinkRecordFactory();

            final SinkRecord createRecord = factory.createRecord("test.topic");
            assertThat(createRecord.valueSchema().name()).doesNotEndWith(".CloudEvents.Envelope");

            final SinkRecord convertedRecord = transform.apply(createRecord);
            assertThat(convertedRecord).isNull();
        }
    }

    @Test
    @FixFor("DBZ-7065")
    void testConvertCloudEventRecordWithEmptyConfig() {
        try (ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm()) {
            final Map<String, String> config = new HashMap<>();
            transform.configure(config);

            final SinkRecordFactory factory = new DebeziumSinkRecordFactory();

            final SinkRecord cloudEventRecord = factory.cloudEventRecord("test.topic");
            assertThat(cloudEventRecord.valueSchema().name()).endsWith(".CloudEvents.Envelope");
            assertThat(cloudEventRecord.valueSchema().fields().size()).isEqualTo(7);
            assertThat(cloudEventRecord.valueSchema().field("id").schema()).isEqualTo(Schema.STRING_SCHEMA);

            final SinkRecord convertedRecord = transform.apply(cloudEventRecord);
            assertThat(convertedRecord).isNull();
        }
    }

    @Test
    @FixFor("DBZ-7065")
    void testConvertCloudEventRecordWithMappingOfIdField() {
        try (ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm()) {
            final Map<String, String> config = new HashMap<>();
            config.put("fields.mapping", "id");
            transform.configure(config);

            final SinkRecordFactory factory = new DebeziumSinkRecordFactory();

            final SinkRecord cloudEventRecord = factory.cloudEventRecord("test.topic");
            assertThat(cloudEventRecord.valueSchema().name()).endsWith(".CloudEvents.Envelope");
            assertThat(cloudEventRecord.valueSchema().fields().size()).isEqualTo(7);
            assertThat(cloudEventRecord.valueSchema().field("id").schema()).isEqualTo(Schema.STRING_SCHEMA);

            final SinkRecord convertedRecord = transform.apply(cloudEventRecord);
            assertThat(convertedRecord).isNotNull();
            assertThat(convertedRecord.valueSchema().type()).isEqualTo(Schema.Type.STRUCT);
            assertThat(convertedRecord.valueSchema().fields().size()).isEqualTo(1);
            assertThat(convertedRecord.valueSchema().field("id").schema()).isEqualTo(Schema.STRING_SCHEMA);
            assertThat(convertedRecord.value()).isInstanceOf(Struct.class);
            assertThat(((Struct) convertedRecord.value()).getString("id")).isNotBlank();
            checkParamsOfOriginalAndConvertedRecordsAreEqual(convertedRecord, cloudEventRecord);
        }
    }

    @Test
    @FixFor("DBZ-7065")
    void testConvertCloudEventRecordWithMappingOfDataField() {
        try (ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm()) {
            final Map<String, String> config = new HashMap<>();
            config.put("fields.mapping", "data");
            transform.configure(config);

            final SinkRecordFactory factory = new DebeziumSinkRecordFactory();

            final SinkRecord cloudEventRecord = factory.cloudEventRecord("test.topic");
            assertThat(cloudEventRecord.valueSchema().name()).endsWith(".CloudEvents.Envelope");
            assertThat(cloudEventRecord.valueSchema().fields().size()).isEqualTo(7);
            assertThat(cloudEventRecord.valueSchema().field("data").schema().type()).isEqualTo(Schema.Type.STRUCT);

            final SinkRecord convertedRecord = transform.apply(cloudEventRecord);
            assertThat(convertedRecord).isNotNull();
            assertThat(convertedRecord.valueSchema().type()).isEqualTo(Schema.Type.STRUCT);
            assertThat(convertedRecord.valueSchema().fields().size()).isEqualTo(1);
            assertThat(convertedRecord.valueSchema().field("data").schema()).isEqualTo(Schema.STRING_SCHEMA);
            assertThat(convertedRecord.value()).isInstanceOf(Struct.class);
            assertThat(((Struct) convertedRecord.value()).getString("data")).isNotBlank();
            checkParamsOfOriginalAndConvertedRecordsAreEqual(convertedRecord, cloudEventRecord);
        }
    }

    @Test
    @FixFor("DBZ-7065")
    void testConvertCloudEventRecordWithMappingOfAllFieldsWithCustomNames() {
        try (ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm()) {
            final Map<String, String> config = new HashMap<>();
            config.put("fields.mapping", "id,source:created_by,specversion:ce_spec_number,type,time:created_at,datacontenttype:payload_format,data:payload");
            transform.configure(config);

            final SinkRecordFactory factory = new DebeziumSinkRecordFactory();

            final SinkRecord cloudEventRecord = factory.cloudEventRecord("test.topic");
            assertThat(cloudEventRecord.valueSchema().name()).endsWith(".CloudEvents.Envelope");
            assertThat(cloudEventRecord.valueSchema().fields().size()).isEqualTo(7);
            assertThat(cloudEventRecord.valueSchema().field("data").schema().type()).isEqualTo(Schema.Type.STRUCT);

            final SinkRecord convertedRecord = transform.apply(cloudEventRecord);
            assertThat(convertedRecord).isNotNull();
            assertThat(convertedRecord.valueSchema().type()).isEqualTo(Schema.Type.STRUCT);
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
            checkParamsOfOriginalAndConvertedRecordsAreEqual(convertedRecord, cloudEventRecord);
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

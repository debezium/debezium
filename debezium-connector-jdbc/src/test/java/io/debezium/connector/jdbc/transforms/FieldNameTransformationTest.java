/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.transforms;

import static org.fest.assertions.Assertions.assertThat;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.fest.assertions.ListAssert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.NamingStyle;
import io.debezium.connector.jdbc.util.SinkRecordBuilder;
import io.debezium.connector.jdbc.util.SinkRecordBuilder.SinkRecordTypeBuilder;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;

/**
 * Tests for the FieldNameTransformation.
 *
 * @author Chris Cranford
 */
public class FieldNameTransformationTest {

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertFieldNameDefaultStyle(SinkRecordFactory factory) {
        try (FieldNameTransformation<SinkRecord> transform = new FieldNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            transform.configure(properties);

            var record = new KafkaDebeziumSinkRecord(transform.apply(createSinkRecord(factory, "id", "id", "name", "nick_name_")),
                    new JdbcSinkConnectorConfig(properties).cloudEventsSchemaNamePattern());
            assertSchemaFieldNames(record.keySchema()).containsOnly("id");
            assertSchemaFieldNames(record.getPayload().schema()).containsOnly("id", "name", "nick_name_");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertFieldNameDefaultStyleWithPrefixSuffix(SinkRecordFactory factory) {
        try (FieldNameTransformation<SinkRecord> transform = new FieldNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("column.naming.prefix", "aa");
            properties.put("column.naming.suffix", "bb");
            transform.configure(properties);

            var record = new KafkaDebeziumSinkRecord(transform.apply(createSinkRecord(factory, "id", "id", "name", "nick_name_")),
                    new JdbcSinkConnectorConfig(properties).cloudEventsSchemaNamePattern());
            assertSchemaFieldNames(record.keySchema()).containsOnly("aaidbb");
            assertSchemaFieldNames(record.getPayload().schema()).containsOnly("aaidbb", "aanamebb", "aanick_name_bb");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertFieldNameToSnakeCase(SinkRecordFactory factory) {
        try (FieldNameTransformation<SinkRecord> transform = new FieldNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("column.naming.style", NamingStyle.SNAKE_CASE.getValue());
            transform.configure(properties);

            var record = new KafkaDebeziumSinkRecord(transform.apply(createSinkRecord(factory, "docId", "docId", "documentName", "nick_name_")),
                    new JdbcSinkConnectorConfig(properties).cloudEventsSchemaNamePattern());
            assertSchemaFieldNames(record.keySchema()).containsOnly("doc_id");
            assertSchemaFieldNames(record.getPayload().schema()).containsOnly("doc_id", "document_name", "nick_name_");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertFieldNameToSnakeCaseWithPrefixSuffix(SinkRecordFactory factory) {
        try (FieldNameTransformation<SinkRecord> transform = new FieldNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("column.naming.style", NamingStyle.SNAKE_CASE.getValue());
            properties.put("column.naming.prefix", "aa");
            properties.put("column.naming.suffix", "bb");
            transform.configure(properties);

            var record = new KafkaDebeziumSinkRecord(transform.apply(createSinkRecord(factory, "docId", "docId", "documentName", "nick_name_")),
                    new JdbcSinkConnectorConfig(properties).cloudEventsSchemaNamePattern());
            assertSchemaFieldNames(record.keySchema()).containsOnly("aadoc_idbb");
            assertSchemaFieldNames(record.getPayload().schema()).containsOnly("aadoc_idbb", "aadocument_namebb", "aanick_name_bb");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertFieldNameToCamelCase(SinkRecordFactory factory) {
        try (FieldNameTransformation<SinkRecord> transform = new FieldNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("column.naming.style", NamingStyle.CAMEL_CASE.getValue());
            transform.configure(properties);

            var record = new KafkaDebeziumSinkRecord(transform.apply(createSinkRecord(factory, "doc_id", "doc_id", "document_name", "nick_name_")),
                    new JdbcSinkConnectorConfig(properties).cloudEventsSchemaNamePattern());
            assertSchemaFieldNames(record.keySchema()).containsOnly("docId");
            assertSchemaFieldNames(record.getPayload().schema()).containsOnly("docId", "documentName", "nickName");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertFieldNameToCamelCaseWithPrefixSuffix(SinkRecordFactory factory) {
        try (FieldNameTransformation<SinkRecord> transform = new FieldNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("column.naming.style", NamingStyle.CAMEL_CASE.getValue());
            properties.put("column.naming.prefix", "aa");
            properties.put("column.naming.suffix", "bb");
            transform.configure(properties);

            var record = new KafkaDebeziumSinkRecord(transform.apply(createSinkRecord(factory, "doc_id", "doc_id", "document_name", "nick_name_")),
                    new JdbcSinkConnectorConfig(properties).cloudEventsSchemaNamePattern());
            assertSchemaFieldNames(record.keySchema()).containsOnly("aadocIdbb");
            assertSchemaFieldNames(record.getPayload().schema()).containsOnly("aadocIdbb", "aadocumentNamebb", "aanickNamebb");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertFieldNameToUpperCase(SinkRecordFactory factory) {
        try (FieldNameTransformation<SinkRecord> transform = new FieldNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("column.naming.style", NamingStyle.UPPER_CASE.getValue());
            transform.configure(properties);

            var record = new KafkaDebeziumSinkRecord(transform.apply(createSinkRecord(factory, "doc_id", "doc_id", "document_name", "nick_name_")),
                    new JdbcSinkConnectorConfig(properties).cloudEventsSchemaNamePattern());
            assertSchemaFieldNames(record.keySchema()).containsOnly("DOC_ID");
            assertSchemaFieldNames(record.getPayload().schema()).containsOnly("DOC_ID", "DOCUMENT_NAME", "NICK_NAME_");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertFieldNameToUpperCaseWithPrefixSuffix(SinkRecordFactory factory) {
        try (FieldNameTransformation<SinkRecord> transform = new FieldNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("column.naming.style", NamingStyle.UPPER_CASE.getValue());
            properties.put("column.naming.prefix", "aa");
            properties.put("column.naming.suffix", "bb");
            transform.configure(properties);

            var record = new KafkaDebeziumSinkRecord(transform.apply(createSinkRecord(factory, "doc_id", "doc_id", "document_name", "nick_name_")),
                    new JdbcSinkConnectorConfig(properties).cloudEventsSchemaNamePattern());
            assertSchemaFieldNames(record.keySchema()).containsOnly("aaDOC_IDbb");
            assertSchemaFieldNames(record.getPayload().schema()).containsOnly("aaDOC_IDbb", "aaDOCUMENT_NAMEbb", "aaNICK_NAME_bb");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertFieldNameToLowerCase(SinkRecordFactory factory) {
        try (FieldNameTransformation<SinkRecord> transform = new FieldNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("column.naming.style", NamingStyle.LOWER_CASE.getValue());
            transform.configure(properties);

            var record = new KafkaDebeziumSinkRecord(transform.apply(createSinkRecord(factory, "Doc_Id", "Doc_Id", "Document_Name", "nick_Name_")),
                    new JdbcSinkConnectorConfig(properties).cloudEventsSchemaNamePattern());
            assertSchemaFieldNames(record.keySchema()).containsOnly("doc_id");
            assertSchemaFieldNames(record.getPayload().schema()).containsOnly("doc_id", "document_name", "nick_name_");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertFieldNameToLowerCaseWithPrefixSuffix(SinkRecordFactory factory) {
        try (FieldNameTransformation<SinkRecord> transform = new FieldNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("column.naming.style", NamingStyle.LOWER_CASE.getValue());
            properties.put("column.naming.prefix", "aa");
            properties.put("column.naming.suffix", "bb");
            transform.configure(properties);

            var record = new KafkaDebeziumSinkRecord(transform.apply(createSinkRecord(factory, "Doc_Id", "Doc_Id", "Document_Name", "nick_Name_")),
                    new JdbcSinkConnectorConfig(properties).cloudEventsSchemaNamePattern());
            assertSchemaFieldNames(record.keySchema()).containsOnly("aadoc_idbb");
            assertSchemaFieldNames(record.getPayload().schema()).containsOnly("aadoc_idbb", "aadocument_namebb", "aanick_name_bb");
        }
    }

    private static ListAssert assertSchemaFieldNames(Schema schema) {
        return assertThat(schema.fields().stream().map(Field::name).toList());
    }

    private static SinkRecord createSinkRecord(SinkRecordFactory factory, String keyFieldName, String... payloadFieldNames) {
        final Schema keySchema = SchemaBuilder.struct().field(keyFieldName, Schema.INT8_SCHEMA).build();
        final Schema sourceSchema = SchemaBuilder.struct().field("ts_ms", Schema.OPTIONAL_INT32_SCHEMA).build();

        final SinkRecordTypeBuilder builder = SinkRecordBuilder.create()
                .flat(factory.isFlattened())
                .name("prefix")
                .topic("topic")
                .offset(1)
                .partition(0);

        final SchemaBuilder recordSchemaBuilder = SchemaBuilder.struct();
        Arrays.stream(payloadFieldNames).forEach(payloadFieldName -> {
            recordSchemaBuilder.field(payloadFieldName, Schema.OPTIONAL_STRING_SCHEMA);
            builder.after(payloadFieldName, "randomValue");
        });

        return builder.keySchema(keySchema)
                .recordSchema(recordSchemaBuilder.build())
                .sourceSchema(sourceSchema)
                .key(keyFieldName, (byte) 1)
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .build()
                .getOriginalKafkaRecord();
    }
}

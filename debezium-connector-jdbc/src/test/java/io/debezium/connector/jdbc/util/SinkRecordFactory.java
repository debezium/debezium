/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.function.UnaryOperator;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.connector.jdbc.JdbcKafkaSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.converters.spi.SerializerType;
import io.debezium.data.SchemaAndValueField;

/**
 * @author Chris Cranford
 */
public interface SinkRecordFactory {

    /**
     * Returns whether the factor constructs flattened records or complex debezium payloads.
     */
    boolean isFlattened();

    /**
     * Returns a {@link SinkRecordBuilder} instance
     */
    default SinkRecordBuilder.SinkRecordTypeBuilder createBuilder(JdbcSinkConnectorConfig config) {
        return SinkRecordBuilder.create(config).flat(isFlattened());
    }

    /**
     * Returns an update {@link SinkRecordBuilder} instance
     */
    default SinkRecordBuilder.SinkRecordTypeBuilder updateBuilder(JdbcSinkConnectorConfig config) {
        return SinkRecordBuilder.update(config).flat(isFlattened());
    }

    /**
     * Returns a delete {@link SinkRecordBuilder} instance
     */
    default SinkRecordBuilder.SinkRecordTypeBuilder deleteBuilder(JdbcSinkConnectorConfig config) {
        return SinkRecordBuilder.delete(config).flat(isFlattened());
    }

    /**
     * Returns a primitive key schema.
     */
    default Schema primitiveKeySchema() {
        return SchemaBuilder.int32().build();
    }

    /**
     * Returns a single field key schema.
     */
    default Schema basicKeySchema() {
        return basicKeySchema(UnaryOperator.identity());
    }

    /**
     * Returns a single field key schema.
     * @param columnNameTransformation transformation for the field name
     */
    default Schema basicKeySchema(UnaryOperator<String> columnNameTransformation) {
        return SchemaBuilder.struct().field(columnNameTransformation.apply("id"), Schema.INT8_SCHEMA).build();
    }

    /**
     * Returns a single field key schema.
     * @param columnNameTransformation transformation for the field name
     * @param schema the schema used for the key field
     */
    default Schema keySchema(UnaryOperator<String> columnNameTransformation, Schema schema) {
        return SchemaBuilder.struct().field(columnNameTransformation.apply("id"), schema).build();
    }

    /**
     * Returns a multiple field key schema.
     */
    default Schema multipleKeySchema() {
        return SchemaBuilder.struct().field("id1", Schema.INT8_SCHEMA).field("id2", Schema.INT32_SCHEMA).build();
    }

    /**
     * Returns a simple source info block schema.
     */
    default Schema basicSourceSchema() {
        return SchemaBuilder.struct().field("ts_ms", Schema.OPTIONAL_INT32_SCHEMA)
                .field("schema", Schema.OPTIONAL_STRING_SCHEMA)
                .field("db", Schema.OPTIONAL_STRING_SCHEMA)
                .field("table", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    default Schema basicRecordSchema() {
        return basicRecordSchema(UnaryOperator.identity());
    }

    default Schema basicRecordSchema(UnaryOperator<String> columnNameTransformation) {
        return SchemaBuilder.struct()
                .field(columnNameTransformation.apply("id"), Schema.INT8_SCHEMA)
                .field(columnNameTransformation.apply("name"), Schema.OPTIONAL_STRING_SCHEMA)
                .field(columnNameTransformation.apply("nick_name_"), nickNameFieldSchema(columnNameTransformation))
                .build();
    }

    default Schema nickNameFieldSchema(UnaryOperator<String> columnNameTransformation) {
        return SchemaBuilder.string()
                .optional()
                .parameter("__debezium.source.column.name", columnNameTransformation.apply("nick_name$"))
                .parameter("__debezium.source.column.type", "varchar")
                .parameter("__debezium.source.column.length", "255")
                .build();
    }

    default Schema multipleKeyRecordSchema() {
        return SchemaBuilder.struct()
                .field("id1", Schema.INT8_SCHEMA)
                .field("id2", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    default Schema allKafkaSchemaTypesSchema() {
        return SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("col_int8", Schema.INT8_SCHEMA)
                .field("col_int8_optional", Schema.OPTIONAL_INT8_SCHEMA)
                .field("col_int16", Schema.INT16_SCHEMA)
                .field("col_int16_optional", Schema.OPTIONAL_INT16_SCHEMA)
                .field("col_int32", Schema.INT32_SCHEMA)
                .field("col_int32_optional", Schema.OPTIONAL_INT32_SCHEMA)
                .field("col_int64", Schema.INT64_SCHEMA)
                .field("col_int64_optional", Schema.OPTIONAL_INT64_SCHEMA)
                .field("col_float32", Schema.FLOAT32_SCHEMA)
                .field("col_float32_optional", Schema.OPTIONAL_FLOAT32_SCHEMA)
                .field("col_float64", Schema.FLOAT64_SCHEMA)
                .field("col_float64_optional", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("col_bool", Schema.BOOLEAN_SCHEMA)
                .field("col_bool_optional", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field("col_string", Schema.STRING_SCHEMA)
                .field("col_string_optional", Schema.OPTIONAL_STRING_SCHEMA)
                .field("col_bytes", Schema.BYTES_SCHEMA)
                .field("col_bytes_optional", Schema.OPTIONAL_BYTES_SCHEMA)
                .build();
    }

    default Schema allKafkaSchemaTypesSchemaWithDefaults() {
        return SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("col_int8", SchemaBuilder.int8().defaultValue((byte) 2).build())
                .field("col_int8_optional", SchemaBuilder.int16().optional().defaultValue((byte) 2).build())
                .field("col_int16", Schema.INT16_SCHEMA)
                .field("col_int16_optional", Schema.OPTIONAL_INT16_SCHEMA)
                .field("col_int32", Schema.INT32_SCHEMA)
                .field("col_int32_optional", Schema.OPTIONAL_INT32_SCHEMA)
                .field("col_int64", Schema.INT64_SCHEMA)
                .field("col_int64_optional", Schema.OPTIONAL_INT64_SCHEMA)
                .field("col_float32", Schema.FLOAT32_SCHEMA)
                .field("col_float32_optional", Schema.OPTIONAL_FLOAT32_SCHEMA)
                .field("col_float64", Schema.FLOAT64_SCHEMA)
                .field("col_float64_optional", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("col_bool", Schema.BOOLEAN_SCHEMA)
                .field("col_bool_optional", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field("col_string", Schema.STRING_SCHEMA)
                .field("col_string_optional", Schema.OPTIONAL_STRING_SCHEMA)
                .field("col_bytes", Schema.BYTES_SCHEMA)
                .field("col_bytes_optional", Schema.OPTIONAL_BYTES_SCHEMA)
                .build();
    }

    default Schema allKafkaSchemaTypesSchemaWithOptionalDefaultValues() {
        final String text = "Hello World";
        return SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("col_int8", Schema.INT8_SCHEMA)
                .field("col_int8_optional", SchemaBuilder.int8().optional().defaultValue((byte) 10).build())
                .field("col_int16", Schema.INT16_SCHEMA)
                .field("col_int16_optional", SchemaBuilder.int16().optional().defaultValue((short) 15).build())
                .field("col_int32", Schema.INT32_SCHEMA)
                .field("col_int32_optional", SchemaBuilder.int32().optional().defaultValue(1024).build())
                .field("col_int64", Schema.INT64_SCHEMA)
                .field("col_int64_optional", SchemaBuilder.int64().optional().defaultValue(1024L).build())
                .field("col_float32", Schema.FLOAT32_SCHEMA)
                .field("col_float32_optional", SchemaBuilder.float32().optional().defaultValue(3.14f).build())
                .field("col_float64", Schema.FLOAT64_SCHEMA)
                .field("col_float64_optional", SchemaBuilder.float64().optional().defaultValue(3.14d).build())
                .field("col_bool", Schema.BOOLEAN_SCHEMA)
                .field("col_bool_optional", SchemaBuilder.bool().optional().defaultValue(true).build())
                .field("col_string", Schema.STRING_SCHEMA)
                .field("col_string_optional", SchemaBuilder.string().optional().defaultValue(text).build())
                .field("col_bytes", Schema.BYTES_SCHEMA)
                .field("col_bytes_optional", SchemaBuilder.bytes().optional().defaultValue(text.getBytes(StandardCharsets.UTF_8)).build())
                .build();
    }

    default JdbcKafkaSinkRecord createRecordNoKey(String topicName, JdbcSinkConnectorConfig config) {
        return SinkRecordBuilder.create(config)
                .flat(isFlattened())
                .name("prefix")
                .topic(topicName)
                .recordSchema(basicRecordSchema())
                .sourceSchema(basicSourceSchema())
                .after("id", (byte) 1)
                .after("name", "John Doe")
                .after("nick_name_", "John Doe$")
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .build();
    }

    default JdbcKafkaSinkRecord createRecord(String topicName, JdbcSinkConnectorConfig config) {
        return createRecord(topicName, (byte) 1, config);
    }

    default JdbcKafkaSinkRecord createRecord(String topicName, byte key, JdbcSinkConnectorConfig config) {
        return createRecord(topicName, key, UnaryOperator.identity(), config);
    }

    default JdbcKafkaSinkRecord createRecord(String topicName, byte key, UnaryOperator<String> columnNameTransformation, JdbcSinkConnectorConfig config) {
        return SinkRecordBuilder.create(config)
                .flat(isFlattened())
                .name("prefix")
                .topic(topicName)
                .offset(1)
                .partition(0)
                .keySchema(basicKeySchema(columnNameTransformation))
                .recordSchema(basicRecordSchema(columnNameTransformation))
                .sourceSchema(basicSourceSchema())
                .key(columnNameTransformation.apply("id"), key)
                .after(columnNameTransformation.apply("id"), key)
                .after(columnNameTransformation.apply("name"), "John Doe")
                .after(columnNameTransformation.apply("nick_name_"), "John Doe$")
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .build();
    }

    default JdbcKafkaSinkRecord createRecordWithSchemaValue(String topicName, byte key, List<String> fieldNames, List<Schema> fieldSchemas, List<Object> values,
                                                            JdbcSinkConnectorConfig config) {
        SinkRecordBuilder.SinkRecordTypeBuilder basicSchemaBuilder = SinkRecordBuilder.create(config)
                .flat(isFlattened())
                .name("prefix")
                .topic(topicName)
                .offset(1)
                .partition(0)
                .keySchema(basicKeySchema())
                .sourceSchema(basicSourceSchema())
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .key("id", key);

        SchemaBuilder recordSchemaBuilder = SchemaBuilder.struct().field("id", Schema.INT8_SCHEMA);

        for (int i = 0; i < fieldNames.size(); i++) {
            recordSchemaBuilder.field(fieldNames.get(i), fieldSchemas.get(i));
        }

        Schema recordSchema = recordSchemaBuilder.build();

        basicSchemaBuilder.recordSchema(recordSchema);

        basicSchemaBuilder.after("id", key);

        for (int i = 0; i < values.size(); i++) {
            basicSchemaBuilder.after(fieldNames.get(i), values.get(i));
        }

        return basicSchemaBuilder.build();
    }

    default JdbcKafkaSinkRecord createInsertSchemaAndValue(String topicName, List<SchemaAndValueField> keyFields, List<SchemaAndValueField> valueFields, int offset,
                                                           JdbcSinkConnectorConfig config) {
        Schema keySchema = null;
        if (!keyFields.isEmpty()) {
            SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
            for (SchemaAndValueField keyField : keyFields) {
                keySchemaBuilder.field(keyField.fieldName(), keyField.schema());
            }
            keySchema = keySchemaBuilder.build();
        }

        final SchemaBuilder recordSchemaBuilder = SchemaBuilder.struct();
        keyFields.forEach(keyField -> recordSchemaBuilder.field(keyField.fieldName(), keyField.schema()));
        valueFields.forEach(valueField -> recordSchemaBuilder.field(valueField.fieldName(), valueField.schema()));
        final Schema recordSchema = recordSchemaBuilder.build();

        SinkRecordBuilder.SinkRecordTypeBuilder builder = SinkRecordBuilder.create(config)
                .flat(isFlattened())
                .name("prefix")
                .topic(topicName)
                .offset(offset)
                .partition(0)
                .keySchema(keySchema)
                .recordSchema(recordSchema)
                .sourceSchema(basicSourceSchema())
                .source("ts_ms", (int) Instant.now().getEpochSecond());

        for (SchemaAndValueField keyField : keyFields) {
            builder.key(keyField.fieldName(), keyField.value());
            builder.after(keyField.fieldName(), keyField.value());
        }

        for (SchemaAndValueField valueField : valueFields) {
            builder.after(valueField.fieldName(), valueField.value());
        }

        return builder.build();
    }

    default JdbcKafkaSinkRecord createRecordWithSchemaValue(String topicName, byte key, String fieldName, Schema fieldSchema, Object value,
                                                            JdbcSinkConnectorConfig config) {
        return SinkRecordBuilder.create(config)
                .flat(isFlattened())
                .name("prefix")
                .topic(topicName)
                .offset(1)
                .partition(0)
                .keySchema(basicKeySchema())
                .recordSchema(SchemaBuilder.struct()
                        .field("id", Schema.INT8_SCHEMA)
                        .field(fieldName, fieldSchema)
                        .build())
                .sourceSchema(basicSourceSchema())
                .key("id", key)
                .after("id", key)
                .after(fieldName, value)
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .build();
    }

    default JdbcKafkaSinkRecord createRecord(String topicName, byte key, String database, String schema, String table, JdbcSinkConnectorConfig config) {
        return SinkRecordBuilder.create(config)
                .flat(isFlattened())
                .name("prefix")
                .topic(topicName)
                .offset(1)
                .partition(0)
                .keySchema(basicKeySchema())
                .recordSchema(basicRecordSchema())
                .sourceSchema(basicSourceSchema())
                .key("id", key)
                .after("id", key)
                .after("name", "John Doe")
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .source("db", database)
                .source("schema", schema)
                .source("table", table)
                .build();
    }

    default JdbcKafkaSinkRecord createRecordMultipleKeyColumns(String topicName, JdbcSinkConnectorConfig config) {
        return SinkRecordBuilder.create(config)
                .flat(isFlattened())
                .name("prefix")
                .topic(topicName)
                .keySchema(multipleKeySchema())
                .recordSchema(multipleKeyRecordSchema())
                .sourceSchema(basicSourceSchema())
                .key("id1", (byte) 1)
                .key("id2", 10)
                .after("id1", (byte) 1)
                .after("id2", 10)
                .after("name", "John Doe")
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .build();
    }

    default JdbcKafkaSinkRecord updateRecord(String topicName, JdbcSinkConnectorConfig config) {
        return SinkRecordBuilder.update(config)
                .flat(isFlattened())
                .name("prefix")
                .topic(topicName)
                .keySchema(basicKeySchema())
                .recordSchema(basicRecordSchema())
                .sourceSchema(basicSourceSchema())
                .key("id", (byte) 1)
                .before("id", (byte) 1)
                .before("name", "John Doe")
                .after("id", (byte) 1)
                .after("name", "Jane Doe")
                .after("nick_name_", "John Doe$")
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .build();
    }

    default JdbcKafkaSinkRecord updateRecordWithSchemaValue(String topicName, byte key, String fieldName, Schema fieldSchema, Object value,
                                                            JdbcSinkConnectorConfig config) {
        return SinkRecordBuilder.update(config)
                .flat(isFlattened())
                .name("prefix")
                .topic(topicName)
                .offset(1)
                .partition(0)
                .keySchema(basicKeySchema())
                .recordSchema(SchemaBuilder.struct()
                        .field("id", Schema.INT8_SCHEMA)
                        .field(fieldName, fieldSchema)
                        .build())
                .sourceSchema(basicSourceSchema())
                .key("id", key)
                .before("id", key)
                .before(fieldName, value)
                .after("id", key)
                .after(fieldName, value)
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .build();
    }

    default JdbcKafkaSinkRecord deleteRecord(String topicName, JdbcSinkConnectorConfig config) {
        return SinkRecordBuilder.delete(config)
                .flat(isFlattened())
                .name("prefix")
                .topic(topicName)
                .keySchema(basicKeySchema())
                .recordSchema(basicRecordSchema())
                .sourceSchema(basicSourceSchema())
                .key("id", (byte) 1)
                .before("id", (byte) 1)
                .before("name", "John Doe")
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .build();
    }

    default JdbcKafkaSinkRecord deleteRecordMultipleKeyColumns(String topicName, JdbcSinkConnectorConfig config) {
        return SinkRecordBuilder.delete(config)
                .flat(isFlattened())
                .name("prefix")
                .topic(topicName)
                .keySchema(multipleKeySchema())
                .recordSchema(multipleKeyRecordSchema())
                .sourceSchema(basicSourceSchema())
                .key("id1", (byte) 1)
                .key("id2", 10)
                .before("id1", (byte) 1)
                .before("id2", 10)
                .before("name", "John Doe")
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .build();
    }

    default JdbcKafkaSinkRecord tombstoneRecord(String topicName, JdbcSinkConnectorConfig config) {
        return SinkRecordBuilder.tombstone(config)
                .topic(topicName)
                .keySchema(basicKeySchema())
                .key("id", (byte) 1)
                .build();
    }

    default JdbcKafkaSinkRecord truncateRecord(String topicName, JdbcSinkConnectorConfig config) {
        return SinkRecordBuilder.truncate(config)
                .flat(isFlattened())
                .topic(topicName)
                .offset(1)
                .partition(0)
                .recordSchema(basicRecordSchema())
                .sourceSchema(basicSourceSchema())
                .build();
    }

    default JdbcKafkaSinkRecord cloudEventRecord(String topicName, SerializerType serializerType, JdbcSinkConnectorConfig config) {
        return cloudEventRecord(topicName, serializerType, null, config);
    }

    default JdbcKafkaSinkRecord cloudEventRecord(String topicName, SerializerType serializerType, String cloudEventsSchemaName, JdbcSinkConnectorConfig config) {
        final JdbcKafkaSinkRecord baseRecord = updateRecord(topicName, config);
        return SinkRecordBuilder.cloudEvent(config)
                .baseRecord(baseRecord.getOriginalKafkaRecord())
                .cloudEventsSerializerType(serializerType)
                .cloudEventsSchemaName(cloudEventsSchemaName)
                .build();
    }
}

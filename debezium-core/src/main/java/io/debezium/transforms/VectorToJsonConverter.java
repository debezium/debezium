/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.data.Json;
import io.debezium.data.SchemaUtil;
import io.debezium.data.vector.DoubleVector;
import io.debezium.data.vector.FloatVector;

/**
 * A transformation that converts Debezium's logical vector data types to JSON, so that the vector data
 * can be consumed by systems that don't support vector types but can handle JSON structured data.
 *
 * The Debezium logical vector data types supported are:
 * <ul>
 *     <li>io.debezium.data.DoubleVector</li>
 *     <li>io.debezium.data.FloatVector</li>
 *     <io>io.debezium.data.vector.SparseVector (PostgreSQL source only)</io>
 * </ul>
 *
 * @author Chris Cranford
 */
public class VectorToJsonConverter<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final String DOUBLE_VECTOR_NAME = DoubleVector.LOGICAL_NAME;
    private static final String FLOAT_VECTOR_NAME = FloatVector.LOGICAL_NAME;
    private static final String SPARSE_VECTOR_NAME = "io.debezium.data.SparseVector";

    private static final List<String> VECTOR_LOGICAL_TYPES = List.of(DOUBLE_VECTOR_NAME, FLOAT_VECTOR_NAME, SPARSE_VECTOR_NAME);

    private SmtManager<R> smtManager;

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);
        smtManager.validate(config, io.debezium.config.Field.setOf());
    }

    @Override
    public R apply(R record) {
        if (record.value() == null || !smtManager.isValidEnvelope(record)) {
            return record;
        }

        final Struct value = requireStruct(record.value(), "Record value should be a struct.");

        final Schema valueSchema = value.schema();
        if (!isConversionRequired(valueSchema)) {
            return record;
        }

        final Struct newValue = transformValue(value); // , getUpdatedRecordSchema(valueSchema));

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                newValue.schema(),
                newValue,
                record.timestamp(),
                record.headers());
    }

    @Override
    public void close() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    private boolean isConversionRequired(Schema originalSchema) {
        final Schema recordSchema = originalSchema.field(Envelope.FieldName.AFTER).schema();
        return recordSchema.fields().stream().anyMatch(field -> isFieldToConvertToJson(field.schema()));
    }

    private Schema getUpdatedRecordSchema(Schema originalSchema) {
        final Schema originalRecordSchema = originalSchema.field(Envelope.FieldName.AFTER).schema();
        final SchemaBuilder schemaBuilder = SchemaUtil.copySchemaBasics(originalRecordSchema);
        for (Field field : originalRecordSchema.fields()) {
            final Schema fieldSchema = field.schema();
            if (isFieldToConvertToJson(fieldSchema)) {
                schemaBuilder.field(field.name(), Json.schema());
            }
            else {
                schemaBuilder.field(field.name(), field.schema());
            }
        }
        return schemaBuilder.build();
    }

    private boolean isFieldToConvertToJson(Schema schema) {
        return schema.name() != null && VECTOR_LOGICAL_TYPES.contains(schema.name());
    }

    private Struct transformValue(Struct originalValue) {
        final Schema originalSchema = originalValue.schema();
        final Schema recordSchema = getUpdatedRecordSchema(originalSchema);

        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(originalSchema);

        final Map<String, Object> valueMap = new HashMap<>();
        for (Field field : originalSchema.fields()) {
            Object fieldValue = originalValue.get(field);
            switch (field.name()) {
                case Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER -> {
                    if (fieldValue != null) {
                        final Struct fieldStruct = requireStruct(fieldValue, "Field should be a struct.");
                        fieldValue = transformRecord(fieldStruct, recordSchema);
                    }
                    builder.field(field.name(), recordSchema);
                }
                default -> builder.field(field.name(), field.schema());
            }
            valueMap.put(field.name(), fieldValue);
        }

        final Struct newValue = new Struct(builder.build());
        valueMap.forEach(newValue::put);

        return newValue;
    }

    private Struct transformRecord(Struct originalValue, Schema recordSchema) {
        Objects.requireNonNull(originalValue, "The value should not be null");
        Objects.requireNonNull(recordSchema, "The record schema should not be null");

        final Struct newRecord = new Struct(recordSchema);

        originalValue.schema().fields()
                .stream()
                .collect(Collectors.toMap(
                        Field::name,
                        field -> transformRecordFieldValue(field, originalValue.get(field))))
                .forEach(newRecord::put);

        return newRecord;
    }

    private Object transformRecordFieldValue(Field field, Object value) {
        final String schemaName = field.schema().name();
        if (SPARSE_VECTOR_NAME.equals(schemaName)) {
            return sparseVectorToJson(requireStruct(value, "SparseVector should be a struct."));
        }
        else if (FLOAT_VECTOR_NAME.equals(schemaName) || DOUBLE_VECTOR_NAME.equals(schemaName)) {
            return vectorToJson((Collection<?>) value);
        }
        return value;
    }

    private String vectorToJson(Collection<?> values) {
        return "{ \"values\": [" + values.stream().map(String::valueOf).collect(Collectors.joining(", ")) + "] }";
    }

    private String sparseVectorToJson(Struct sparseVector) {
        final Short dimensions = sparseVector.getInt16("dimensions");
        final Map<Short, Double> vectorMap = sparseVector.getMap("vector");

        final String vector = vectorMap.entrySet().stream()
                .map(e -> "\"" + e.getKey() + "\": " + e.getValue())
                .collect(Collectors.joining(", "));

        return String.format("{ \"dimensions\": %d, \"vector\": { %s } }", dimensions, vector);
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
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
import io.debezium.annotation.Immutable;
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
 *     <li>io.debezium.data.vector.SparseVector (PostgreSQL source only)</li>
 * </ul>
 *
 * @author Chris Cranford
 */
public class VectorToJsonConverter<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final String DOUBLE_VECTOR_NAME = DoubleVector.LOGICAL_NAME;
    private static final String FLOAT_VECTOR_NAME = FloatVector.LOGICAL_NAME;
    private static final String SPARSE_VECTOR_NAME = "io.debezium.data.SparseVector";

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }

        final TransformationResult result = transformStruct(requireStruct(record.value(), "Value should be a struct"), record.valueSchema());

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                result.transformedSchema,
                result.transformedStruct,
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

    private TransformationResult transformStruct(Struct originalStruct, Schema originalSchema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(originalSchema);

        final Map<String, Object> fieldValues = new HashMap<>();
        for (Field field : originalSchema.fields()) {
            Schema fieldSchema = field.schema();
            Object fieldValue = originalStruct != null ? originalStruct.get(field) : null;
            if (DOUBLE_VECTOR_NAME.equals(fieldSchema.name()) || FLOAT_VECTOR_NAME.equals(fieldSchema.name())) {
                // Convert DoubleVector and FloatVector logical types to a Json logical type
                builder.field(field.name(), getJsonSchema(fieldSchema));
                fieldValues.put(field.name(), vectorToJson((Collection<?>) fieldValue));
            }
            else if (SPARSE_VECTOR_NAME.equals(fieldSchema.name())) {
                // Convert SparseVector logical type to a Json logical type
                final Struct fieldStruct = requireStructOrNull(fieldValue, "SparseVector should be a struct.");
                builder.field(field.name(), getJsonSchema(fieldSchema));
                fieldValues.put(field.name(), sparseVectorToJson(fieldStruct));
            }
            else if (Schema.Type.STRUCT.equals(fieldSchema.type())) {
                // Transform nested Struct
                final Struct fieldStruct = requireStructOrNull(fieldValue, "Should be a struct.");
                final TransformationResult result = transformStruct(fieldStruct, fieldSchema);
                builder.field(field.name(), result.transformedSchema);
                fieldValues.put(field.name(), result.transformedStruct);
            }
            else {
                // Keep original values
                builder.field(field.name(), fieldSchema);
                fieldValues.put(field.name(), fieldValue);
            }
        }

        final Schema transformedSchema = builder.build();

        final Struct transformedStruct;
        if (originalStruct != null) {
            transformedStruct = new Struct(transformedSchema);
            fieldValues.forEach(transformedStruct::put);
        }
        else {
            transformedStruct = null;
        }

        return new TransformationResult(transformedSchema, transformedStruct);
    }

    private static Schema getJsonSchema(Schema fieldSchema) {
        if (fieldSchema.isOptional()) {
            return Json.builder().optional().build();
        }
        return Json.schema();
    }

    private static String vectorToJson(Collection<?> values) {
        if (values == null) {
            return null;
        }
        return "{ \"values\": [" + values.stream().map(String::valueOf).collect(Collectors.joining(", ")) + "] }";
    }

    private static String sparseVectorToJson(Struct sparseVector) {
        if (sparseVector == null) {
            return null;
        }

        final Short dimensions = sparseVector.getInt16("dimensions");
        final Map<Short, Double> vectorMap = new TreeMap<>(sparseVector.getMap("vector"));

        final String vector = vectorMap.entrySet().stream()
                .map(e -> "\"" + e.getKey() + "\": " + e.getValue())
                .collect(Collectors.joining(", "));

        return String.format("{ \"dimensions\": %d, \"vector\": { %s } }", dimensions, vector);
    }

    @Immutable
    private static class TransformationResult {
        final Schema transformedSchema;
        final Struct transformedStruct;

        TransformationResult(Schema transformedSchema, Struct transformedStruct) {
            this.transformedSchema = transformedSchema;
            this.transformedStruct = transformedStruct;
        }
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.NullNode;

import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.FieldNameSelector.FieldNamer;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.transforms.outbox.EventRouterConfigDefinition.JsonPayloadNullFieldBehavior;

public class JsonSchemaData {
    private final JsonPayloadNullFieldBehavior jsonPayloadNullFieldBehavior;
    private final FieldNamer<String> fieldNamer;

    public JsonSchemaData() {
        this.jsonPayloadNullFieldBehavior = JsonPayloadNullFieldBehavior.IGNORE;
        this.fieldNamer = FieldNameSelector.defaultNonRelationalSelector(SchemaNameAdjuster.NO_OP);
    }

    public JsonSchemaData(JsonPayloadNullFieldBehavior jsonPayloadNullFieldBehavior, FieldNamer<String> fieldNamer) {
        this.jsonPayloadNullFieldBehavior = jsonPayloadNullFieldBehavior;
        this.fieldNamer = fieldNamer;
    }

    /**
     * Build a new connect Schema inferring structure and types from Json node.
     */
    public Schema toConnectSchema(String key, JsonNode node) {
        switch (node.getNodeType()) {
            case STRING:
                return Schema.OPTIONAL_STRING_SCHEMA;
            case BOOLEAN:
                return Schema.OPTIONAL_BOOLEAN_SCHEMA;
            case NUMBER:
                if (node.isInt()) {
                    return Schema.OPTIONAL_INT32_SCHEMA;
                }
                if (node.isLong()) {
                    return Schema.OPTIONAL_INT64_SCHEMA;
                }
                return Schema.OPTIONAL_FLOAT64_SCHEMA;
            case ARRAY:
                ArrayNode arrayNode = (ArrayNode) node;
                return arrayNode.isEmpty() ? null : SchemaBuilder.array(toConnectSchemaWithCycles(key, arrayNode)).optional().build();
            case OBJECT:
                final SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(key).optional();
                node.fields().forEachRemaining(entry -> {
                    final String fieldName = fieldNamer.fieldNameFor(entry.getKey());
                    final Schema fieldSchema = toConnectSchema(key + "." + fieldName, entry.getValue());
                    if (fieldSchema != null && !hasField(schemaBuilder, fieldName)) {
                        schemaBuilder.field(fieldName, fieldSchema);
                    }
                });
                return schemaBuilder.build();
            case NULL:
                if (jsonPayloadNullFieldBehavior.equals(JsonPayloadNullFieldBehavior.OPTIONAL_BYTES)) {
                    return Schema.OPTIONAL_BYTES_SCHEMA;
                }
                return null;
            default:
                return null;
        }
    }

    private Schema toConnectSchemaWithCycles(String key, ArrayNode array) throws ConnectException {
        Schema schema = null;
        final JsonNode sample = getFirstArrayElement(array);
        if (sample.isObject()) {
            final Set<Schema> elementSchemas = new HashSet<>();
            final Iterator<JsonNode> elements = array.elements();
            while (elements.hasNext()) {
                final JsonNode element = elements.next();
                if (!element.isObject()) {
                    continue;
                }
                elementSchemas.add(toConnectSchema(key, element));
            }
            for (Schema element : elementSchemas) {
                // TODO: need to support nested json object
                schema = mergeSchema(schema, element);
            }
        }
        else {
            schema = toConnectSchema(null, sample);
            if (schema == null) {
                throw new ConnectException(String.format("Array '%s' has unrecognized member schema.", array.asText()));
            }
        }

        return schema;
    }

    private Schema mergeSchema(Schema left, Schema right) {
        if (left == null) {
            return right;
        }

        Map<String, Field> fields = new LinkedHashMap<>();
        left.fields().forEach(field -> fields.put(field.name(), field));
        right.fields().forEach(field -> {
            Field oldField = fields.get(field.name());
            if (oldField == null) {
                fields.put(field.name(), field);
            }
            else {
                if (!Objects.equals(oldField.schema(), field.schema())
                        && oldField.schema().type() == Schema.Type.BYTES
                        && field.schema().type() != Schema.Type.BYTES) {
                    fields.put(field.name(), field);
                }
            }
        });

        SchemaBuilder newBuilder = SchemaUtil.copySchemaBasics(left);
        fields.forEach((k, v) -> newBuilder.field(k, v.schema()));
        return newBuilder.build();
    }

    private JsonNode getFirstArrayElement(ArrayNode array) throws ConnectException {
        JsonNode refNode = NullNode.getInstance();
        Schema refSchema = null;
        // Get first non-null element type and check other member types.
        Iterator<JsonNode> elements = array.elements();
        while (elements.hasNext()) {
            JsonNode element = elements.next();

            // Skip null elements.
            if (element.isNull()) {
                continue;
            }

            // Set first non-null element if not set yet.
            if (refNode.isNull()) {
                refNode = element;
            }

            // Check types of elements.
            if (element.getNodeType() != refNode.getNodeType()) {
                throw new ConnectException(String.format("Field is not a homogenous array (%s x %s).",
                        refNode.asText(), element.getNodeType().toString()));
            }

            // We may return different schemas for NUMBER type, check here they are same.
            if (refNode.getNodeType() == JsonNodeType.NUMBER) {
                if (refSchema == null) {
                    refSchema = toConnectSchema(null, refNode);
                }
                Schema elementSchema = toConnectSchema(null, element);
                if (refSchema != elementSchema) {
                    throw new ConnectException(String.format("Field is not a homogenous array (%s x %s), different number types (%s x %s)",
                            refNode.asText(), element.asText(), refSchema, elementSchema));
                }
            }
        }

        return refNode;
    }

    private boolean hasField(SchemaBuilder builder, String fieldName) {
        return builder.field(fieldName) != null;
    }

    /**
     * Convert a Jackson JsonNode into a new Struct according the schema.
     * @param document The JSON document to convert
     * @param schema The Schema for this document
     * @return A new connect Struct for the JSON node.
     */
    public Object toConnectData(JsonNode document, Schema schema) {
        if (document == null) {
            return null;
        }
        return jsonNodeToStructInternal(document, schema);
    }

    private Struct jsonNodeToStructInternal(JsonNode document, Schema schema) {
        final Struct struct = new Struct(schema);
        for (Field field : schema.fields()) {
            if (document.has(field.name())) {
                struct.put(field.name(),
                        getStructFieldValue(document.path(field.name()), field.schema()));
            }
        }
        return struct;
    }

    private Object getStructFieldValue(JsonNode node, Schema schema) {
        switch (node.getNodeType()) {
            case STRING:
                return node.asText();
            case BOOLEAN:
                return node.asBoolean();
            case NUMBER:
                if (node.isFloat()) {
                    return node.floatValue();
                }
                if (node.isDouble()) {
                    return node.asDouble();
                }
                if (node.isInt()) {
                    return node.asInt();
                }
                if (node.isLong()) {
                    return node.asLong();
                }
                return node.decimalValue();
            case ARRAY:
                return getArrayAsList((ArrayNode) node, schema);
            case OBJECT:
                return jsonNodeToStructInternal(node, schema);
            default:
                return null;
        }
    }

    private List<Object> getArrayAsList(ArrayNode array, Schema schema) {
        List<Object> arrayObjects = new ArrayList<>(array.size());
        Iterator<JsonNode> elements = array.elements();
        while (elements.hasNext()) {
            arrayObjects.add(getStructFieldValue(elements.next(), schema.valueSchema()));
        }
        return arrayObjects;
    }
}

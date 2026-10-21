/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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

import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.FieldNameSelector.FieldNamer;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.transforms.outbox.EventRouterConfigDefinition.JsonPayloadEmptyArrayBehavior;
import io.debezium.transforms.outbox.EventRouterConfigDefinition.JsonPayloadNullFieldBehavior;
import io.debezium.util.Strings;

public class JsonSchemaData {
    private static final Schema EMPTY_ARRAY_SCHEMA = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
    private final JsonPayloadNullFieldBehavior jsonPayloadNullFieldBehavior;
    private final JsonPayloadEmptyArrayBehavior jsonPayloadEmptyArrayBehavior;
    private final FieldNamer<String> fieldNamer;

    public JsonSchemaData() {
        this(JsonPayloadNullFieldBehavior.IGNORE, JsonPayloadEmptyArrayBehavior.IGNORE,
                FieldNameSelector.defaultNonRelationalSelector(SchemaNameAdjuster.NO_OP));
    }

    public JsonSchemaData(JsonPayloadNullFieldBehavior jsonPayloadNullFieldBehavior, FieldNamer<String> fieldNamer) {
        this(jsonPayloadNullFieldBehavior, JsonPayloadEmptyArrayBehavior.IGNORE, fieldNamer);
    }

    public JsonSchemaData(JsonPayloadNullFieldBehavior jsonPayloadNullFieldBehavior,
                          JsonPayloadEmptyArrayBehavior jsonPayloadEmptyArrayBehavior, FieldNamer<String> fieldNamer) {
        this.jsonPayloadNullFieldBehavior = jsonPayloadNullFieldBehavior;
        this.jsonPayloadEmptyArrayBehavior = jsonPayloadEmptyArrayBehavior;
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
                if (arrayNode.isEmpty()) {
                    if (jsonPayloadEmptyArrayBehavior.equals(JsonPayloadEmptyArrayBehavior.OPTIONAL_STRING)) {
                        return EMPTY_ARRAY_SCHEMA;
                    }
                    return null;
                }
                return SchemaBuilder.array(toConnectSchemaWithCycles(key, arrayNode)).optional().build();
            case OBJECT:
                final SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(key).optional();
                node.fields().forEachRemaining(entry -> {
                    final String fieldName = fieldNamer.fieldNameFor(entry.getKey());
                    final String fieldKey = (Strings.isNullOrBlank(key) ? "" : key + ".") + fieldName;
                    final Schema fieldSchema = toConnectSchema(fieldKey, entry.getValue());
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
        final Set<Schema> elementSchemas = new LinkedHashSet<>();
        final Iterator<JsonNode> elements = array.elements();
        while (elements.hasNext()) {
            final JsonNode element = elements.next();
            // only object elements are named after the key, as before, so struct names inside lists of lists do not change
            elementSchemas.add(toConnectSchema(element.isObject() ? key : null, element));
        }
        for (Schema element : elementSchemas) {
            schema = mergeSchema(schema, element);
        }
        if (schema == null) {
            throw new ConnectException(String.format("Array '%s' has unrecognized member schema.", array.asText()));
        }
        return schema;
    }

    private Schema mergeSchema(Schema left, Schema right) {
        if (left == null) {
            return right;
        }
        if (right == null || Objects.equals(left, right)) {
            return left;
        }
        if (left.type() == Schema.Type.BYTES) {
            return right;
        }
        if (right.type() == Schema.Type.BYTES) {
            return left;
        }

        if (left == EMPTY_ARRAY_SCHEMA && right.type() == Schema.Type.ARRAY) {
            return right;
        }

        if (right == EMPTY_ARRAY_SCHEMA && left.type() == Schema.Type.ARRAY) {
            return left;
        }

        if (left.type() == Schema.Type.ARRAY && right.type() == Schema.Type.ARRAY) {
            return SchemaBuilder.array(mergeSchema(left.valueSchema(), right.valueSchema())).optional().build();
        }
        if (left.type() != Schema.Type.STRUCT || right.type() != Schema.Type.STRUCT) {
            throw new ConnectException(String.format("Array elements have conflicting types (%s x %s).", left.type(), right.type()));
        }

        Map<String, Field> fields = new LinkedHashMap<>();
        left.fields().forEach(field -> fields.put(field.name(), field));
        right.fields().forEach(field -> {
            Field oldField = fields.get(field.name());
            if (oldField == null) {
                fields.put(field.name(), field);
            }
            else {
                Schema oldSchema = oldField.schema();
                Schema newSchema = field.schema();
                if (oldSchema.type() != newSchema.type() && oldSchema.type() != Schema.Type.BYTES && newSchema.type() != Schema.Type.BYTES) {
                    throw new ConnectException(String.format("Field '%s' has conflicting types in array elements (%s x %s).",
                            field.name(), oldSchema.type(), newSchema.type()));
                }
                fields.put(field.name(), new Field(field.name(), oldField.index(), mergeSchema(oldSchema, newSchema)));
            }
        });

        SchemaBuilder newBuilder = SchemaUtil.copySchemaBasics(left).optional();
        fields.forEach((k, v) -> newBuilder.field(k, v.schema()));
        return newBuilder.build();
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

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.mongodb.transforms.MongoToRelationalMapper.JsonOutputMode;
import io.debezium.data.Json;
import io.debezium.schema.SchemaNameAdjuster;

/**
 * A fixed projection for one MongoDB collection. Unselected values never reach the type converter.
 */
final class MongoDocumentMapping {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final Set<String> OPTIONS = Set.of("path", "type", "scale", "precision", "length");

    private record MappingField(String name, MongoDocumentPath path, Schema schema) {
    }

    private final List<MappingField> fields;
    private final Schema schema;
    private final JsonOutputMode jsonOutputMode;
    private final Set<String> jsonPaths;

    MongoDocumentMapping(String namespace, String json) {
        this(namespace, json, JsonOutputMode.INPUT);
    }

    MongoDocumentMapping(String namespace, String json, JsonOutputMode jsonOutputMode) {
        this.jsonOutputMode = jsonOutputMode;
        final JsonNode mapping;
        try {
            mapping = MAPPER.readTree(json);
        }
        catch (JsonProcessingException e) {
            final var location = e.getLocation();
            final var position = location == null ? "" : " at line " + location.getLineNr() + ", column " + location.getColumnNr();
            throw new ConfigException("Schema mapping for " + namespace + " must be valid JSON without duplicate fields or trailing content: "
                    + e.getOriginalMessage() + position);
        }
        if (mapping == null || !mapping.isObject() || mapping.isEmpty()) {
            throw new ConfigException("Schema mapping for " + namespace + " must be a non-empty JSON object");
        }
        List<MappingField> parsed = new ArrayList<>();
        final var builder = SchemaBuilder.struct().optional().name(SchemaNameAdjuster.create().adjust(namespace + ".Value"));
        mapping.fields().forEachRemaining(entry -> {
            final String name = entry.getKey();
            if (!name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                throw new ConfigException("Mapping output field names must match [A-Za-z_][A-Za-z0-9_]*: " + name);
            }
            final var definition = entry.getValue();
            if (!definition.isObject()) {
                throw new ConfigException("Mapping field " + name + " must be an object with path and type");
            }
            definition.fieldNames().forEachRemaining(option -> {
                if (!OPTIONS.contains(option)) {
                    throw new ConfigException("Unknown option for mapping field " + name + ": " + option);
                }
            });
            final String path = requiredText(definition, "path", name);
            final String type = requiredText(definition, "type", name);
            final var fieldSchema = MongoMappingType.schema(type, integer(definition, "scale"), integer(definition, "precision"), integer(definition, "length"));
            parsed.add(new MappingField(name, new MongoDocumentPath(path), fieldSchema));
            builder.field(name, fieldSchema);
        });
        fields = List.copyOf(parsed);
        schema = builder.build();
        jsonPaths = fields.stream()
                .filter(field -> Json.LOGICAL_NAME.equals(field.schema().name()) && !field.path().isRoot())
                .map(field -> field.path().pointer())
                .collect(Collectors.toUnmodifiableSet());
    }

    Schema schema() {
        return schema;
    }

    Struct convert(BsonDocument document, String originalJson) {
        if (document == null) {
            return null;
        }
        final var result = new Struct(schema);
        Map<String, String> inputJson = null;
        for (MappingField field : fields) {
            try {
                final var selected = field.path().read(document);
                final Object value;
                if (selected == null || selected.isNull()) {
                    value = null;
                }
                else if (jsonOutputMode == JsonOutputMode.INPUT && Json.LOGICAL_NAME.equals(field.schema().name())) {
                    if (field.path().isRoot()) {
                        value = originalJson;
                    }
                    else {
                        if (inputJson == null) {
                            inputJson = MongoDocumentJson.extract(originalJson, jsonPaths);
                        }
                        value = inputJson.get(field.path().pointer());
                        if (value == null) {
                            throw new DataException("No incoming JSON representation for document path '" + field.path().pointer() + "'");
                        }
                    }
                }
                else {
                    value = MongoMappingType.convert(selected, field.schema());
                }
                result.put(field.name(), value);
            }
            catch (RuntimeException e) {
                throw new DataException("Cannot convert selected MongoDB field '" + field.name() + "' to "
                        + (field.schema().name() == null ? field.schema().type() : field.schema().name()), e);
            }
        }
        return result;
    }

    private static String requiredText(JsonNode definition, String option, String field) {
        final var value = definition.get(option);
        if (value == null || !value.isTextual()) {
            throw new ConfigException("Mapping field " + field + " requires a string " + option);
        }
        return value.textValue();
    }

    private static Integer integer(JsonNode definition, String option) {
        final var value = definition.get(option);
        if (value == null) {
            return null;
        }
        if (!value.isIntegralNumber() || !value.canConvertToInt()) {
            throw new ConfigException("Mapping option " + option + " must be a 32-bit integer");
        }
        return value.intValue();
    }
}

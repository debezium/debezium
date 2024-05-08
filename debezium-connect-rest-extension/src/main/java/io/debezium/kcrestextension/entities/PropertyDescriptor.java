/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kcrestextension.entities;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * JSON model that describes a property of a Single Message Transform (SMT).
 */
public class PropertyDescriptor {

    @JsonProperty
    public String title;

    @JsonProperty("x-name")
    public String name;

    @JsonProperty
    public String description;

    @JsonProperty
    public String type;

    @JsonProperty("enum")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<String> allowedValues;

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String format;

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String defaultValue;

    public PropertyDescriptor(String className, ConfigDef.ConfigKey configKey) {
        Map<String, List<String>> specialEnums = new HashMap<>();
        specialEnums.put(
                "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState#array.encoding",
                Arrays.asList("array", "document"));
        specialEnums.put(
                "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState#delete.handling.mode",
                Arrays.asList("drop", "rewrite", "none"));
        specialEnums.put(
                "io.debezium.transforms.ExtractNewRecordState#delete.handling.mode",
                Arrays.asList("drop", "rewrite", "none"));
        specialEnums.put(
                "io.debezium.transforms.outbox.EventRouter#debezium.op.invalid.behavior",
                Arrays.asList("warn", "error", "fatal"));
        specialEnums.put(
                "io.debezium.transforms.ScriptingTransformation#null.handling.mode",
                Arrays.asList("keep", "drop", "evaluate"));

        this.title = configKey.displayName;
        this.name = configKey.name;
        this.description = configKey.documentation;
        if (!Objects.equals(ConfigDef.NO_DEFAULT_VALUE, configKey.defaultValue)
                && null != configKey.defaultValue) {
            this.defaultValue = String.valueOf(configKey.defaultValue);
        }
        JsonType jsonType = toJsonType(configKey.type());
        this.type = jsonType.schemaType;
        this.format = jsonType.format;

        String classPropertyName = className + "#" + configKey.name;
        if (specialEnums.containsKey(classPropertyName)) {
            this.allowedValues = specialEnums.get(classPropertyName);
        }
    }

    public static class JsonType {
        @JsonProperty
        public final String schemaType;

        @JsonProperty
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public final String format;

        public JsonType(String schemaType, String format) {
            this.schemaType = schemaType;
            this.format = format;
        }

        public JsonType(String schemaType) {
            this.schemaType = schemaType;
            this.format = null;
        }
    }

    private static JsonType toJsonType(ConfigDef.Type type) {
        switch (type) {
            case BOOLEAN:
                return new JsonType("BOOLEAN");
            case CLASS:
                return new JsonType("STRING", "class");
            case DOUBLE:
                return new JsonType("NUMBER", "double");
            case INT:
            case SHORT:
                return new JsonType("INTEGER", "int32");
            case LIST:
                return new JsonType("STRING", "list,regex");
            case LONG:
                return new JsonType("INTEGER", "int64");
            case PASSWORD:
                return new JsonType("STRING", "password");
            case STRING:
                return new JsonType("STRING");
            default:
                throw new IllegalArgumentException("Unsupported property type: " + type);
        }
    }

}

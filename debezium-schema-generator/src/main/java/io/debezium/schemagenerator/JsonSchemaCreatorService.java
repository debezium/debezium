/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.kafka.common.config.ConfigDef;
import org.eclipse.microprofile.openapi.models.media.Schema;

import io.debezium.config.Field;
import io.debezium.metadata.ConnectorMetadata;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.schemagenerator.schema.Schema.FieldFilter;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.smallrye.openapi.api.models.media.SchemaImpl;

public class JsonSchemaCreatorService {

    private final String connectorBaseName;
    private final String connectorName;
    private final ConnectorMetadata connectorMetadata;
    private final FieldFilter fieldFilter;
    private final List<String> errors = new ArrayList<>();

    public JsonSchemaCreatorService(ConnectorMetadata connectorMetadata, FieldFilter fieldFilter) {
        this.connectorBaseName = connectorMetadata.getConnectorDescriptor().getId();
        this.connectorName = connectorBaseName + "-" + connectorMetadata.getConnectorDescriptor().getVersion();
        this.connectorMetadata = connectorMetadata;
        this.fieldFilter = fieldFilter;
    }

    public static class JsonSchemaType {
        public final Schema.SchemaType schemaType;
        public final String format;

        public JsonSchemaType(Schema.SchemaType schemaType, String format) {
            this.schemaType = schemaType;
            this.format = format;
        }

        public JsonSchemaType(Schema.SchemaType schemaType) {
            this.schemaType = schemaType;
            this.format = null;
        }
    }

    public List<String> getErrors() {
        return errors;
    }

    private Field checkField(Field field) {
        String propertyName = field.name();

        if (propertyName.contains("whitelist")
                || propertyName.contains("blacklist")
                || propertyName.startsWith("internal.")) {
            // skip legacy and internal properties
            return null;
        }

        if (!fieldFilter.include(field)) {
            // when a property includeList is specified, skip properties not in the list
            this.errors.add("[INFO] Skipped property \"" + propertyName
                    + "\" for connector \"" + connectorName + "\" because it was not in the include list file.");
            return null;
        }

        if (null == field.group()) {
            this.errors.add("[WARN] Missing GroupEntry for property \"" + propertyName
                    + "\" for connector \"" + connectorName + "\".");
            return field.withGroup(Field.createGroupEntry(Field.Group.ADVANCED));
        }

        return field;
    }

    private static JsonSchemaType toJsonSchemaType(ConfigDef.Type type) {
        switch (type) {
            case BOOLEAN:
                return new JsonSchemaType(Schema.SchemaType.BOOLEAN);
            case CLASS:
                return new JsonSchemaType(Schema.SchemaType.STRING, "class");
            case DOUBLE:
                return new JsonSchemaType(Schema.SchemaType.NUMBER, "double");
            case INT:
            case SHORT:
                return new JsonSchemaType(Schema.SchemaType.INTEGER, "int32");
            case LIST:
                return new JsonSchemaType(Schema.SchemaType.STRING, "list,regex");
            case LONG:
                return new JsonSchemaType(Schema.SchemaType.INTEGER, "int64");
            case PASSWORD:
                return new JsonSchemaType(Schema.SchemaType.STRING, "password");
            case STRING:
                return new JsonSchemaType(Schema.SchemaType.STRING);
            default:
                throw new IllegalArgumentException("Unsupported property type: " + type);
        }
    }

    public Schema buildConnectorSchema() {
        Schema schema = new SchemaImpl(connectorName);
        String connectorVersion = connectorMetadata.getConnectorDescriptor().getVersion();
        schema.setTitle(connectorMetadata.getConnectorDescriptor().getName());
        schema.setType(Schema.SchemaType.OBJECT);
        schema.addExtension("connector-id", connectorBaseName);
        schema.addExtension("version", connectorVersion);
        schema.addExtension("className", connectorMetadata.getConnectorDescriptor().getClassName());

        Map<Field.Group, SortedMap<Integer, SchemaImpl>> orderedPropertiesByCategory = new HashMap<>();

        Arrays.stream(Field.Group.values()).forEach(category -> {
            orderedPropertiesByCategory.put(category, new TreeMap<>());
        });

        connectorMetadata.getConnectorFields().forEach(field -> processField(schema, orderedPropertiesByCategory, field));

        Arrays.stream(Field.Group.values()).forEach(
                group -> orderedPropertiesByCategory.get(group).forEach((position, propertySchema) -> schema.addProperty(propertySchema.getName(), propertySchema)));

        // Allow additional properties until OAS 3.1 is not avaialble with Swagger/microprofile-openapi
        // We need JSON Schema `patternProperties`, defined here: https://json-schema.org/understanding-json-schema/reference/object.html#pattern-properties
        // previously added to OAS 3.1: https://github.com/OAI/OpenAPI-Specification/pull/2489
        // see https://github.com/eclipse/microprofile-open-api/issues/333
        // see https://github.com/swagger-api/swagger-core/issues/3913
        schema.additionalPropertiesBoolean(true);

        return schema;
    }

    private void processField(Schema schema, Map<Field.Group, SortedMap<Integer, SchemaImpl>> orderedPropertiesByCategory, Field field) {
        String propertyName = field.name();
        Field checkedField = checkField(field);
        if (null != checkedField) {
            SchemaImpl propertySchema = new SchemaImpl(propertyName);
            Set<?> allowedValues = checkedField.allowedValues();
            if (null != allowedValues && !allowedValues.isEmpty()) {
                propertySchema.enumeration(new ArrayList<>(allowedValues));
            }
            if (checkedField.isRequired()) {
                propertySchema.nullable(false);
                schema.addRequired(propertyName);
            }
            propertySchema.description(checkedField.description());
            propertySchema.defaultValue(checkedField.defaultValue());
            JsonSchemaType jsonSchemaType = toJsonSchemaType(checkedField.type());
            propertySchema.type(jsonSchemaType.schemaType);
            if (null != jsonSchemaType.format) {
                propertySchema.format(jsonSchemaType.format);
            }
            propertySchema.title(checkedField.displayName());
            Map<String, Object> extensions = new HashMap<>();
            extensions.put("name", checkedField.name()); // @TODO remove "x-name" in favor of map key?
            Field.GroupEntry groupEntry = checkedField.group();
            extensions.put("category", groupEntry.getGroup().name());
            propertySchema.extensions(extensions);
            SortedMap<Integer, SchemaImpl> groupProperties = orderedPropertiesByCategory.get(groupEntry.getGroup());
            if (groupProperties.containsKey(groupEntry.getPositionInGroup())) {
                errors.add("[ERROR] Position in group \"" + groupEntry.getGroup().name() + "\" for property \""
                        + propertyName + "\" is used more than once for connector \"" + connectorName + "\".");
            }
            else {
                groupProperties.put(groupEntry.getPositionInGroup(), propertySchema);
            }

            if (propertyName.equals(HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY.name())) {
                // todo: how to eventually support varied storage modules
                KafkaSchemaHistory.ALL_FIELDS.forEach(historyField -> processField(schema, orderedPropertiesByCategory, historyField));
            }
        }
    }
}

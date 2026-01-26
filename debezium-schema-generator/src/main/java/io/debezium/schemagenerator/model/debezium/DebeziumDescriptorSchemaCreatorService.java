/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.model.debezium;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;
import io.debezium.metadata.ConnectorMetadata;
import io.debezium.schemagenerator.schema.Schema.FieldFilter;

/**
 * Service to convert ConnectorMetadata to Debezium Descriptor format DTOs.
 */
public class DebeziumDescriptorSchemaCreatorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumDescriptorSchemaCreatorService.class);

    private final ConnectorMetadata connectorMetadata;
    private final FieldFilter fieldFilter;

    public DebeziumDescriptorSchemaCreatorService(ConnectorMetadata connectorMetadata, FieldFilter fieldFilter) {
        this.connectorMetadata = connectorMetadata;
        this.fieldFilter = fieldFilter;
    }

    public ConnectorDescriptor buildDescriptor() {
        // Build metadata
        Metadata metadata = new Metadata(
                "Captures changes from a " + connectorMetadata.getConnectorDescriptor().getDisplayName(),
                null);

        // Build properties
        List<Property> properties = new ArrayList<>();
        connectorMetadata.getConnectorFields().forEach(field -> {
            Property property = buildProperty(field);
            if (property != null) {
                properties.add(property);
            }
        });

        // Build and return descriptor
        return new ConnectorDescriptor(
                connectorMetadata.getConnectorDescriptor().getDisplayName(),
                determineConnectorType(),
                connectorMetadata.getConnectorDescriptor().getVersion(),
                metadata,
                properties,
                buildGroups());
    }

    private String determineConnectorType() {
        String className = connectorMetadata.getConnectorDescriptor().getClassName();
        if (className.contains("Source")) {
            return "source-connector";
        }
        else if (className.contains("Sink")) {
            return "sink-connector";
        }
        return "source-connector"; // Default
    }

    private Property buildProperty(Field field) {
        // Apply field filter
        if (!fieldFilter.include(field)) {
            return null;
        }

        // Build display
        String groupName = field.group() != null ? formatGroupName(field.group().getGroup()) : null;
        Integer groupOrder = field.group() != null ? field.group().getPositionInGroup() : null;

        Display display = new Display(
                field.displayName(),
                field.description(),
                groupName,
                groupOrder,
                mapWidth(field.width()),
                mapImportance(field.importance()));

        // Build valueDependants
        List<ValueDependant> valueDependants = null;
        if (field.valueDependants() != null && !field.valueDependants().isEmpty()) {
            valueDependants = field.valueDependants().entrySet().stream()
                    .map(entry -> new ValueDependant(
                            Collections.singletonList(entry.getKey().toString()),
                            entry.getValue()))
                    .collect(Collectors.toList());
        }

        List<Validation> validations = buildValidations(field);

        return new Property(
                field.name(),
                mapType(field.type()),
                field.isRequired() ? true : null,
                display,
                validations,
                valueDependants);
    }

    private List<Validation> buildValidations(Field field) {
        List<Validation> validations = new ArrayList<>();

        if (field.allowedValues() != null && !field.allowedValues().isEmpty()) {
            List<String> values = field.allowedValues().stream()
                    .map(Object::toString)
                    .collect(Collectors.toList());
            validations.add(Validation.enumValues(values));
        }

        Field.Validator validator = field.validator();
        if (validator != null && validator.getClass().getSimpleName().equals("RangeValidator")) {
            try {
                Number min = extractRangeValidatorField(validator, "min");
                Number max = extractRangeValidatorField(validator, "max");

                if (min != null && max != null) {
                    validations.add(Validation.range(min, max));
                }
                else if (min != null) {
                    validations.add(Validation.min(min));
                }
                else if (max != null) {
                    validations.add(Validation.max(max));
                }
            }
            catch (Exception e) {
                LOGGER.warn("Unable to extract min and max values from validator", e);
            }
        }

        return validations.isEmpty() ? null : validations;
    }

    private Number extractRangeValidatorField(Field.Validator validator, String fieldName) throws Exception {
        java.lang.reflect.Field field = validator.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Number) field.get(validator);
    }

    private List<Group> buildGroups() {
        Map<Field.Group, Integer> groupOrders = new LinkedHashMap<>();
        int order = 0;
        for (Field.Group group : Field.Group.values()) {
            groupOrders.put(group, order++);
        }

        return Arrays.stream(Field.Group.values())
                .map(group -> new Group(
                        formatGroupName(group),
                        groupOrders.get(group),
                        getGroupDescription(group)))
                .collect(Collectors.toList());
    }

    private String formatGroupName(Field.Group group) {
        // Convert enum name to readable format: CONNECTION_ADVANCED_SSL -> Connection Advanced SSL
        return Arrays.stream(group.name().split("_"))
                .map(word -> word.charAt(0) + word.substring(1).toLowerCase())
                .collect(Collectors.joining(" "));
    }

    private String getGroupDescription(Field.Group group) {
        return switch (group) {
            case CONNECTION -> "Connection configuration";
            case CONNECTION_ADVANCED_SSL -> "Advanced SSL connection configuration";
            case CONNECTION_ADVANCED -> "Advanced connection configuration";
            case CONNECTION_ADVANCED_REPLICATION -> "Advanced replication configuration";
            case CONNECTION_ADVANCED_PUBLICATION -> "Advanced publication configuration";
            case FILTERS -> "Filtering options for tables and changes";
            case CONNECTOR_SNAPSHOT -> "Snapshot configuration";
            case CONNECTOR -> "Connector configuration";
            case ADVANCED_HEARTBEAT -> "Heartbeat configuration";
            case CONNECTOR_ADVANCED -> "Advanced connector configuration";
            case ADVANCED -> "Advanced configuration";
            default -> "";
        };
    }

    private String mapType(ConfigDef.Type type) {
        return switch (type) {
            case BOOLEAN -> "boolean";
            case INT, SHORT, LONG, DOUBLE -> "number";
            case LIST -> "list";
            default -> "string";
        };
    }

    private String mapWidth(ConfigDef.Width width) {
        return switch (width) {
            case SHORT -> "short";
            case MEDIUM -> "medium";
            case LONG -> "long";
            default -> null;
        };
    }

    private String mapImportance(ConfigDef.Importance importance) {
        return importance.name().toLowerCase();
    }
}

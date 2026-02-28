/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.schema.debezium;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.schemagenerator.model.debezium.ComponentDescriptor;
import io.debezium.schemagenerator.model.debezium.Display;
import io.debezium.schemagenerator.model.debezium.Group;
import io.debezium.schemagenerator.model.debezium.Metadata;
import io.debezium.schemagenerator.model.debezium.Property;
import io.debezium.schemagenerator.model.debezium.Validation;
import io.debezium.schemagenerator.model.debezium.ValueDependant;
import io.debezium.schemagenerator.schema.Schema.FieldFilter;

/**
 * Service to convert ConnectorMetadata to Debezium Descriptor format DTOs.
 */
public class DebeziumDescriptorSchemaCreator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumDescriptorSchemaCreator.class);

    private final ComponentMetadata componentMetadata;
    private final FieldFilter fieldFilter;

    public DebeziumDescriptorSchemaCreator(ComponentMetadata componentMetadata, FieldFilter fieldFilter) {
        this.componentMetadata = componentMetadata;
        this.fieldFilter = fieldFilter;
    }

    public ComponentDescriptor buildDescriptor() {

        Metadata metadata = new Metadata(
                // TODO provide a mechanism to get a meaningful description
                componentMetadata.getComponentDescriptor().getDisplayName(),
                null);

        List<Property> properties = new ArrayList<>();
        Set<String> usedGroups = new LinkedHashSet<>();
        StreamSupport.stream(componentMetadata.getConfigDefinition().all().spliterator(), false)
                .map(this::buildProperty)
                .filter(Objects::nonNull)
                .forEach(property -> {
                    usedGroups.add(property.display().group().toLowerCase());
                    properties.add(property);
                });

        return new ComponentDescriptor(
                componentMetadata.getComponentDescriptor().getDisplayName(),
                componentMetadata.getComponentDescriptor().getType(),
                componentMetadata.getComponentDescriptor().getVersion(),
                metadata,
                properties,
                buildGroups(usedGroups));
    }

    private Property buildProperty(Field field) {

        if (!fieldFilter.include(field)) {
            return null;
        }

        String groupName = field.group() != null ? formatGroupName(field.group().getGroup()) : null;
        Integer groupOrder = field.group() != null ? field.group().getPositionInGroup() : null;

        Display display = new Display(
                field.displayName(),
                enrichDescriptionWithDefault(field),
                groupName,
                groupOrder,
                mapWidth(field.width()),
                mapImportance(field.importance()));

        List<ValueDependant> valueDependants = buildValueDependants(field);

        List<Validation> validations = buildValidations(field);

        return new Property(
                field.name(),
                mapType(field.type()),
                field.isRequired() ? true : null,
                display,
                validations,
                valueDependants);
    }

    private String enrichDescriptionWithDefault(Field field) {
        String desc = field.description();
        Object defaultValue = field.defaultValue();
        if (defaultValue == null) {
            return desc;
        }

        desc = desc.replaceAll("(?i)default\\s*(value)?\\s*(is|:)\\s*[^.]*\\.", "").trim();

        return desc + " Default: " + defaultValue.toString();
    }

    private static List<ValueDependant> buildValueDependants(Field field) {

        if (field.valueDependants() == null || field.valueDependants().isEmpty()) {
            return List.of();
        }

        return field.valueDependants().entrySet().stream()
                .map(entry -> new ValueDependant(
                        Collections.singletonList(entry.getKey().toString()),
                        entry.getValue()))
                .collect(Collectors.toList());
    }

    private List<Validation> buildValidations(Field field) {

        List<Validation> validations = new ArrayList<>();

        if (field.allowedValues() != null) {
            List<String> values = field.allowedValues().stream()
                    .map(Object::toString)
                    .collect(Collectors.toList());
            if (!values.isEmpty()) {
                validations.add(Validation.enumValues(values));
            }
        }

        if (field.validator() != null && field.validator().getClass().getSimpleName().equals("RangeValidator")) {
            extractRangeValidator(field.validator(), validations)
                    .ifPresent(validations::add);
        }

        return validations;
    }

    private Optional<Validation> extractRangeValidator(Field.Validator validator, List<Validation> validations) {
        try {
            Number min = extractRangeValidatorField(validator, "min");
            Number max = extractRangeValidatorField(validator, "max");

            if (min != null && max != null) {
                return Optional.of(Validation.range(min, max));
            }
            if (min != null) {
                return Optional.of(Validation.min(min));
            }
            if (max != null) {
                return Optional.of(Validation.max(max));
            }
        }
        catch (Exception e) {
            LOGGER.warn("Unable to extract min and max values from validator", e);
        }
        return Optional.empty();
    }

    private Number extractRangeValidatorField(Field.Validator validator, String fieldName) throws Exception {
        java.lang.reflect.Field field = validator.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Number) field.get(validator);
    }

    private List<Group> buildGroups(Set<String> usedGroups) {

        return IntStream.range(0, Field.Group.values().length)
                .mapToObj(groupPosition -> new Group(
                        formatGroupName(Field.Group.values()[groupPosition]),
                        groupPosition,
                        getGroupDescription(Field.Group.values()[groupPosition])))
                .filter(group -> usedGroups.contains(group.name().toLowerCase()))
                .collect(Collectors.toList());
    }

    private String formatGroupName(Field.Group group) {

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

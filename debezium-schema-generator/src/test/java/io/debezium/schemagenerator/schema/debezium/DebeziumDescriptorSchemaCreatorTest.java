/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.schema.debezium;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashSet;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.DependentFieldMatcher;
import io.debezium.config.Field;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.schemagenerator.model.debezium.Group;
import io.debezium.schemagenerator.model.debezium.Property;
import io.debezium.schemagenerator.model.debezium.Validation;
import io.debezium.schemagenerator.model.debezium.ValueDependant;

/**
 * Unit test for {@link DebeziumDescriptorSchemaCreator}.
 */
class DebeziumDescriptorSchemaCreatorTest {

    @Test
    void shouldBuildBasicDescriptor() {
        ComponentMetadata metadata = createBasicComponentMetadata();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> true);

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        assertThat(descriptor).isNotNull();
        assertThat(descriptor.name()).isEqualTo("Debezium PostgreSQL Connector");
        assertThat(descriptor.type()).isEqualTo("source-connector");
        assertThat(descriptor.version()).isEqualTo("1.0.0");
        assertThat(descriptor.metadata()).isNotNull();
        assertThat(descriptor.metadata().description()).isEqualTo("Debezium PostgreSQL Connector");
        assertThat(descriptor.properties()).hasSize(2);
        assertThat(descriptor.groups()).isNotEmpty();
    }

    @Test
    void shouldMapPropertyTypes() {
        ComponentMetadata metadata = createMetadataWithVariousTypes();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> true);

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        Property booleanProp = findProperty(descriptor, "boolean.property");
        assertThat(booleanProp.type()).isEqualTo("boolean");

        Property intProp = findProperty(descriptor, "int.property");
        assertThat(intProp.type()).isEqualTo("number");

        Property shortProp = findProperty(descriptor, "short.property");
        assertThat(shortProp.type()).isEqualTo("number");

        Property longProp = findProperty(descriptor, "long.property");
        assertThat(longProp.type()).isEqualTo("number");

        Property doubleProp = findProperty(descriptor, "double.property");
        assertThat(doubleProp.type()).isEqualTo("number");

        Property listProp = findProperty(descriptor, "list.property");
        assertThat(listProp.type()).isEqualTo("list");

        Property stringProp = findProperty(descriptor, "string.property");
        assertThat(stringProp.type()).isEqualTo("string");
    }

    @Test
    void shouldMapDisplayProperties() {
        ComponentMetadata metadata = createMetadataWithDisplayProperties();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> true);

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        Property prop = descriptor.properties().get(0);
        assertThat(prop.display()).isNotNull();
        assertThat(prop.display().label()).isEqualTo("Test Property");
        assertThat(prop.display().description()).isEqualTo("A test property");
        assertThat(prop.display().group()).isEqualTo("Connection");
        assertThat(prop.display().groupOrder()).isEqualTo(5);
        assertThat(prop.display().width()).isEqualTo("medium");
        assertThat(prop.display().importance()).isEqualTo("high");
    }

    @Test
    void shouldMapWidthCorrectly() {
        ComponentMetadata metadata = createMetadataWithDifferentWidths();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> true);

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        Property shortProp = findProperty(descriptor, "short.width");
        assertThat(shortProp.display().width()).isEqualTo("short");

        Property mediumProp = findProperty(descriptor, "medium.width");
        assertThat(mediumProp.display().width()).isEqualTo("medium");

        Property longProp = findProperty(descriptor, "long.width");
        assertThat(longProp.display().width()).isEqualTo("long");

        Property noneProp = findProperty(descriptor, "none.width");
        assertThat(noneProp.display().width()).isNull();
    }

    @Test
    void shouldMapImportanceCorrectly() {
        ComponentMetadata metadata = createMetadataWithDifferentImportance();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> true);

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        Property highProp = findProperty(descriptor, "high.importance");
        assertThat(highProp.display().importance()).isEqualTo("high");

        Property mediumProp = findProperty(descriptor, "medium.importance");
        assertThat(mediumProp.display().importance()).isEqualTo("medium");

        Property lowProp = findProperty(descriptor, "low.importance");
        assertThat(lowProp.display().importance()).isEqualTo("low");
    }

    @Test
    void shouldHandleRequiredFields() {
        ComponentMetadata metadata = createMetadataWithRequiredFields();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> true);

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        Property requiredProp = findProperty(descriptor, "required.field");
        assertThat(requiredProp.required()).isTrue();

        Property optionalProp = findProperty(descriptor, "optional.field");
        assertThat(optionalProp.required()).isNull();
    }

    @Test
    void shouldExtractEnumValidations() {
        ComponentMetadata metadata = createMetadataWithEnumValidation();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> true);

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        Property prop = findProperty(descriptor, "enum.field");
        assertThat(prop.validation()).isNotNull();
        assertThat(prop.validation()).hasSize(1);

        Validation validation = prop.validation().get(0);
        assertThat(validation.type()).isEqualTo("enum");
        assertThat(validation.values()).containsExactly("value1", "value2", "value3");
    }

    @Test
    void shouldExtractRangeValidations() {
        ComponentMetadata metadata = createMetadataWithRangeValidation();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> true);

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        Property prop = findProperty(descriptor, "range.field");
        assertThat(prop.validation()).isNotNull();
        assertThat(prop.validation()).hasSize(1);

        Validation validation = prop.validation().get(0);
        assertThat(validation.type()).isEqualTo("range");
        assertThat(validation.min()).isEqualTo(1);
        assertThat(validation.max()).isEqualTo(100);
    }

    @Test
    void shouldExtractMinValidations() {
        ComponentMetadata metadata = createMetadataWithMinValidation();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> true);

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        Property prop = findProperty(descriptor, "min.field");
        assertThat(prop.validation()).isNotNull();
        assertThat(prop.validation()).hasSize(1);

        Validation validation = prop.validation().get(0);
        assertThat(validation.type()).isEqualTo("min");
        assertThat(validation.min()).isEqualTo(0);
    }

    @Test
    void shouldHandleValueDependants() {
        ComponentMetadata metadata = createMetadataWithValueDependants();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> true);

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        Property prop = findProperty(descriptor, "adapter.field");
        assertThat(prop.valueDependants()).isNotNull();
        assertThat(prop.valueDependants()).hasSize(1);

        ValueDependant valueDependant = prop.valueDependants().get(0);
        assertThat(valueDependant.values()).containsExactly("value1");
        assertThat(valueDependant.dependants()).containsExactly("dependent.field");
    }

    @Test
    void shouldFilterInternalFields() {
        ComponentMetadata metadata = createMetadataWithInternalFields();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> !f.name().startsWith("internal."));

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        assertThat(descriptor.properties())
                .extracting(Property::name)
                .doesNotContain("internal.field");
        assertThat(descriptor.properties())
                .extracting(Property::name)
                .contains("normal.field");
    }

    @Test
    void shouldBuildGroupsCorrectly() {
        ComponentMetadata metadata = createMetadataWithMultipleGroups();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> true);

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        assertThat(descriptor.groups()).isNotEmpty();

        Group connectionGroup = findGroup(descriptor, "Connection");
        assertThat(connectionGroup).isNotNull();
        assertThat(connectionGroup.description()).isEqualTo("Connection configuration");

        Group filtersGroup = findGroup(descriptor, "Filters");
        assertThat(filtersGroup).isNotNull();
        assertThat(filtersGroup.description()).isEqualTo("Filtering options for tables and changes");

        Group connectorGroup = findGroup(descriptor, "Connector");
        assertThat(connectorGroup).isNotNull();
        assertThat(connectorGroup.description()).isEqualTo("Connector configuration");
    }

    @Test
    void shouldOrderGroupsCorrectly() {
        ComponentMetadata metadata = createMetadataWithMultipleGroups();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> true);

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        assertThat(descriptor.groups()).isNotEmpty();

        // Groups should be ordered by Field.Group enum order
        Group firstGroup = descriptor.groups().get(0);
        assertThat(firstGroup.name()).isEqualTo("Connection");
        assertThat(firstGroup.order()).isEqualTo(0);
    }

    @Test
    void shouldHandleFieldsWithoutExplicitGroup() {
        ComponentMetadata metadata = createMetadataWithFieldsWithoutGroup();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> true);

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        Property prop = findProperty(descriptor, "no.group.field");
        assertThat(prop).isNotNull();
        // Fields without explicit group get ADVANCED assigned by ConfigDefinition
        assertThat(prop.display().group()).isEqualTo("Advanced");
    }

    @Test
    void shouldHandleEmptyValidations() {
        ComponentMetadata metadata = createMetadataWithoutValidations();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> true);

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        Property prop = descriptor.properties().get(0);
        assertThat(prop.validation()).isEmpty();
    }

    @Test
    void shouldHandleEmptyValueDependants() {
        ComponentMetadata metadata = createBasicComponentMetadata();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> true);

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        Property prop = descriptor.properties().get(0);
        assertThat(prop.valueDependants()).isEmpty();
    }

    @Test
    void shouldIncludeOnlyUsedGroups() {
        ComponentMetadata metadata = createBasicComponentMetadata();
        DebeziumDescriptorSchemaCreator creator = new DebeziumDescriptorSchemaCreator(
                metadata,
                f -> true);

        io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor = creator.buildDescriptor();

        // Only CONNECTION group is used in basic metadata
        assertThat(descriptor.groups())
                .extracting(Group::name)
                .containsOnly("Connection");
    }

    private Property findProperty(io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor, String name) {
        return descriptor.properties().stream()
                .filter(p -> p.name().equals(name))
                .findFirst()
                .orElse(null);
    }

    private Group findGroup(io.debezium.schemagenerator.model.debezium.ComponentDescriptor descriptor, String name) {
        return descriptor.groups().stream()
                .filter(g -> g.name().equals(name))
                .findFirst()
                .orElse(null);
    }

    // Metadata factory methods

    private ComponentMetadata createBasicComponentMetadata() {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor("io.debezium.connector.postgresql.PostgresConnector", "1.0.0");
            }

            @Override
            public Field.Set getComponentFields() {
                Field field1 = Field.create("field.one")
                        .withDisplayName("Field One")
                        .withType(ConfigDef.Type.STRING)
                        .withWidth(ConfigDef.Width.MEDIUM)
                        .withImportance(ConfigDef.Importance.HIGH)
                        .withDescription("First field")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0));

                Field field2 = Field.create("field.two")
                        .withDisplayName("Field Two")
                        .withType(ConfigDef.Type.INT)
                        .withWidth(ConfigDef.Width.SHORT)
                        .withImportance(ConfigDef.Importance.MEDIUM)
                        .withDescription("Second field")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1));

                return Field.setOf(field1, field2);
            }
        };
    }

    private ComponentMetadata createMetadataWithVariousTypes() {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor("io.debezium.test.TestConnector", "1.0.0");
            }

            @Override
            public Field.Set getComponentFields() {
                return Field.setOf(
                        Field.create("boolean.property")
                                .withDisplayName("Boolean")
                                .withType(ConfigDef.Type.BOOLEAN)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("Boolean field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0)),
                        Field.create("int.property")
                                .withDisplayName("Int")
                                .withType(ConfigDef.Type.INT)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("Int field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1)),
                        Field.create("short.property")
                                .withDisplayName("Short")
                                .withType(ConfigDef.Type.SHORT)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("Short field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 2)),
                        Field.create("long.property")
                                .withDisplayName("Long")
                                .withType(ConfigDef.Type.LONG)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("Long field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 3)),
                        Field.create("double.property")
                                .withDisplayName("Double")
                                .withType(ConfigDef.Type.DOUBLE)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("Double field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 4)),
                        Field.create("list.property")
                                .withDisplayName("List")
                                .withType(ConfigDef.Type.LIST)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("List field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 5)),
                        Field.create("string.property")
                                .withDisplayName("String")
                                .withType(ConfigDef.Type.STRING)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("String field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 6)));
            }
        };
    }

    private ComponentMetadata createMetadataWithDisplayProperties() {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor("io.debezium.test.TestConnector", "1.0.0");
            }

            @Override
            public Field.Set getComponentFields() {
                Field field = Field.create("test.property")
                        .withDisplayName("Test Property")
                        .withType(ConfigDef.Type.STRING)
                        .withWidth(ConfigDef.Width.MEDIUM)
                        .withImportance(ConfigDef.Importance.HIGH)
                        .withDescription("A test property")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 5));

                return Field.setOf(field);
            }
        };
    }

    private ComponentMetadata createMetadataWithDifferentWidths() {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor("io.debezium.test.TestConnector", "1.0.0");
            }

            @Override
            public Field.Set getComponentFields() {
                return Field.setOf(
                        Field.create("short.width")
                                .withDisplayName("Short Width")
                                .withType(ConfigDef.Type.STRING)
                                .withWidth(ConfigDef.Width.SHORT)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("Short width field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0)),
                        Field.create("medium.width")
                                .withDisplayName("Medium Width")
                                .withType(ConfigDef.Type.STRING)
                                .withWidth(ConfigDef.Width.MEDIUM)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("Medium width field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1)),
                        Field.create("long.width")
                                .withDisplayName("Long Width")
                                .withType(ConfigDef.Type.STRING)
                                .withWidth(ConfigDef.Width.LONG)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("Long width field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 2)),
                        Field.create("none.width")
                                .withDisplayName("None Width")
                                .withType(ConfigDef.Type.STRING)
                                .withWidth(ConfigDef.Width.NONE)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("None width field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 3)));
            }
        };
    }

    private ComponentMetadata createMetadataWithDifferentImportance() {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor("io.debezium.test.TestConnector", "1.0.0");
            }

            @Override
            public Field.Set getComponentFields() {
                return Field.setOf(
                        Field.create("high.importance")
                                .withDisplayName("High Importance")
                                .withType(ConfigDef.Type.STRING)
                                .withWidth(ConfigDef.Width.MEDIUM)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("High importance field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0)),
                        Field.create("medium.importance")
                                .withDisplayName("Medium Importance")
                                .withType(ConfigDef.Type.STRING)
                                .withWidth(ConfigDef.Width.MEDIUM)
                                .withImportance(ConfigDef.Importance.MEDIUM)
                                .withDescription("Medium importance field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1)),
                        Field.create("low.importance")
                                .withDisplayName("Low Importance")
                                .withType(ConfigDef.Type.STRING)
                                .withWidth(ConfigDef.Width.MEDIUM)
                                .withImportance(ConfigDef.Importance.LOW)
                                .withDescription("Low importance field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 2)));
            }
        };
    }

    private ComponentMetadata createMetadataWithRequiredFields() {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor("io.debezium.test.TestConnector", "1.0.0");
            }

            @Override
            public Field.Set getComponentFields() {
                return Field.setOf(
                        Field.create("required.field")
                                .withDisplayName("Required Field")
                                .withType(ConfigDef.Type.STRING)
                                .withWidth(ConfigDef.Width.MEDIUM)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("Required field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0))
                                .required(),
                        Field.create("optional.field")
                                .withDisplayName("Optional Field")
                                .withType(ConfigDef.Type.STRING)
                                .withWidth(ConfigDef.Width.MEDIUM)
                                .withImportance(ConfigDef.Importance.MEDIUM)
                                .withDescription("Optional field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1)));
            }
        };
    }

    private ComponentMetadata createMetadataWithEnumValidation() {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor("io.debezium.test.TestConnector", "1.0.0");
            }

            @Override
            public Field.Set getComponentFields() {
                java.util.Set<String> allowedValues = new LinkedHashSet<>();
                allowedValues.add("value1");
                allowedValues.add("value2");
                allowedValues.add("value3");

                Field field = Field.create("enum.field")
                        .withDisplayName("Enum Field")
                        .withType(ConfigDef.Type.STRING)
                        .withWidth(ConfigDef.Width.MEDIUM)
                        .withImportance(ConfigDef.Importance.HIGH)
                        .withDescription("Enum field")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0))
                        .withAllowedValues(allowedValues);

                return Field.setOf(field);
            }
        };
    }

    private ComponentMetadata createMetadataWithRangeValidation() {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor("io.debezium.test.TestConnector", "1.0.0");
            }

            @Override
            public Field.Set getComponentFields() {
                Field field = Field.create("range.field")
                        .withDisplayName("Range Field")
                        .withType(ConfigDef.Type.INT)
                        .withWidth(ConfigDef.Width.MEDIUM)
                        .withImportance(ConfigDef.Importance.HIGH)
                        .withDescription("Range field")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0))
                        .withValidation(Field.RangeValidator.between(1, 100));

                return Field.setOf(field);
            }
        };
    }

    private ComponentMetadata createMetadataWithMinValidation() {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor("io.debezium.test.TestConnector", "1.0.0");
            }

            @Override
            public Field.Set getComponentFields() {
                Field field = Field.create("min.field")
                        .withDisplayName("Min Field")
                        .withType(ConfigDef.Type.INT)
                        .withWidth(ConfigDef.Width.MEDIUM)
                        .withImportance(ConfigDef.Importance.HIGH)
                        .withDescription("Min field")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0))
                        .withValidation(Field.RangeValidator.atLeast(0));

                return Field.setOf(field);
            }
        };
    }

    private ComponentMetadata createMetadataWithValueDependants() {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor("io.debezium.test.TestConnector", "1.0.0");
            }

            @Override
            public Field.Set getComponentFields() {
                Field adapterField = Field.create("adapter.field")
                        .withDisplayName("Adapter Field")
                        .withType(ConfigDef.Type.STRING)
                        .withWidth(ConfigDef.Width.MEDIUM)
                        .withImportance(ConfigDef.Importance.HIGH)
                        .withDescription("Adapter field")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0))
                        .withDependents("value1", DependentFieldMatcher.exact("dependent.field"));

                Field dependentField = Field.create("dependent.field")
                        .withDisplayName("Dependent Field")
                        .withType(ConfigDef.Type.STRING)
                        .withWidth(ConfigDef.Width.MEDIUM)
                        .withImportance(ConfigDef.Importance.LOW)
                        .withDescription("Dependent field")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1));

                ConfigDefinition config = ConfigDefinition.editor()
                        .name("Test")
                        .connector(adapterField, dependentField)
                        .create();

                return Field.setOf(config.all());
            }
        };
    }

    private ComponentMetadata createMetadataWithInternalFields() {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor("io.debezium.test.TestConnector", "1.0.0");
            }

            @Override
            public Field.Set getComponentFields() {
                return Field.setOf(
                        Field.create("normal.field")
                                .withDisplayName("Normal Field")
                                .withType(ConfigDef.Type.STRING)
                                .withWidth(ConfigDef.Width.MEDIUM)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("Normal field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0)),
                        Field.create("internal.field")
                                .withDisplayName("Internal Field")
                                .withType(ConfigDef.Type.STRING)
                                .withWidth(ConfigDef.Width.MEDIUM)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("Internal field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1)));
            }
        };
    }

    private ComponentMetadata createMetadataWithMultipleGroups() {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor("io.debezium.test.TestConnector", "1.0.0");
            }

            @Override
            public Field.Set getComponentFields() {
                return Field.setOf(
                        Field.create("connection.field")
                                .withDisplayName("Connection Field")
                                .withType(ConfigDef.Type.STRING)
                                .withWidth(ConfigDef.Width.MEDIUM)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("Connection field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0)),
                        Field.create("filters.field")
                                .withDisplayName("Filters Field")
                                .withType(ConfigDef.Type.STRING)
                                .withWidth(ConfigDef.Width.MEDIUM)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("Filters field")
                                .withGroup(Field.createGroupEntry(Field.Group.FILTERS, 0)),
                        Field.create("connector.field")
                                .withDisplayName("Connector Field")
                                .withType(ConfigDef.Type.STRING)
                                .withWidth(ConfigDef.Width.MEDIUM)
                                .withImportance(ConfigDef.Importance.HIGH)
                                .withDescription("Connector field")
                                .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 0)));
            }
        };
    }

    private ComponentMetadata createMetadataWithFieldsWithoutGroup() {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor("io.debezium.test.TestConnector", "1.0.0");
            }

            @Override
            public Field.Set getComponentFields() {
                Field field = Field.create("no.group.field")
                        .withDisplayName("No Group Field")
                        .withType(ConfigDef.Type.STRING)
                        .withWidth(ConfigDef.Width.MEDIUM)
                        .withImportance(ConfigDef.Importance.HIGH)
                        .withDescription("Field without group");

                return Field.setOf(field);
            }
        };
    }

    private ComponentMetadata createMetadataWithoutValidations() {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor("io.debezium.test.TestConnector", "1.0.0");
            }

            @Override
            public Field.Set getComponentFields() {
                Field field = Field.create("no.validation.field")
                        .withDisplayName("No Validation Field")
                        .withType(ConfigDef.Type.STRING)
                        .withWidth(ConfigDef.Width.MEDIUM)
                        .withImportance(ConfigDef.Importance.HIGH)
                        .withDescription("Field without validations")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0));

                return Field.setOf(field);
            }
        };
    }
}

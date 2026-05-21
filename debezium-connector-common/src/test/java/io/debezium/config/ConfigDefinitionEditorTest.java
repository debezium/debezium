/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Test;

public class ConfigDefinitionEditorTest {

    private static final Field FIELD_A = Field.create("field.a").withDescription("Field A");
    private static final Field FIELD_B = Field.create("field.b").withDescription("Field B");
    private static final Field FIELD_C = Field.create("field.c").withDescription("Field C");
    private static final Field FIELD_D = Field.create("field.d").withDescription("Field D");
    private static final Field FIELD_E = Field.create("field.e").withDescription("Field E");

    @Test
    public void groupAfterShouldInsertFieldsAfterAnchor() {
        ConfigDefinition configDef = ConfigDefinition.editor()
                .name("test-connector")
                .group(Field.Group.CONNECTOR, FIELD_A, FIELD_B, FIELD_D)
                .groupAfter(Field.Group.CONNECTOR, FIELD_B, FIELD_C)
                .create();

        assertThat(fieldNames(configDef)).containsExactly("field.a", "field.b", "field.c", "field.d");
    }

    @Test
    public void groupAfterShouldInsertMultipleFieldsAfterAnchorInOrder() {
        ConfigDefinition configDef = ConfigDefinition.editor()
                .name("test-connector")
                .group(Field.Group.CONNECTOR, FIELD_A, FIELD_D)
                .groupAfter(Field.Group.CONNECTOR, FIELD_A, FIELD_B, FIELD_C)
                .create();

        assertThat(fieldNames(configDef)).containsExactly("field.a", "field.b", "field.c", "field.d");
    }

    @Test
    public void groupAfterShouldAppendToEndWhenAnchorNotFound() {
        ConfigDefinition configDef = ConfigDefinition.editor()
                .name("test-connector")
                .group(Field.Group.CONNECTOR, FIELD_A, FIELD_B)
                .groupAfter(Field.Group.CONNECTOR, FIELD_E, FIELD_C)
                .create();

        assertThat(fieldNames(configDef)).containsExactly("field.a", "field.b", "field.c");
    }

    @Test
    public void groupBeforeShouldInsertFieldsBeforeAnchor() {
        ConfigDefinition configDef = ConfigDefinition.editor()
                .name("test-connector")
                .group(Field.Group.CONNECTOR, FIELD_A, FIELD_C, FIELD_D)
                .groupBefore(Field.Group.CONNECTOR, FIELD_C, FIELD_B)
                .create();

        assertThat(fieldNames(configDef)).containsExactly("field.a", "field.b", "field.c", "field.d");
    }

    @Test
    public void groupBeforeShouldInsertMultipleFieldsBeforeAnchorInOrder() {
        ConfigDefinition configDef = ConfigDefinition.editor()
                .name("test-connector")
                .group(Field.Group.CONNECTOR, FIELD_A, FIELD_D)
                .groupBefore(Field.Group.CONNECTOR, FIELD_D, FIELD_B, FIELD_C)
                .create();

        assertThat(fieldNames(configDef)).containsExactly("field.a", "field.b", "field.c", "field.d");
    }

    @Test
    public void groupBeforeShouldAppendToEndWhenAnchorNotFound() {
        ConfigDefinition configDef = ConfigDefinition.editor()
                .name("test-connector")
                .group(Field.Group.CONNECTOR, FIELD_A, FIELD_B)
                .groupBefore(Field.Group.CONNECTOR, FIELD_E, FIELD_C)
                .create();

        assertThat(fieldNames(configDef)).containsExactly("field.a", "field.b", "field.c");
    }

    @Test
    public void groupFirstShouldInsertFieldsAtBeginning() {
        ConfigDefinition configDef = ConfigDefinition.editor()
                .name("test-connector")
                .group(Field.Group.CONNECTOR, FIELD_C, FIELD_D)
                .groupFirst(Field.Group.CONNECTOR, FIELD_A, FIELD_B)
                .create();

        assertThat(fieldNames(configDef)).containsExactly("field.a", "field.b", "field.c", "field.d");
    }

    @Test
    public void childConnectorShouldInheritParentFieldsInOrder() {
        ConfigDefinition parent = ConfigDefinition.editor()
                .name("parent-connector")
                .group(Field.Group.CONNECTOR, FIELD_A, FIELD_B, FIELD_C)
                .create();

        ConfigDefinition child = parent.edit()
                .name("child-connector")
                .group(Field.Group.CONNECTOR, FIELD_D)
                .create();

        assertThat(fieldNames(child)).containsExactly("field.a", "field.b", "field.c", "field.d");
    }

    @Test
    public void childConnectorShouldOverrideParentFieldInPlace() {
        Field overriddenB = Field.create("field.b").withDescription("Overridden Field B");

        ConfigDefinition parent = ConfigDefinition.editor()
                .name("parent-connector")
                .group(Field.Group.CONNECTOR, FIELD_A, FIELD_B, FIELD_C)
                .create();

        ConfigDefinition child = parent.edit()
                .name("child-connector")
                .group(Field.Group.CONNECTOR, overriddenB)
                .create();

        assertThat(fieldNames(child)).containsExactly("field.a", "field.b", "field.c");

        Field resolvedB = StreamSupport.stream(child.all().spliterator(), false)
                .filter(f -> f.name().equals("field.b"))
                .findFirst()
                .orElseThrow();
        assertThat(resolvedB.description()).isEqualTo("Overridden Field B");
    }

    @Test
    public void childConnectorShouldMoveFieldAcrossGroups() {
        ConfigDefinition parent = ConfigDefinition.editor()
                .name("parent-connector")
                .group(Field.Group.CONNECTOR, FIELD_A, FIELD_B)
                .group(Field.Group.CONNECTION, FIELD_C)
                .create();

        ConfigDefinition child = parent.edit()
                .name("child-connector")
                .group(Field.Group.CONNECTOR, FIELD_C)
                .create();

        List<String> connectorNames = child.fieldsByGroup().getOrDefault(Field.Group.CONNECTOR, List.of())
                .stream().map(Field::name).toList();
        List<String> connectionNames = child.fieldsByGroup().getOrDefault(Field.Group.CONNECTION, List.of())
                .stream().map(Field::name).toList();

        assertThat(connectorNames).contains("field.c");
        assertThat(connectionNames).doesNotContain("field.c");
    }

    private static List<String> fieldNames(ConfigDefinition configDef) {
        return StreamSupport.stream(configDef.all().spliterator(), false)
                .map(Field::name)
                .toList();
    }
}

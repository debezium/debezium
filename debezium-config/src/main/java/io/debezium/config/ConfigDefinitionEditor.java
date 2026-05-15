/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Editor for creating {@link ConfigDefinition}s.
 *
 * @author Gunnar Morling
 */
public class ConfigDefinitionEditor {

    private String connectorName;
    private Map<Field.Group, List<Field>> fieldsByGroup = new LinkedHashMap<>();

    ConfigDefinitionEditor() {
    }

    ConfigDefinitionEditor(ConfigDefinition template) {
        connectorName = template.connectorName();
        // Copy fields from template's type, connector, history, events into the new group-based structure
        for (Field field : template.type()) {
            addToGroup(field);
        }
        for (Field field : template.connector()) {
            addToGroup(field);
        }
        for (Field field : template.history()) {
            addToGroup(field);
        }
        for (Field field : template.events()) {
            addToGroup(field);
        }
    }

    public ConfigDefinitionEditor name(String name) {
        this.connectorName = name;
        return this;
    }

    /**
     * Adds fields to the specified group, appending them to the end of the group's list.
     * This is the most common operation for adding fields.
     *
     * @param group the field group
     * @param fields the fields to add
     * @return this editor for method chaining
     */
    public ConfigDefinitionEditor group(Field.Group group, Field... fields) {
        List<Field> groupFields = fieldsByGroup.computeIfAbsent(group, k -> new ArrayList<>());
        for (Field field : fields) {
            if (!groupFields.contains(field)) {
                groupFields.add(field);
            }
        }
        return this;
    }

    /**
     * Adds fields to the specified group, inserting them after the specified anchor field.
     *
     * @param group the field group
     * @param anchor the field after which to insert
     * @param fields the fields to insert
     * @return this editor for method chaining
     */
    public ConfigDefinitionEditor groupAfter(Field.Group group, Field anchor, Field... fields) {
        List<Field> groupFields = fieldsByGroup.computeIfAbsent(group, k -> new ArrayList<>());
        int anchorIndex = groupFields.indexOf(anchor);
        if (anchorIndex == -1) {
            // Anchor not found, append to end
            return group(group, fields);
        }
        int insertIndex = anchorIndex + 1;
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            if (!groupFields.contains(field)) {
                groupFields.add(insertIndex + i, field);
            }
        }
        return this;
    }

    /**
     * Adds fields to the specified group, inserting them before the specified anchor field.
     *
     * @param group the field group
     * @param anchor the field before which to insert
     * @param fields the fields to insert
     * @return this editor for method chaining
     */
    public ConfigDefinitionEditor groupBefore(Field.Group group, Field anchor, Field... fields) {
        List<Field> groupFields = fieldsByGroup.computeIfAbsent(group, k -> new ArrayList<>());
        int anchorIndex = groupFields.indexOf(anchor);
        if (anchorIndex == -1) {
            // Anchor not found, append to end
            return group(group, fields);
        }
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[fields.length - 1 - i];
            if (!groupFields.contains(field)) {
                groupFields.add(anchorIndex, field);
            }
        }
        return this;
    }

    /**
     * Adds fields to the specified group, inserting them at the beginning of the group's list.
     *
     * @param group the field group
     * @param fields the fields to insert
     * @return this editor for method chaining
     */
    public ConfigDefinitionEditor groupFirst(Field.Group group, Field... fields) {
        List<Field> groupFields = fieldsByGroup.computeIfAbsent(group, k -> new ArrayList<>());
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[fields.length - 1 - i];
            if (!groupFields.contains(field)) {
                groupFields.add(0, field);
            }
        }
        return this;
    }

    /**
     * Removes the given fields from this configuration editor.
     */
    public ConfigDefinitionEditor excluding(Field... fields) {
        List<Field> fieldsToRemove = Arrays.asList(fields);
        for (List<Field> groupFields : fieldsByGroup.values()) {
            groupFields.removeAll(fieldsToRemove);
        }
        return this;
    }

    public ConfigDefinition create() {
        java.util.Set<String> allFieldNames = new java.util.LinkedHashSet<>();
        for (List<Field> groupFields : fieldsByGroup.values()) {
            groupFields.forEach(f -> allFieldNames.add(f.name()));
        }

        Map<Field.Group, List<Field>> resolvedFieldsByGroup = new LinkedHashMap<>();
        for (Map.Entry<Field.Group, List<Field>> entry : fieldsByGroup.entrySet()) {
            resolvedFieldsByGroup.put(entry.getKey(), resolvePatterns(entry.getValue(), allFieldNames));
        }

        return new ConfigDefinition(connectorName, resolvedFieldsByGroup);
    }

    private List<Field> resolvePatterns(List<Field> fields, java.util.Set<String> allFieldNames) {
        return fields.stream()
                .map(field -> field.resolvePatterns(allFieldNames))
                .collect(java.util.stream.Collectors.toList());
    }

    private void addToGroup(Field field) {
        if (field.group() != null) {
            Field.Group group = field.group().getGroup();
            List<Field> groupFields = fieldsByGroup.computeIfAbsent(group, k -> new ArrayList<>());
            if (!groupFields.contains(field)) {
                groupFields.add(field);
            }
        }
    }

    // Legacy methods for backward compatibility during transition
    @Deprecated
    public ConfigDefinitionEditor type(Field... fields) {
        for (Field field : fields) {
            addToGroup(field);
        }
        return this;
    }

    @Deprecated
    public ConfigDefinitionEditor connector(Field... fields) {
        for (Field field : fields) {
            addToGroup(field);
        }
        return this;
    }

    @Deprecated
    public ConfigDefinitionEditor history(Field... fields) {
        for (Field field : fields) {
            addToGroup(field);
        }
        return this;
    }

    @Deprecated
    public ConfigDefinitionEditor events(Field... fields) {
        for (Field field : fields) {
            addToGroup(field);
        }
        return this;
    }
}
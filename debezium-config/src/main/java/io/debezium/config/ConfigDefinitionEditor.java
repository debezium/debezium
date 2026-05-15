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
        for (Map.Entry<Field.Group, List<Field>> entry : template.fieldsByGroup().entrySet()) {
            fieldsByGroup.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                    .addAll(entry.getValue());
        }
    }

    public ConfigDefinitionEditor name(String name) {
        this.connectorName = name;
        return this;
    }

    /**
     * Adds fields to the specified group, appending them to the end of the group's list.
     * If a field with the same name already exists in the group, it is replaced in-place
     * (allowing child connectors to override parent field definitions, e.g. to add a custom validator).
     *
     * @param group the field group
     * @param fields the fields to add
     * @return this editor for method chaining
     */
    public ConfigDefinitionEditor group(Field.Group group, Field... fields) {
        List<Field> groupFields = fieldsByGroup.computeIfAbsent(group, k -> new ArrayList<>());
        for (Field field : fields) {
            removeFromOtherGroups(group, field.name());
            int existingIdx = indexOfByName(groupFields, field.name());
            if (existingIdx >= 0) {
                groupFields.set(existingIdx, field);
            }
            else {
                groupFields.add(field);
            }
        }
        return this;
    }

    /**
     * Adds fields to the specified group, inserting them after the specified anchor field.
     * If a field with the same name already exists in the group, it is replaced in-place.
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
        int insertOffset = 0;
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            removeFromOtherGroups(group, field.name());
            int existingIdx = indexOfByName(groupFields, field.name());
            if (existingIdx >= 0) {
                groupFields.set(existingIdx, field);
            }
            else {
                groupFields.add(insertIndex + insertOffset, field);
                insertOffset++;
            }
        }
        return this;
    }

    /**
     * Adds fields to the specified group, inserting them before the specified anchor field.
     * If a field with the same name already exists in the group, it is replaced in-place.
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
        int insertOffset = 0;
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[fields.length - 1 - i];
            removeFromOtherGroups(group, field.name());
            int existingIdx = indexOfByName(groupFields, field.name());
            if (existingIdx >= 0) {
                groupFields.set(existingIdx, field);
            }
            else {
                groupFields.add(anchorIndex + insertOffset, field);
            }
        }
        return this;
    }

    /**
     * Adds fields to the specified group, inserting them at the beginning of the group's list.
     * If a field with the same name already exists in the group, it is replaced in-place.
     * If a field with the same name exists in a different group, it is moved to this group.
     *
     * @param group the field group
     * @param fields the fields to insert
     * @return this editor for method chaining
     */
    public ConfigDefinitionEditor groupFirst(Field.Group group, Field... fields) {
        List<Field> groupFields = fieldsByGroup.computeIfAbsent(group, k -> new ArrayList<>());
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[fields.length - 1 - i];
            removeFromOtherGroups(group, field.name());
            int existingIdx = indexOfByName(groupFields, field.name());
            if (existingIdx >= 0) {
                groupFields.set(existingIdx, field);
            }
            else {
                groupFields.add(0, field);
            }
        }
        return this;
    }

    private void removeFromOtherGroups(Field.Group targetGroup, String fieldName) {
        for (Map.Entry<Field.Group, List<Field>> entry : fieldsByGroup.entrySet()) {
            if (entry.getKey() != targetGroup) {
                entry.getValue().removeIf(f -> f.name().equals(fieldName));
            }
        }
    }

    private static int indexOfByName(List<Field> fields, String name) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
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

    // Legacy methods for backward compatibility during transition.
    // Fields with an explicit .withGroup() annotation keep their annotated group via group();
    // unannotated fields fall back to the default group so they are not silently dropped.
    @Deprecated
    public ConfigDefinitionEditor type(Field... fields) {
        return group(Field.Group.CONNECTION, fields);
    }

    @Deprecated
    public ConfigDefinitionEditor connector(Field... fields) {
        return group(Field.Group.CONNECTOR, fields);
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
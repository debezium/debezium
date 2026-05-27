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

    private String componentName;
    private Map<Field.Group, List<Field>> fieldsByGroup = new LinkedHashMap<>();

    ConfigDefinitionEditor() {
    }

    ConfigDefinitionEditor(ConfigDefinition template) {
        componentName = template.componentName();
        for (Map.Entry<Field.Group, List<Field>> entry : template.fieldsByGroup().entrySet()) {
            fieldsByGroup.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                    .addAll(entry.getValue());
        }
    }

    public ConfigDefinitionEditor name(String name) {
        this.componentName = name;
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
        insertFields(groupFields, group, groupFields.size(), fields);
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
            return group(group, fields);
        }
        insertFields(groupFields, group, anchorIndex + 1, fields);
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
            return group(group, fields);
        }
        insertFields(groupFields, group, anchorIndex, fields);
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
        insertFields(groupFields, group, 0, fields);
        return this;
    }

    private void insertFields(List<Field> groupFields, Field.Group group, int insertIndex, Field... fields) {
        int insertOffset = 0;
        for (Field field : fields) {
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
            List<Field> resolved = resolvePatterns(entry.getValue(), allFieldNames);
            resolvedFieldsByGroup.put(entry.getKey(), assignPositions(entry.getKey(), resolved));
        }

        return new ConfigDefinition(componentName, resolvedFieldsByGroup);
    }

    private List<Field> resolvePatterns(List<Field> fields, java.util.Set<String> allFieldNames) {
        return fields.stream()
                .map(field -> field.resolvePatterns(allFieldNames))
                .collect(java.util.stream.Collectors.toList());
    }

    private List<Field> assignPositions(Field.Group group, List<Field> fields) {
        List<Field> result = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            result.add(fields.get(i).withGroup(Field.createGroupEntry(group, i)));
        }
        return result;
    }
}

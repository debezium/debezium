/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;

/**
 * Defines the configuration options of a connector.
 *
 * @author Jiri Pechanec
 * @author Gunnar Morling
 */
@ThreadSafe
@Immutable
public class ConfigDefinition {

    private final String connectorName;
    private final Map<Field.Group, List<Field>> fieldsByGroup;
    private final Map<String, Integer> fieldGroupOrderMap;

    ConfigDefinition(String connectorName, Map<Field.Group, List<Field>> fieldsByGroup) {
        this.connectorName = connectorName;
        this.fieldsByGroup = Collections.unmodifiableMap(fieldsByGroup);
        this.fieldGroupOrderMap = Collections.unmodifiableMap(buildFieldGroupOrderMap());
    }

    // Legacy constructor for backward compatibility
    @Deprecated
    ConfigDefinition(String connectorName, List<Field> type, List<Field> connector, List<Field> history,
                     List<Field> events) {
        this.connectorName = connectorName;
        this.fieldsByGroup = convertToGroupMap(type, connector, history, events);
        this.fieldGroupOrderMap = Collections.unmodifiableMap(buildFieldGroupOrderMap());
    }

    private Map<Field.Group, List<Field>> convertToGroupMap(List<Field> type, List<Field> connector, List<Field> history, List<Field> events) {
        Map<Field.Group, List<Field>> map = new LinkedHashMap<>();
        for (Field field : type) {
            addToGroupMap(map, field);
        }
        for (Field field : connector) {
            addToGroupMap(map, field);
        }
        for (Field field : history) {
            addToGroupMap(map, field);
        }
        for (Field field : events) {
            addToGroupMap(map, field);
        }
        return map;
    }

    private void addToGroupMap(Map<Field.Group, List<Field>> map, Field field) {
        if (field.group() != null) {
            Field.Group group = field.group().getGroup();
            List<Field> groupFields = map.computeIfAbsent(group, k -> new ArrayList<>());
            if (!groupFields.contains(field)) {
                groupFields.add(field);
            }
        }
    }

    /**
     * Builds a map of field name to its order within its {@link Field.Group},
     * based on the order fields appear in this {@link ConfigDefinition}.
     * If a field has an explicit positionInGroup (not the default 9999), that value is used directly.
     * Otherwise, the position is assigned implicitly based on the field's appearance order within its group.
     */
    private Map<String, Integer> buildFieldGroupOrderMap() {
        final Map<Field.Group, Integer> groupCounters = new HashMap<>();
        final Map<String, Integer> fieldOrders = new LinkedHashMap<>();
        for (List<Field> groupFields : fieldsByGroup.values()) {
            for (Field field : groupFields) {
                if (field.group() != null) {
                    final Field.Group group = field.group().getGroup();
                    final int explicitPosition = field.group().getPositionInGroup();
                    if (explicitPosition != 9999) {
                        fieldOrders.put(field.name(), explicitPosition);
                    }
                    else {
                        final int order = groupCounters.merge(group, 0, Integer::sum);
                        groupCounters.put(group, order + 1);
                        fieldOrders.put(field.name(), order);
                    }
                }
            }
        }
        return fieldOrders;
    }

    /**
     * Returns a map of field name to its implicit 0-based order within its {@link Field.Group},
     * derived from the order fields appear in this {@link ConfigDefinition}.
     */
    public Map<String, Integer> fieldGroupOrderMap() {
        return fieldGroupOrderMap;
    }

    /**
     * Returns an editor for new empty config definition instance.
     */
    public static ConfigDefinitionEditor editor() {
        return new ConfigDefinitionEditor();
    }

    /**
     * Returns an editor for a config definition instance seeded with the values from this config definition.
     */
    public ConfigDefinitionEditor edit() {
        return new ConfigDefinitionEditor(this);
    }

    public Iterable<Field> all() {
        final List<Field> all = new ArrayList<>();
        for (List<Field> groupFields : fieldsByGroup.values()) {
            all.addAll(groupFields);
        }
        return all;
    }

    public ConfigDef configDef() {
        final ConfigDef config = new ConfigDef();
        for (Map.Entry<Field.Group, List<Field>> entry : fieldsByGroup.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                Field.group(config, entry.getKey().name(), entry.getValue().toArray(new Field[0]));
            }
        }
        return config;
    }

    public String connectorName() {
        return connectorName;
    }

    // Legacy methods for backward compatibility - deprecated
    @Deprecated
    public List<Field> type() {
        return getFieldsForGroupLegacy("type");
    }

    @Deprecated
    public List<Field> connector() {
        return getFieldsForGroupLegacy("connector");
    }

    @Deprecated
    public List<Field> history() {
        return getFieldsForGroupLegacy("history");
    }

    @Deprecated
    public List<Field> events() {
        return getFieldsForGroupLegacy("events");
    }

    private List<Field> getFieldsForGroupLegacy(String groupName) {
        // This is a best-effort legacy method - it returns empty list since we no longer have fixed buckets
        // Callers should migrate to using the group-based API
        return Collections.emptyList();
    }

    public Map<Field.Group, List<Field>> fieldsByGroup() {
        return fieldsByGroup;
    }
}

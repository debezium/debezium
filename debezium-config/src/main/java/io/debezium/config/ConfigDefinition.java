/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.ArrayList;
import java.util.Collections;
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

    private final String componentName;
    private final Map<Field.Group, List<Field>> fieldsByGroup;

    ConfigDefinition(String componentName, Map<Field.Group, List<Field>> fieldsByGroup) {
        this.componentName = componentName;
        this.fieldsByGroup = Collections.unmodifiableMap(fieldsByGroup);
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

    public String componentName() {
        return componentName;
    }

    public Map<Field.Group, List<Field>> fieldsByGroup() {
        return fieldsByGroup;
    }
}

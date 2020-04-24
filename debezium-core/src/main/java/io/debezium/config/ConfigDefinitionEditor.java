/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;

/**
 * Editor for creating {@link ConfigDefinition}s.
 *
 * @author Gunnar Morling
 */
public class ConfigDefinitionEditor {

    private String connectorName;
    private List<Field> type = new ArrayList<>();
    private List<Field> connector = new ArrayList<>();
    private List<Field> history = new ArrayList<>();
    private List<Field> events = new ArrayList<>();
    private Set<Field> exclude = new HashSet<>();

    ConfigDefinitionEditor() {
    }

    ConfigDefinitionEditor(ConfigDefinition template) {
        connectorName = template.getConnectorName();
        type.addAll(template.getType());
        connector.addAll(template.getConnector());
        history.addAll(template.getHistory());
        events.addAll(template.getEvents());
        exclude.addAll(template.getExclude());
    }

    public ConfigDefinitionEditor name(String name) {
        this.connectorName = name;
        return this;
    }

    public ConfigDefinitionEditor type(Field... fields) {
        type.addAll(Arrays.asList(fields));
        return this;
    }

    public ConfigDefinitionEditor connector(Field... fields) {
        connector.addAll(Arrays.asList(fields));
        return this;
    }

    public ConfigDefinitionEditor history(Field... fields) {
        history.addAll(Arrays.asList(fields));
        return this;
    }

    public ConfigDefinitionEditor events(Field... fields) {
        events.addAll(Arrays.asList(fields));
        return this;
    }

    public ConfigDefinitionEditor exclude(Field... fields) {
        exclude.addAll(Arrays.asList(fields));
        return this;
    }

    public Iterable<Field> all() {
        final List<Field> all = new ArrayList<>();
        addToList(all, type);
        addToList(all, connector);
        addToList(all, history);
        addToList(all, events);
        return removeExcluded(all);
    }

    public ConfigDef configDef() {
        final ConfigDef config = new ConfigDef();
        addToConfigDef(config, connectorName, type);
        addToConfigDef(config, "Connector", connector);
        addToConfigDef(config, "History Storage", history);
        addToConfigDef(config, "Events", events);
        return config;
    }

    private void addToList(List<Field> list, List<Field> fields) {
        if (fields != null) {
            list.addAll(fields);
        }
    }

    private void addToConfigDef(ConfigDef configDef, String group, List<Field> fields) {
        if (!fields.isEmpty()) {
            Field.group(configDef, group, removeExcluded(fields).toArray(new Field[fields.size()]));
        }
    }

    private List<Field> removeExcluded(List<Field> list) {
        return list.stream()
                .filter(f -> !exclude.contains(f))
                .collect(Collectors.toList());
    }

    public ConfigDefinition create() {
        return new ConfigDefinition(connectorName, type, connector, history, events, exclude);
    }
}
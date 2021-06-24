/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

    ConfigDefinitionEditor() {
    }

    ConfigDefinitionEditor(ConfigDefinition template) {
        connectorName = template.connectorName();
        type.addAll(template.type());
        connector.addAll(template.connector());
        history.addAll(template.history());
        events.addAll(template.events());
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

    /**
     * Removes the given fields from this configuration editor.
     */
    public ConfigDefinitionEditor excluding(Field... fields) {
        type.removeAll(Arrays.asList(fields));
        connector.removeAll(Arrays.asList(fields));
        history.removeAll(Arrays.asList(fields));
        events.removeAll(Arrays.asList(fields));
        return this;
    }

    public ConfigDefinition create() {
        return new ConfigDefinition(connectorName, type, connector, history, events);
    }
}
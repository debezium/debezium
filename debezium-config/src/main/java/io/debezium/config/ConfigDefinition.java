/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    private final List<Field> type;
    private final List<Field> connector;
    private final List<Field> history;
    private final List<Field> events;

    ConfigDefinition(String connectorName, List<Field> type, List<Field> connector, List<Field> history,
                     List<Field> events) {
        this.connectorName = connectorName;
        this.type = Collections.unmodifiableList(type);
        this.connector = Collections.unmodifiableList(connector);
        this.history = Collections.unmodifiableList(history);
        this.events = Collections.unmodifiableList(events);
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

        addToList(all, type);
        addToList(all, connector);
        addToList(all, history);
        addToList(all, events);

        return all;
    }

    public ConfigDef configDef() {
        final ConfigDef config = new ConfigDef();
        addToConfigDef(config, connectorName, type);
        addToConfigDef(config, "Connector", connector);
        addToConfigDef(config, "History Storage", history);
        addToConfigDef(config, "Events", events);
        return config;
    }

    public String connectorName() {
        return connectorName;
    }

    public List<Field> type() {
        return type;
    }

    public List<Field> connector() {
        return connector;
    }

    public List<Field> history() {
        return history;
    }

    public List<Field> events() {
        return events;
    }

    private void addToList(List<Field> list, List<Field> fields) {
        if (fields != null) {
            list.addAll(fields);
        }
    }

    private void addToConfigDef(ConfigDef configDef, String group, List<Field> fields) {
        if (!fields.isEmpty()) {
            Field.group(configDef, group, fields.toArray(new Field[0]));
        }
    }
}

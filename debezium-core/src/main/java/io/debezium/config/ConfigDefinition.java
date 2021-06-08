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

    private static final String CONNECTOR_GROUP = "Connector";
    private static final String HISTORY_STORAGE_GROUP = "History Storage";
    private static final String EVENTS_GROUP = "Events";

    private final String connectorName;
    private final List<Field> type;
    private final List<Field> connector;
    private final List<Field> history;
    private final List<Field> events;

    private List<Field> mergedFields;

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

    /**
     * Gets a sorted collection of all configuration fields for a connector.
     * <p>
     * If the fields in this definition are not already sorted, this method sorts
     * the fields taking 'group' and 'orderInGroup' into account.
     *
     * @return A sorted collection of configuration fields; never null.
     */
    public Iterable<Field> all() {
        return getOrCreateMergeFields();
    }

    /**
     * Gets a ConfigDef whose fields are sorted according to the 'groupName' and 'orderInGroup'
     * properties of each individual field.
     *
     * @return A sorted ConfigDef; never null
     */
    public ConfigDef configDef() {
        final ConfigDef config = new ConfigDef();

        final List<Field> fields = getOrCreateMergeFields();

        // group was already added during merging
        Field.group(config, null, fields.toArray(new Field[fields.size()]));
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

    private List<Field> getOrCreateMergeFields() {
        if (mergedFields != null) {
            return mergedFields;
        }

        mergedFields = new ArrayList<>();

        addToMergedList(mergedFields, connectorName, this.type);
        addToMergedList(mergedFields, CONNECTOR_GROUP, this.connector);
        addToMergedList(mergedFields, HISTORY_STORAGE_GROUP, this.history);
        addToMergedList(mergedFields, EVENTS_GROUP, this.events);

        Field.sort(mergedFields);
        return mergedFields;
    }

    private void addToMergedList(final List<Field> mergedFields, String groupName, List<Field> fields) {
        if (!isNullOrEmpty(fields) && groupName != null) {
            mergedFields.addAll(Field.addGroupNameAndOrderInGroup(groupName, fields.toArray(new Field[fields.size()])));
        }
    }

    private static boolean isNullOrEmpty(final List<Field> fields) {
        return fields == null ? true : fields.isEmpty();
    }
}

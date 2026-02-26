/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metadata;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.Field;

public interface ComponentMetadata {

    ComponentDescriptor getComponentDescriptor();

    /**
     * Returns the configuration definition for this metadata provider.
     *
     * <p><b>Known limitations:</b></p>
     * <ul>
     *   <li><b>ConfigDefinition is connector-centric:</b> It assumes a fixed categorization
     *       (type/connector/history/events) that makes sense for database connectors but not
     *       for other components like SMTs, which lack "history" or "events" configuration.</li>
     *   <li><b>Dual grouping mechanism:</b> Fields can define their own {@link Field.Group}
     *       (e.g., CONNECTION, FILTERS), while ConfigDefinition imposes additional category-level
     *       groups when converting to {@link org.apache.kafka.common.config.ConfigDef} via
     *       {@code ConfigDefinition#configDef()}. This creates two overlapping grouping systems.</li>
     *   <li><b>Default implementation workaround:</b> This default implementation places all fields
     *       into the "type" category, which may not be semantically appropriate for non-connector
     *       components.</li>
     * </ul>
     *
     * <p>Consider refactoring to make ConfigDefinition more generic and rely solely on
     * field-level groups rather than imposing hardcoded categories.
     * <p>
     * Groups should be extracted from the fields and the group order defined by {@link io.debezium.config.Field.Group}</p>
     *
     * In future only this method should be used and the {@code getConnectorFields()} removed
     */
    default ConfigDefinition getConfigDefinition() {
        return ConfigDefinition.editor()
                .name(getClass().getName())
                .type(getComponentFields().asArray())
                .create();
    }

    Field.Set getComponentFields();
}

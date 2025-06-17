/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import io.debezium.config.Configuration;

public record OpenLineageJobIdentifier(String namespace, String name) {

    public static final String CONNECTOR_NAME_PROPERTY = "topic.prefix";
    public static final String CONNECTOR_TASK_ID = "task.id";

    /**
     * Format string for constructing job names in the pattern "connectorLogicalName.taskNumber".
     * The first placeholder (%s) is replaced with the connector logical name (topic.prefix), and the second
     * placeholder (%s) is replaced with the task number (0, 1, 2, ...).
     *
     * Example: For connector "my-connector" and task 0, the resulting job name would be "my-connector.0"
     */
    private static final String JOB_NAME_FORMAT = "%s.%s";

    public static OpenLineageJobIdentifier from(Configuration connectorConfig, DebeziumOpenLineageConfiguration debeziumOpenLineageConfiguration) {

        return new OpenLineageJobIdentifier(debeziumOpenLineageConfiguration.job().namespace(),
                String.format(JOB_NAME_FORMAT,
                        connectorConfig.getString(CONNECTOR_NAME_PROPERTY),
                        connectorConfig.getString(CONNECTOR_TASK_ID, "0")));
    }
}

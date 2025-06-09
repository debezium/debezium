/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

public class DebeziumHeaders {

    public static final String DEBEZIUM_HEADER_PREFIX = "__debezium.";
    public static final String DEBEZIUM_CONTEXT_HEADER_PREFIX = DEBEZIUM_HEADER_PREFIX + "context.";
    public static final String DEBEZIUM_CONNECTOR_LOGICAL_NAME_HEADER = DEBEZIUM_CONTEXT_HEADER_PREFIX + "connectorLogicalName";
    public static final String DEBEZIUM_TASK_ID_HEADER = DEBEZIUM_CONTEXT_HEADER_PREFIX + "taskId";
    public static final String DEBEZIUM_CONNECTOR_NAME_HEADER = DEBEZIUM_CONTEXT_HEADER_PREFIX + "connectorName";
}

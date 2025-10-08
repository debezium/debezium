/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.Map;

import org.apache.kafka.connect.source.SourceTask;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;

/**
 * Contains contextual information and objects scoped to the lifecycle of Debezium's {@link SourceTask} implementations.
 *
 * @author Gunnar Morling
 */
public class CdcSourceTaskContext {

    private final String connectorType;
    private final String connectorLogicalName;
    private final String connectorPluginName;
    private final String taskId;
    private final Map<String, String> customMetricTags;
    private final Clock clock;
    private final CommonConnectorConfig config;

    public CdcSourceTaskContext(CommonConnectorConfig config,
                                String taskId,
                                Map<String, String> customMetricTags) {
        this.connectorType = config.getContextName();
        this.connectorLogicalName = config.getLogicalName();
        this.connectorPluginName = config.getConnectorName();
        this.taskId = taskId;
        this.customMetricTags = customMetricTags;
        this.config = config;
        this.clock = Clock.system();
    }

    public CdcSourceTaskContext(CommonConnectorConfig config,
                                Map<String, String> customMetricTags) {
        this(config, "0", customMetricTags);
    }

    /**
     * Configure the logger's Mapped Diagnostic Context (MDC) properties for the thread making this call.
     *
     * @param contextName the name of the context; may not be null
     * @return the previous MDC context; never null
     * @throws IllegalArgumentException if {@code contextName} is null
     */
    public LoggingContext.PreviousContext configureLoggingContext(String contextName) {
        return LoggingContext.forConnector(connectorType, connectorLogicalName, contextName);
    }

    public LoggingContext.PreviousContext configureLoggingContext(String contextName, Partition partition) {
        return LoggingContext.forConnector(connectorType, connectorLogicalName, taskId, contextName, partition);
    }

    /**
     * Run the supplied function in the temporary connector MDC context, and when complete always return the MDC context to its
     * state before this method was called.
     *
     * @param connectorConfig the configuration of the connector; may not be null
     * @param contextName the name of the context; may not be null
     * @param operation the function to run in the new MDC context; may not be null
     * @throws IllegalArgumentException if any of the parameters are null
     */
    public void temporaryLoggingContext(CommonConnectorConfig connectorConfig, String contextName, Runnable operation) {
        LoggingContext.temporarilyForConnector("MySQL", connectorConfig.getLogicalName(), contextName, operation);
    }

    /**
     * Returns a clock for obtaining the current time.
     */
    public Clock getClock() {
        return clock;
    }

    public String getConnectorType() {
        return connectorType;
    }

    public String getConnectorLogicalName() {
        return connectorLogicalName;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getConnectorPluginName() {
        return connectorPluginName;
    }

    public Map<String, String> getCustomMetricTags() {
        return customMetricTags;
    }

    public CommonConnectorConfig getConfig() {
        return config;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.connect.source.SourceTask;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.spi.Partition;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;

/**
 * Contains contextual information and objects scoped to the lifecycle of Debezium's {@link SourceTask} implementations.
 *
 * @author Gunnar Morling
 */
public class CdcSourceTaskContext {

    private final String connectorType;
    private final String connectorName;
    private final String taskId;
    private final Clock clock;
    private final Map<String, String> customMetricTags;

    /**
     * Obtains the data collections captured at the point of invocation.
     */
    private final Supplier<Collection<? extends DataCollectionId>> collectionsSupplier;

    public CdcSourceTaskContext(String connectorType, String connectorName, String taskId,
                                Map<String, String> customMetricTags,
                                Supplier<Collection<? extends DataCollectionId>> collectionsSupplier) {
        this.connectorType = connectorType;
        this.connectorName = connectorName;
        this.taskId = taskId;
        this.collectionsSupplier = collectionsSupplier != null ? collectionsSupplier : Collections::emptyList;
        this.customMetricTags = customMetricTags;
        this.clock = Clock.system();
    }

    public CdcSourceTaskContext(String connectorType, String connectorName,
                                Map<String, String> customMetricTags,
                                Supplier<Collection<? extends DataCollectionId>> collectionsSupplier) {
        this(connectorType, connectorName, "0", customMetricTags, collectionsSupplier);
    }

    /**
     * Configure the logger's Mapped Diagnostic Context (MDC) properties for the thread making this call.
     *
     * @param contextName the name of the context; may not be null
     * @return the previous MDC context; never null
     * @throws IllegalArgumentException if {@code contextName} is null
     */
    public LoggingContext.PreviousContext configureLoggingContext(String contextName) {
        return LoggingContext.forConnector(connectorType, connectorName, contextName);
    }

    public LoggingContext.PreviousContext configureLoggingContext(String contextName, Partition partition) {
        return LoggingContext.forConnector(connectorType, connectorName, taskId, contextName, partition);
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

    public String[] capturedDataCollections() {
        return collectionsSupplier.get()
                .stream()
                .map(DataCollectionId::toString)
                .toArray(String[]::new);
    }

    public String getConnectorType() {
        return connectorType;
    }

    public String getConnectorName() {
        return connectorName;
    }

    public String getTaskId() {
        return taskId;
    }

    public Map<String, String> getCustomMetricTags() {
        return customMetricTags;
    }
}

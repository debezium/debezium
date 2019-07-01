/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

import org.apache.kafka.connect.source.SourceTask;

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
    private final Clock clock;

    /**
     * Obtains the data collections captured at the point of invocation.
     */
    private final Supplier<Collection<? extends DataCollectionId>> collectionsSupplier;

    public CdcSourceTaskContext(String connectorType, String connectorName, Supplier<Collection<? extends DataCollectionId>> collectionsSupplier) {
        this.connectorType = connectorType;
        this.connectorName = connectorName;
        this.collectionsSupplier = collectionsSupplier != null ? collectionsSupplier : Collections::emptyList;

        this.clock = Clock.system();
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
}

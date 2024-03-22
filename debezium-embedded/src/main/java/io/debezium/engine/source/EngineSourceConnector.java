/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceConnectorContext;

/**
 * Implementation of {@link DebeziumSourceConnector} which currently serves only as a wrapper
 * around Kafka Connect {@link SourceConnector}.
 *
 * @author vjuranek
 */
public class EngineSourceConnector implements DebeziumSourceConnector {

    private final SourceConnector connectConnector;
    private DebeziumSourceConnectorContext context;

    public EngineSourceConnector(final SourceConnector connectConnector) {
        this.connectConnector = connectConnector;
    }

    public SourceConnector connectConnector() {
        return connectConnector;
    }

    @Override
    public DebeziumSourceConnectorContext context() {
        return this.context;
    }

    @Override
    public void initialize(DebeziumSourceConnectorContext context) {
        this.context = context;
        this.connectConnector.initialize((SourceConnectorContext) context);
    }
}
/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.Configuration;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.Signaler;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.runtime.Connector;
import io.debezium.runtime.DebeziumStatus;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.quarkus.debezium.engine.capture.CapturingInvokerRegistry;

@ApplicationScoped
class SourceRecordDebezium extends RunnableDebezium {

    private final DebeziumEngineConfiguration debeziumEngineConfiguration;
    private final DebeziumEngine<?> engine;
    private final Connector connector;
    private final StateHandler stateHandler;

    SourceRecordDebezium(DebeziumEngineConfiguration debeziumEngineConfiguration,
                         StateHandler stateHandler,
                         Connector connector,
                         CapturingInvokerRegistry<RecordChangeEvent<SourceRecord>> registry) {
        this.debeziumEngineConfiguration = debeziumEngineConfiguration;
        this.stateHandler = stateHandler;

        this.engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(Configuration.empty()
                        .withSystemProperties(Function.identity())
                        .edit()
                        .with(Configuration.from(debeziumEngineConfiguration.configuration()))
                        .build().asProperties())
                .using(this.stateHandler.connectorCallback())
                .using(this.stateHandler.completionCallback())
                .notifying(event -> registry.get(event).capture(event))
                .build();
        this.connector = connector;
    }

    @Override
    public Signaler signaler() {
        return engine.getSignaler();
    }

    @Override
    public Map<String, String> configuration() {
        return debeziumEngineConfiguration.configuration();
    }

    @Override
    public DebeziumStatus status() {
        return stateHandler.get();
    }

    @Override
    public Connector connector() {
        return connector;
    }

    protected void run() {
        this.engine.run();
    }

    protected void close() throws IOException {
        this.engine.close();
    }

}

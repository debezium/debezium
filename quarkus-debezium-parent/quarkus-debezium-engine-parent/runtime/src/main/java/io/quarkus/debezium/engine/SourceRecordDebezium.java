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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.Signaler;
import io.debezium.runtime.Connector;
import io.debezium.runtime.DebeziumStatus;
import io.debezium.runtime.EngineManifest;
import io.quarkus.debezium.engine.capture.consumer.SourceRecordEventConsumer;

@ApplicationScoped
class SourceRecordDebezium extends RunnableDebezium {

    private static final Logger LOGGER = LoggerFactory.getLogger(SourceRecordDebezium.class.getName());
    private final Map<String, String> configuration;
    private final DebeziumEngine<?> engine;
    private final Connector connector;
    private final StateHandler stateHandler;
    private final EngineManifest engineManifest;

    SourceRecordDebezium(Map<String, String> configuration,
                         StateHandler stateHandler,
                         Connector connector,
                         SourceRecordEventConsumer consumer,
                         EngineManifest engineManifest) {
        LOGGER.trace("Creating SourceRecordDebezium for engine {}", engineManifest);
        this.configuration = configuration;
        this.stateHandler = stateHandler;
        this.engine = DebeziumEngine.create(Connect.class, Connect.class)
                .using(Configuration.empty()
                        .withSystemProperties(Function.identity())
                        .edit()
                        .with(Configuration.from(configuration))
                        .build().asProperties())
                .using(this.stateHandler.connectorCallback(engineManifest, this))
                .using(this.stateHandler.completionCallback(engineManifest, this))
                .notifying(consumer)
                .build();
        this.connector = connector;
        this.engineManifest = engineManifest;
    }

    @Override
    public Signaler signaler() {
        return engine.getSignaler();
    }

    @Override
    public Map<String, String> configuration() {
        return configuration;
    }

    @Override
    public DebeziumStatus status() {
        return stateHandler.get(engineManifest);
    }

    @Override
    public Connector connector() {
        return connector;
    }

    @Override
    public EngineManifest manifest() {
        return engineManifest;
    }

    protected void run() {
        this.engine.run();
    }

    protected void close() throws IOException {
        this.engine.close();
    }

}

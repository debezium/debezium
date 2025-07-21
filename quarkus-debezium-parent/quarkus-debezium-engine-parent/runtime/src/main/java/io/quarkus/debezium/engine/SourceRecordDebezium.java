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
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.Connector;
import io.debezium.runtime.DebeziumStatus;
import io.quarkus.debezium.engine.capture.CapturingInvokerRegistry;

@ApplicationScoped
class SourceRecordDebezium extends RunnableDebezium {

    private final Map<String, String> configuration;
    private final DebeziumEngine<?> engine;
    private final Connector connector;
    private final StateHandler stateHandler;

    SourceRecordDebezium(Map<String, String> configuration,
                         StateHandler stateHandler,
                         Connector connector,
                         CapturingInvokerRegistry<CapturingEvent<SourceRecord>> registry) {
        this.configuration = configuration;
        this.stateHandler = stateHandler;
        this.engine = DebeziumEngine.create(Connect.class, Connect.class)
                .using(Configuration.empty()
                        .withSystemProperties(Function.identity())
                        .edit()
                        .with(Configuration.from(configuration))
                        .build().asProperties())
                .using(this.stateHandler.connectorCallback())
                .using(this.stateHandler.completionCallback())
                .notifying(event -> {
                    var capturingEvent = new CapturingEvent<SourceRecord>() {
                        @Override
                        public SourceRecord value() {
                            return event.value();
                        }

                        @Override
                        public String destination() {
                            return event.destination();
                        }

                        @Override
                        public String source() {
                            return "NOT_AVAILABLE";
                        }
                    };

                    registry.get(capturingEvent).capture(capturingEvent);
                })
                .build();
        this.stateHandler.setDebeziumEngine(this);
        this.connector = connector;
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

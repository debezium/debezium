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
import io.debezium.engine.format.ChangeEventFormat;
import io.quarkus.debezium.configuration.DebeziumEngineConfiguration;

@ApplicationScoped
public class SourceRecordDebezium extends RunnableDebezium {
    private final Logger LOGGER = LoggerFactory.getLogger(SourceRecordDebezium.class);

    private final DebeziumEngineConfiguration debeziumEngineConfiguration;
    private final DebeziumEngine<?> engine;

    public SourceRecordDebezium(DebeziumEngineConfiguration debeziumEngineConfiguration) {
        this.debeziumEngineConfiguration = debeziumEngineConfiguration;

        this.engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(Configuration.empty()
                        .withSystemProperties(Function.identity())
                        .edit()
                        .with(Configuration.from(debeziumEngineConfiguration.configuration()))
                        .build().asProperties())
                .notifying(event -> LOGGER.info("**EXPERIMENTAL** {}", event.record().value().toString()))
                .build();
    }

    @Override
    public Signaler signaler() {
        return engine.getSignaler();
    }

    @Override
    public Map<String, String> configuration() {
        return debeziumEngineConfiguration.configuration();
    }

    protected void run() {
        this.engine.run();
    }

    protected void close() throws IOException {
        this.engine.close();
    }
}

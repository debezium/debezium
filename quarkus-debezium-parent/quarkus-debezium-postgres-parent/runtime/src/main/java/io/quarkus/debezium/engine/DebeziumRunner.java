/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumRunner.class);

    private final ExecutorService executorService;
    private final RunnableDebezium engine;

    public DebeziumRunner(ExecutorService executorService, Debezium debezium) {
        this.executorService = executorService;
        this.engine = (RunnableDebezium) debezium;
    }

    public void start() {
        LOGGER.info("Starting Debezium Engine...");
        executorService.execute(engine::run);
    }

    public void shutdown() throws RuntimeException {
        LOGGER.info("Shutting down Debezium Engine...");
        try {
            engine.close();
        }
        catch (IOException e) {
            throw new RuntimeException("Impossible to shutdown Debezium Engine ", e);
        }
        executorService.shutdown();
    }
}

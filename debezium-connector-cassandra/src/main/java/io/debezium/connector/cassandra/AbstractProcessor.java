/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An abstract processor designed to be a convenient superclass for all concrete processors for Cassandra
 * connector task. The class handles concurrency control for starting and stopping the processor.
 */
public abstract class AbstractProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractProcessor.class);

    private final String name;
    private final AtomicBoolean taskState;
    private boolean processorState;

    public AbstractProcessor(String name, AtomicBoolean taskState) {
        this.name = name;
        this.taskState = taskState;
        this.processorState = false;
    }

    public boolean isTaskRunning() {
        return taskState.get();
    }

    public boolean isProcessorRunning() {
        return processorState;
    }

    /**
     * Override initialize to initialize resources before starting the processor
     */
    public void initialize() throws Exception { }

    /**
     * Override destroy to clean up resources after stopping the processor
     */
    public void destroy() throws Exception { }

    public void start() throws InterruptedException, IOException {
        if (!isTaskRunning()) {
            taskState.set(true);
        }
        if (!isProcessorRunning()) {
            processorState = true;
            LOGGER.info("Starting {}", name);
            doStart();
        } else {
            LOGGER.info("Ignoring start signal for {} because it is already started", name);
        }
    }

    public void stop() throws InterruptedException, IOException {
        if (isTaskRunning()) {
            taskState.set(false);
        }
        if (isProcessorRunning()) {
            processorState = false;
            doStop();
            LOGGER.info("Stopped {}", name);
        } else {
            LOGGER.info("Ignoring stop signal for {} because it is already stopped", name);
        }
    }

    /**
     * The processor has been requested to start, so perform any work required to start the processor
     */
    public abstract void doStart() throws InterruptedException, IOException;

    /**
     * The processor has been requested to stop, so perform any work required to stop the processor
     */
    public abstract void doStop() throws InterruptedException, IOException;

    public String getName() {
        return name;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A component that blocks doing nothing until the connector task is stopped
 *
 * @author Peter Goransson
 */
public class BlockingReader implements Reader {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final AtomicReference<Runnable> uponCompletion = new AtomicReference<>();
    private final AtomicReference<State> state = new AtomicReference<>();

    private final CountDownLatch latch = new CountDownLatch(1);

    private final String name;

    public BlockingReader(String name) {
        this.name = name;
    }

    /**
     * Waits indefinitely until the connector task is shut down
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        latch.await();
        state.set(State.STOPPING);

        return null;
    }

    @Override
    public State state() {
        return state.get();
    }

    @Override
    public void uponCompletion(Runnable handler) {
        assert this.uponCompletion.get() == null;
        this.uponCompletion.set(handler);
    }

    @Override
    public void start() {
        state.set(State.RUNNING);
        logger.info("Connector has completed all of its work but will continue in the running state. It can be shut down at any time.");
    }

    @Override
    public void stop() {
        try {
            try {
                state.set(State.STOPPED);
            }
            finally {
                latch.countDown();
            }

            // Cleanup Resources
            Runnable completionHandler = uponCompletion.getAndSet(null); // set to null so that we call it only once
            if (completionHandler != null) {
                completionHandler.run();
            }

        } finally {
            logger.info("Blocking Reader has completed.");
        }
    }

    @Override
    public String name() {
        return name;
    }

}

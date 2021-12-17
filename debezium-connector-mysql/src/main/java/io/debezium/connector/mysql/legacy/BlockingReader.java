/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.ConfigurationDefaults;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * A component that blocks doing nothing until the connector task is stopped
 *
 * @author Peter Goransson
 */
public class BlockingReader implements Reader {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final AtomicReference<Consumer<MySqlPartition>> uponCompletion = new AtomicReference<>();
    private final AtomicReference<MySqlPartition> partition = new AtomicReference<>();
    private final AtomicReference<State> state = new AtomicReference<>();
    private final Metronome metronome;

    private final String name;
    private final String runningLogMessage;

    public BlockingReader(String name, String runningLogMessage) {
        this.name = name;
        this.metronome = Metronome.parker(ConfigurationDefaults.RETURN_CONTROL_INTERVAL, Clock.SYSTEM);
        this.runningLogMessage = runningLogMessage;

    }

    /**
     * Does nothing until the connector task is shut down, but regularly returns control back to Connect in order for being paused if requested.
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (state.get() == State.STOPPED) {
            return null;
        }

        metronome.pause();
        state.compareAndSet(State.RUNNING, State.STOPPING);

        return null;
    }

    @Override
    public State state() {
        return state.get();
    }

    @Override
    public void uponCompletion(Consumer<MySqlPartition> handler) {
        assert this.uponCompletion.get() == null;
        this.uponCompletion.set(handler);
    }

    @Override
    public void start(MySqlPartition partition) {
        state.set(State.RUNNING);
        logger.info(runningLogMessage);
    }

    @Override
    public void stop() {
        try {
            state.set(State.STOPPED);

            // Cleanup Resources
            Consumer<MySqlPartition> completionHandler = uponCompletion.getAndSet(null); // set to null so that we call it only once
            if (completionHandler != null) {
                completionHandler.accept(partition.get());
            }

        }
        finally {
            logger.info("Blocking Reader has completed.");
        }
    }

    @Override
    public String name() {
        return name;
    }

}

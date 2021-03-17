/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.util.Clock;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * A component that blocks doing nothing for a specified period of time or until the connector task is stopped
 *
 * @author Peter Goransson
 */
public class TimedBlockingReader extends BlockingReader {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final Duration timeout;
    private volatile Timer timer;

    /**
     * @param name Name of the reader
     * @param timeout Duration of time until this TimedBlockingReader should stop
     */
    public TimedBlockingReader(String name, Duration timeout) {
        super(name, "The connector will wait for " + timeout.toMillis() + " ms before proceeding");
        this.timeout = timeout;
    }

    @Override
    public void start() {
        super.start();
        this.timer = Threads.timer(Clock.SYSTEM, timeout);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        super.poll();

        // Stop when we've reached the timeout threshold
        if (timer != null && timer.expired()) {
            stop();
        }

        return null;
    }
}

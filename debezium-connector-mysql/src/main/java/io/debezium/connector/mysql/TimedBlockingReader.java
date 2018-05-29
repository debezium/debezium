/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.ConfigurationDefaults;

/**
 * A component that blocks doing nothing for a specified period of time or until the connector task is stopped
 *
 * @author Peter Goransson
 */
public class TimedBlockingReader extends BlockingReader {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    // calculated number of ticks until this TimedBlockingReader should stop
    private long ticks;

    /**
     * @param name Name of the reader
     * @param timeout Duration of time until this TimedBlockingReader should stop
     */
    public TimedBlockingReader(String name, Duration timeoutMinutes) {
        super(name, "The connector will wait for " + timeoutMinutes.toMinutes() + " minutes before proceeding");

        // number of ticks calculated based upon the BlockingReader's metronome period
        ticks = timeoutMinutes.toMillis() / ConfigurationDefaults.RETURN_CONTROL_INTERVAL.toMillis();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        super.poll();

        // Stop when we've reached the timeout threshold
        if (--ticks <= 0) {
            stop();
        }

        return null;
    }


}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.channels.process;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.ThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.signal.SignalRecord;
import io.debezium.pipeline.signal.channels.SignalChannelReader;

/**
 * Implementation of {@link SignalChannelReader} that also implements {@link SignalChannelWriter}
 * used for sending signal from the same JVM process
 *
 * <p>
 *     Mainly targeted at Debezium Engine
 * </p>
 */
@ThreadSafe
public class InProcessSignalChannel implements SignalChannelReader, SignalChannelWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(InProcessSignalChannel.class);
    public static final String CHANNEL_NAME = "in-process";

    private final AtomicBoolean open = new AtomicBoolean(false);
    private final Queue<SignalRecord> signals = new ConcurrentLinkedQueue<>();

    @Override
    public String name() {
        return CHANNEL_NAME;
    }

    @Override
    public void init(CommonConnectorConfig connectorConfig) {
        open.compareAndSet(false, true);
        LOGGER.info("Reading signals from {} channel", CHANNEL_NAME);
    }

    @Override
    public List<SignalRecord> read() {
        return Stream.ofNullable(signals.poll()).toList();
    }

    @Override
    public void close() {
        if (open.compareAndSet(true, false)) {
            drain();
        }
    }

    private void drain() {
        while (!signals.isEmpty()) {
            signals.clear();
        }
    }

    @Override
    public void signal(SignalRecord signal) {
        if (!open.get()) {
            throw new DebeziumException("Channel already closed");
        }
        signals.add(signal);
    }
}

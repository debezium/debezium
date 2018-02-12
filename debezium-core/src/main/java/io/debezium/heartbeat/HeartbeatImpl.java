/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.function.BlockingConsumer;
import io.debezium.util.Clock;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * Default implementation of Heartbeat
 *
 * @author Jiri Pechanec
 *
 */
class HeartbeatImpl implements Heartbeat {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatImpl.class);

    /**
     * Default length of interval in which connector generates periodically
     * heartbeat messages. A size of 0 disables heartbeat.
     */
    static final int DEFAULT_HEARTBEAT_INTERVAL = 0;

    /**
     * Default prefix for names of heartbeat topics
     */
    static final String DEFAULT_HEARTBEAT_TOPICS_PREFIX = "__debezium-heartbeat";

    private static Schema KEY_SCHEMA = Schema.STRING_SCHEMA;

    private final String topicName;
    private final Supplier<OffsetPosition> positionSupplier;
    private final Duration heartbeatInterval;
    private final String key;

    private volatile Timer heartbeatTimeout;

    HeartbeatImpl(Configuration configuration, String topicName,
            String key, Supplier<OffsetPosition> positionSupplier) {
        this.topicName = topicName;
        this.positionSupplier = positionSupplier;
        this.key = key;

        heartbeatInterval = configuration.getDuration(HeartbeatImpl.HEARTBEAT_INTERVAL, ChronoUnit.MILLIS);
        heartbeatTimeout = resetHeartbeat();
    }

    @Override
    public void heartbeat(Consumer<SourceRecord> consumer) {
        if (heartbeatTimeout.expired()) {
            LOGGER.debug("Generating heartbeat event");
            consumer.accept(heartbeatRecord());
            heartbeatTimeout = resetHeartbeat();
        }
    }

    @Override
    public void heartbeat(BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        if (heartbeatTimeout.expired()) {
            LOGGER.debug("Generating heartbeat event");
            consumer.accept(heartbeatRecord());
            heartbeatTimeout = resetHeartbeat();
        }
    }

    /**
     * Produce an empty record to the heartbeat topic.
     *
     */
    private SourceRecord heartbeatRecord() {
        final Integer partition = 0;
        OffsetPosition position = positionSupplier.get();

        return new SourceRecord(position.partition(), position.offset(),
                topicName, partition,  KEY_SCHEMA, key, null, null);
    }

    private Timer resetHeartbeat() {
        return Threads.timer(Clock.SYSTEM, heartbeatInterval);
    }
}

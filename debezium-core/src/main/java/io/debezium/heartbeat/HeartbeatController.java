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

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.function.BlockingConsumer;
import io.debezium.util.Clock;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * A class that is able to generate periodic heartbeat messages based on a pre-configured interval. The clients are
 * supposed to call method {@link #heartbeat(Consumer)} from a main loop of a connector.
 *
 * @author Jiri Pechanec
 *
 */
public class HeartbeatController {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatController.class);

    /**
     * Default length of interval in which connector generates periodically
     * heartbeat messages. A size of 0 disables heartbeat.
     */
    private static final int DEFAULT_HEARTBEAT_INTERVAL = 0;

    /**
     * Default prefix for names of heartbeat topics
     */
    private static final String DEFAULT_HEARTBEAT_TOPICS_PREFIX = "__debezium-heartbeat";

    public static final Field HEARTBEAT_INTERVAL = Field.create("heartbeat.interval.ms")
                                                                      .withDisplayName("Conector heartbeat interval (milli-seconds)")
                                                                      .withType(Type.INT)
                                                                      .withWidth(Width.MEDIUM)
                                                                      .withImportance(Importance.MEDIUM)
                                                                      .withDescription("Length of an interval in milli-seconds in in which the connector periodically sends heartbeat messages "
                                                                              + "to a heartbeat topic. "
                                                                              + "Use 0 to disable heartbeat messages. "
                                                                              + "Disabled by default.")
                                                                      .withDefault(DEFAULT_HEARTBEAT_INTERVAL)
                                                                      .withValidation(Field::isNonNegativeInteger);

    public static final Field HEARTBEAT_TOPICS_PREFIX = Field.create("heartbeat.topics.prefix")
                                                             .withDisplayName("A prefix used for naming of heartbeat topics")
                                                             .withType(Type.STRING)
                                                             .withWidth(Width.MEDIUM)
                                                             .withImportance(Importance.LOW)
                                                             .withDescription("The prefix that is used to name heartbeat topics."
                                                                     + "Defaults to " + DEFAULT_HEARTBEAT_TOPICS_PREFIX + ".")
                                                             .withDefault(DEFAULT_HEARTBEAT_TOPICS_PREFIX);

    private final String topicName;
    private final Supplier<OffsetPosition> positionSupplier;
    private final Duration heartbeatInterval;

    private volatile Timer heartbeatTimeout;

    public HeartbeatController(Configuration configuration, String topicName,
            Supplier<OffsetPosition> positionSupplier) {
        this.topicName = topicName;
        this.positionSupplier = positionSupplier;

        heartbeatInterval = configuration.getDuration(HeartbeatController.HEARTBEAT_INTERVAL, ChronoUnit.MILLIS);
        heartbeatTimeout = resetHeartbeat();
    }

    /**
     * Generates a heartbeat record if defined time has elapsed
     *
     * @param consumer - a code to place record among others to be sent into Connect
     */
    public void heartbeat(Consumer<SourceRecord> consumer) {
        if (heartbeatTimeout.expired()) {
            LOGGER.debug("Generating heartbeat event");
            consumer.accept(heartbeatRecord());
            heartbeatTimeout = resetHeartbeat();
        }
    }

    /**
     * Generates a heartbeat record if defined time has elapsed
     *
     * @param consumer - a code to place record among others to be sent into Connect
     */
    public void heartbeat(BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        if (heartbeatTimeout.expired()) {
            LOGGER.info("Generating heartbeat event");
            consumer.accept(heartbeatRecord());
            heartbeatTimeout = resetHeartbeat();
        }
    }

    /**
     * Produce an empty record to heartbeat topic.
     *
     */
    private SourceRecord heartbeatRecord() {
        final Integer partition = 0;
        OffsetPosition position = positionSupplier.get();

        return new SourceRecord(position.partition(), position.offset(),
                topicName, partition, null, null, null, null);
    }

    private Timer resetHeartbeat() {
        return Threads.timer(Clock.SYSTEM, heartbeatInterval.isZero() ? Duration.ofMillis(Long.MAX_VALUE) : heartbeatInterval);
    }
}
